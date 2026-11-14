// cmd/financials-taxonomy is the ticket-06 operator tool. It has three
// read-mostly modes:
//
//	classify  — one discriminant probe per ticker; dry-run by default, writes
//	            the evidence CSV, --apply seeds ticker_financial_taxonomy.
//	reconcile — joins the uncovered-name list against the IDX profile-backed
//	            tickers table; read-only, writes a CSV.
//	matrix    — fetches the page's own type lists for one general, one bank and
//	            one insurer, then emits the per-taxonomy applicability matrix.
//
// It never fetches full statements and never writes depth values: ticket 05's
// sweep owns depth_status and the depth columns.
package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/config"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/financials"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/yahoo"
)

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}
	switch args[0] {
	case "classify":
		runClassify(args[1:])
	case "reconcile":
		runReconcile(args[1:])
	case "matrix":
		runMatrix(args[1:])
	case "help", "-h", "--help":
		usage()
	default:
		if strings.HasPrefix(args[0], "-") {
			// A bare `financials-taxonomy --tickers-file ...` is the classify
			// mode, matching the command's primary job.
			runClassify(args)
			return
		}
		fatalf("financials-taxonomy: unknown command %q", args[0])
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `financials-taxonomy — Yahoo/Morningstar taxonomy population and evidence (ticket 06)

Usage:
  financials-taxonomy classify  --tickers-file FILE [--out FILE] [--apply --yes]
  financials-taxonomy classify  --from-csv FILE --apply --yes
  financials-taxonomy reconcile --uncovered FILE [--out FILE]
  financials-taxonomy matrix    [--general PTBA] [--bank BBRI] [--insurance ABDA]

classify:
  Reads one ticker per line (commas are also accepted), probes the small
  discriminant set, and writes ticker,taxonomy,method,evidence,applied,error.
  Dry-run by default; --apply needs --yes and writes only ticker_financial_taxonomy
  with depth_status='pending'. "other" rows are reported but not persisted.
  Ambiguous probe results (other, or an insurer with only the shared interest-
  income code) get one income-page type-list check unless --verify-pages=false.
  --from-csv applies an already-reviewed evidence CSV without probing again.

reconcile:
  Reads the uncovered-name CSV (ticker|sector) and the tickers table, then writes
  ticker|sector|classification|name|listing_board|listing_date|active. Read-only.

matrix:
  Fetches the page's own income/balance-sheet/cash-flow type lists for one ticker
  per template (9 paced page fetches, never a 650-name burst), writes the evidence
  JSON and the concept applicability markdown.
`)
}

func fatalf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "financials-taxonomy: "+format+"\n", args...)
	os.Exit(1)
}

func newLogger() *logrus.Logger {
	log := logrus.New()
	log.SetOutput(os.Stderr)
	log.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})
	return log
}

// ─── classify ──────────────────────────────────────────────────────────────

// yahooTaxonomySource adapts *yahoo.Client to the usecase's classify-only seam.
// When the discriminant probe is ambiguous, it confirms the result with one
// statement-page type list. That is the only page fetch in the command and it
// applies to a handful of tickers, never the whole universe.
type yahooTaxonomySource struct {
	client      *yahoo.Client
	verifyPages bool
}

func (s yahooTaxonomySource) ClassifyTaxonomy(ctx context.Context, ticker string) (financials.Taxonomy, string, map[string]bool, error) {
	cls, err := s.client.Classify(ctx, ticker)
	if err != nil {
		return "", "", nil, err
	}
	if !s.verifyPages || !cls.Ambiguous() {
		return cls.Taxonomy, cls.Method, cls.Evidence, nil
	}

	list, err := s.client.DiscoverTypeList(ctx, ticker, "financials")
	if err != nil {
		// The probe already succeeded; a failed page check must not fail the
		// ticker. Keep the probe result and let the CSV show the weaker method.
		return cls.Taxonomy, cls.Method, cls.Evidence, nil
	}
	taxonomy, ok := taxonomyFromTypeList(list)
	if !ok {
		return cls.Taxonomy, cls.Method, cls.Evidence, nil
	}
	evidence := make(map[string]bool, len(cls.Evidence)+1)
	for k, v := range cls.Evidence {
		evidence[k] = v
	}
	evidence["page_type_list"] = true
	return taxonomy, yahoo.MethodPageTypeList, evidence, nil
}

// taxonomyFromTypeList resolves the template from the smallest embedded type
// list on the income page: 154 general, 108 bank, 74 insurance. The page also
// embeds a ~500-type preload, so the smallest group is the statement list.
func taxonomyFromTypeList(list *yahoo.TypeList) (financials.Taxonomy, bool) {
	if list == nil || len(list.Groups) == 0 {
		return financials.TaxonomyOther, false
	}
	best := 0
	for i, g := range list.Groups {
		if len(g.Types) < len(list.Groups[best].Types) {
			best = i
		}
	}
	switch len(list.Groups[best].Types) {
	case 154:
		return financials.TaxonomyGeneral154, true
	case 108:
		return financials.TaxonomyBank108, true
	case 74:
		return financials.TaxonomyInsurance74, true
	}
	return financials.TaxonomyOther, false
}

func runClassify(args []string) {
	fs := flag.NewFlagSet("classify", flag.ExitOnError)
	tickersFile := fs.String("tickers-file", "", "file with one ticker per line (comma-separated also accepted)")
	fromCSV := fs.String("from-csv", "", "apply an existing evidence CSV instead of probing")
	out := fs.String("out", "yahoo-taxonomy.csv", "evidence CSV output path (- for stdout)")
	apply := fs.Bool("apply", false, "upsert the classification into ticker_financial_taxonomy")
	yes := fs.Bool("yes", false, "required with --apply; confirms the printed database target")
	limit := fs.Int("limit", 0, "classify at most N tickers (0 = all)")
	concurrency := fs.Int("concurrency", 4, "probes in flight")
	spacing := fs.Duration("spacing", 800*time.Millisecond, "shared delay between probes")
	timeout := fs.Duration("timeout", 45*time.Minute, "overall run timeout")
	verifyPages := fs.Bool("verify-pages", true, "confirm ambiguous probe results with one income-page type list")
	_ = fs.Parse(args)

	if *apply && !*yes {
		fatalf("classify: --apply requires --yes (it writes the database target printed below)")
	}

	var tickers []string
	var results []usecase.TaxonomyResult
	if *fromCSV != "" {
		if strings.TrimSpace(*tickersFile) != "" {
			fatalf("classify: --from-csv and --tickers-file are mutually exclusive")
		}
		if !*apply {
			fatalf("classify: --from-csv requires --apply (the CSV is already the dry-run evidence)")
		}
		loaded, err := readTaxonomyCSV(*fromCSV)
		if err != nil {
			fatalf("classify: %v", err)
		}
		if *limit > 0 && *limit < len(loaded) {
			loaded = loaded[:*limit]
		}
		results = loaded
	} else {
		if strings.TrimSpace(*tickersFile) == "" {
			fatalf("classify: --tickers-file or --from-csv is required")
		}
		loaded, err := readTickerFile(*tickersFile)
		if err != nil {
			fatalf("classify: %v", err)
		}
		if *limit > 0 && *limit < len(loaded) {
			loaded = loaded[:*limit]
		}
		if len(loaded) == 0 {
			fatalf("classify: no tickers in %s", *tickersFile)
		}
		tickers = loaded
	}

	log := newLogger()
	var db *sqlx.DB
	if *apply {
		vip := config.NewViper()
		fmt.Fprintf(os.Stderr, "financials-taxonomy: applying to %s/%s\n",
			vip.GetString("database.host"), vip.GetString("database.name"))
		db = config.NewDatabase(vip, log)
		defer db.Close()
	}

	client := yahoo.NewClient()
	uc := usecase.NewTaxonomyPopulation(
		db, log,
		yahooTaxonomySource{client: client, verifyPages: *verifyPages},
		repository.NewFinancialDepthRepository(log),
		usecase.WithTaxonomyPacing(*concurrency, *spacing),
	)

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	var err error
	if *fromCSV != "" {
		results, err = uc.ApplyResults(ctx, results)
	} else {
		results, err = uc.Populate(ctx, tickers, *apply)
	}
	if err != nil {
		fatalf("classify: %v", err)
	}

	if err := writeTaxonomyCSV(*out, results); err != nil {
		fatalf("classify: write %s: %v", *out, err)
	}

	var ok, failed, applied int
	counts := map[financials.Taxonomy]int{}
	for _, r := range results {
		if r.Err != nil {
			failed++
			continue
		}
		ok++
		counts[r.Taxonomy]++
		if r.Applied {
			applied++
		}
	}
	fmt.Fprintf(os.Stderr, "financials-taxonomy: classified=%d failed=%d applied=%d\n", ok, failed, applied)
	for _, tax := range []financials.Taxonomy{
		financials.TaxonomyGeneral154, financials.TaxonomyBank108,
		financials.TaxonomyInsurance74, financials.TaxonomyOther,
	} {
		if counts[tax] > 0 {
			fmt.Fprintf(os.Stderr, "  %-14s %d\n", tax, counts[tax])
		}
	}
	if ok == 0 {
		os.Exit(1)
	}
}

func readTickerFile(path string) ([]string, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return parseTickerText(string(body)), nil
}

// readTaxonomyCSV reloads the evidence written by writeTaxonomyCSV. It keeps a
// row's error so ApplyResults skips it, and rejects an invalid taxonomy rather
// than letting the table CHECK fail mid-batch.
func readTaxonomyCSV(path string) ([]usecase.TaxonomyResult, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	records, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	out := make([]usecase.TaxonomyResult, 0, len(records))
	for i, rec := range records {
		if i == 0 && len(rec) > 0 && strings.EqualFold(strings.TrimSpace(rec[0]), "ticker") {
			continue
		}
		if len(rec) < 3 {
			continue
		}
		res := usecase.TaxonomyResult{
			Ticker:   strings.ToUpper(strings.TrimSpace(rec[0])),
			Taxonomy: financials.Taxonomy(strings.TrimSpace(rec[1])),
			Method:   strings.TrimSpace(rec[2]),
		}
		if len(rec) > 3 {
			res.Evidence = parseEvidence(rec[3])
		}
		if len(rec) > 5 && strings.TrimSpace(rec[5]) != "" {
			res.Err = errors.New(strings.TrimSpace(rec[5]))
		}
		if res.Ticker == "" {
			continue
		}
		if res.Err == nil && !res.Taxonomy.Valid() {
			res.Err = fmt.Errorf("invalid taxonomy %q", rec[1])
		}
		out = append(out, res)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no rows in %s", path)
	}
	return out, nil
}

// parseEvidence reads the CODE=1;CODE=0 evidence column.
func parseEvidence(s string) map[string]bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	out := make(map[string]bool)
	for _, part := range strings.Split(s, ";") {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			continue
		}
		out[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1]) == "1"
	}
	return out
}

// parseTickerText splits on newlines, commas, tabs and spaces, skips blank lines
// and # comments, uppercases, strips .JK, and deduplicates in first-seen order.
func parseTickerText(text string) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0)
	for _, line := range strings.Split(text, "\n") {
		if i := strings.IndexByte(line, '#'); i >= 0 {
			line = line[:i]
		}
		for _, raw := range strings.FieldsFunc(line, func(r rune) bool {
			switch r {
			case '\r', ',', '\t', ' ':
				return true
			}
			return false
		}) {
			t := strings.ToUpper(strings.TrimSpace(raw))
			t = strings.TrimSuffix(t, ".JK")
			if t == "" {
				continue
			}
			if _, ok := seen[t]; ok {
				continue
			}
			seen[t] = struct{}{}
			out = append(out, t)
		}
	}
	return out
}

func writeTaxonomyCSV(path string, results []usecase.TaxonomyResult) error {
	var w io.Writer = os.Stdout
	var file *os.File
	if path != "-" {
		f, err := os.Create(path)
		if err != nil {
			return err
		}
		file = f
		w = f
		defer file.Close()
	}

	cw := csv.NewWriter(w)
	if err := cw.Write([]string{"ticker", "taxonomy", "method", "evidence", "applied", "error"}); err != nil {
		return err
	}
	for _, r := range results {
		errText := ""
		if r.Err != nil {
			errText = r.Err.Error()
		}
		row := []string{
			r.Ticker,
			string(r.Taxonomy),
			r.Method,
			evidenceString(r.Evidence),
			fmt.Sprintf("%t", r.Applied),
			errText,
		}
		if err := cw.Write(row); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}

// evidenceString renders the probe map deterministically: CODE=1;CODE=0.
func evidenceString(evidence map[string]bool) string {
	if len(evidence) == 0 {
		return ""
	}
	keys := make([]string, 0, len(evidence))
	for k := range evidence {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		v := "0"
		if evidence[k] {
			v = "1"
		}
		parts = append(parts, k+"="+v)
	}
	return strings.Join(parts, ";")
}

// ─── reconcile ─────────────────────────────────────────────────────────────

type uncoveredRow struct {
	Ticker string
	Sector string
}

type tickerReconcileRow struct {
	Code         string     `db:"code"`
	Name         string     `db:"name"`
	Active       bool       `db:"active"`
	ListingDate  *time.Time `db:"listing_date"`
	ListingBoard *string    `db:"listing_board"`
}

func runReconcile(args []string) {
	fs := flag.NewFlagSet("reconcile", flag.ExitOnError)
	uncoveredPath := fs.String("uncovered", ".scratch/financials-v2/evidence/yahoo-uncovered-sector-known.csv", "uncovered-name CSV (ticker|sector)")
	out := fs.String("out", ".scratch/financials-v2/evidence/yahoo-uncovered-reconciled.csv", "output CSV (- for stdout)")
	limit := fs.Int("limit", 0, "reconcile at most N rows (0 = all)")
	_ = fs.Parse(args)

	uncovered, err := readUncovered(*uncoveredPath)
	if err != nil {
		fatalf("reconcile: %v", err)
	}
	if *limit > 0 && *limit < len(uncovered) {
		uncovered = uncovered[:*limit]
	}

	log := newLogger()
	vip := config.NewViper()
	db := config.NewDatabase(vip, log)
	defer db.Close()

	var rows []tickerReconcileRow
	if err := db.Select(&rows, `
		SELECT code, COALESCE(name, '') AS name, active, listing_date, listing_board
		  FROM tickers ORDER BY code`); err != nil {
		fatalf("reconcile: read tickers: %v", err)
	}
	byCode := make(map[string]*tickerReconcileRow, len(rows))
	for i := range rows {
		byCode[rows[i].Code] = &rows[i]
	}

	type result struct {
		uncoveredRow
		Classification string
		Name           string
		ListingBoard   string
		ListingDate    string
		Active         bool
	}
	results := make([]result, 0, len(uncovered))
	counts := map[string]int{}
	for _, u := range uncovered {
		r := result{uncoveredRow: u}
		if t, ok := byCode[u.Ticker]; ok {
			r.Name = t.Name
			r.Active = t.Active
			if t.ListingBoard != nil {
				r.ListingBoard = *t.ListingBoard
			}
			if t.ListingDate != nil {
				r.ListingDate = t.ListingDate.Format("2006-01-02")
			}
			if t.Active {
				r.Classification = "listed_active"
			} else {
				r.Classification = "listed_inactive"
			}
		} else {
			r.Classification = "not_listed"
		}
		counts[r.Classification]++
		results = append(results, r)
	}

	var w io.Writer = os.Stdout
	var file *os.File
	if *out != "-" {
		f, err := os.Create(*out)
		if err != nil {
			fatalf("reconcile: create %s: %v", *out, err)
		}
		file = f
		defer file.Close()
		w = f
	}
	cw := csv.NewWriter(w)
	cw.Comma = '|'
	_ = cw.Write([]string{"ticker", "sector", "classification", "name", "listing_board", "listing_date", "active"})
	for _, r := range results {
		_ = cw.Write([]string{
			r.Ticker, r.Sector, r.Classification, r.Name, r.ListingBoard, r.ListingDate,
			fmt.Sprintf("%t", r.Active),
		})
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		fatalf("reconcile: write %s: %v", *out, err)
	}

	fmt.Fprintf(os.Stderr, "financials-taxonomy: reconciled %d uncovered name(s): listed_active=%d listed_inactive=%d not_listed=%d\n",
		len(results), counts["listed_active"], counts["listed_inactive"], counts["not_listed"])
}

func readUncovered(path string) ([]uncoveredRow, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.Comma = '|'
	r.FieldsPerRecord = -1
	records, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	out := make([]uncoveredRow, 0, len(records))
	for i, rec := range records {
		if len(rec) == 0 {
			continue
		}
		if i == 0 && strings.EqualFold(strings.TrimSpace(rec[0]), "ticker") {
			continue
		}
		ticker := strings.ToUpper(strings.TrimSpace(rec[0]))
		if ticker == "" {
			continue
		}
		sector := ""
		if len(rec) > 1 {
			sector = strings.TrimSpace(rec[1])
		}
		out = append(out, uncoveredRow{Ticker: ticker, Sector: sector})
	}
	return out, nil
}

// ─── matrix ────────────────────────────────────────────────────────────────

type matrixTemplate struct {
	Taxonomy     financials.Taxonomy        `json:"taxonomy"`
	Ticker       string                     `json:"ticker"`
	IncomeTokens int                        `json:"income_tokens"`
	Statements   map[string]matrixStatement `json:"statements"`
}

// matrixStatement keeps the page's own statement list separate from the
// combined preload. The income page embeds both: the 154/108/74 list is the
// template signature, while the ~500-type preload carries the page-computed
// rows (pretax, NI, EPS, WASO, EBIT, EBITDA). Applicability is the union; the
// signature is what makes a generic-shell response detectable.
type matrixStatement struct {
	Statement []string `json:"statement_codes"`
	Preload   []string `json:"preload_codes,omitempty"`
}

type matrixEvidence struct {
	CapturedAt string           `json:"captured_at"`
	Templates  []matrixTemplate `json:"templates"`
}

// expectedIncomeTypes maps a template to the page's income-list size. It is a
// sanity gate, not a derivation: a 503/429 page can serve a generic 154 list for
// every ticker, and a matrix built from that would be wrong.
var expectedIncomeTypes = map[financials.Taxonomy]int{
	financials.TaxonomyGeneral154:  154,
	financials.TaxonomyBank108:     108,
	financials.TaxonomyInsurance74: 74,
}

func runMatrix(args []string) {
	fs := flag.NewFlagSet("matrix", flag.ExitOnError)
	general := fs.String("general", "PTBA", "general-template representative")
	bank := fs.String("bank", "BBRI", "bank-template representative")
	insurance := fs.String("insurance", "ABDA", "insurance-template representative")
	out := fs.String("out", "yahoo-taxonomy-type-lists.json", "evidence JSON output (- for stdout)")
	matrixOut := fs.String("matrix-out", "yahoo-taxonomy-applicability.md", "applicability markdown output (- for none)")
	spacing := fs.Duration("spacing", time.Second, "delay between page fetches")
	_ = fs.Parse(args)

	client := yahoo.NewClient()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	representatives := []matrixTemplate{
		{Taxonomy: financials.TaxonomyGeneral154, Ticker: *general},
		{Taxonomy: financials.TaxonomyBank108, Ticker: *bank},
		{Taxonomy: financials.TaxonomyInsurance74, Ticker: *insurance},
	}
	for i := range representatives {
		tpl := &representatives[i]
		tpl.Statements = make(map[string]matrixStatement, 3)
		for _, statement := range []string{"financials", "balance-sheet", "cash-flow"} {
			statementCodes, preloadCodes, tokens, err := discoverStatementCodes(ctx, client, tpl.Ticker, statement)
			if err != nil {
				fatalf("matrix: %s/%s: %v", tpl.Ticker, statement, err)
			}
			if statement == "financials" {
				tpl.IncomeTokens = tokens
				want := expectedIncomeTypes[tpl.Taxonomy]
				if tokens != want {
					fatalf("matrix: %s income list has %d type tokens, want %d (%s); page may have served a generic shell",
						tpl.Ticker, tokens, want, tpl.Taxonomy)
				}
			}
			tpl.Statements[statement] = matrixStatement{Statement: statementCodes, Preload: preloadCodes}
			sleepCtx(ctx, *spacing)
		}
		fmt.Fprintf(os.Stderr, "financials-taxonomy: %s %s income=%d+%d balance=%d cashflow=%d\n",
			tpl.Taxonomy, tpl.Ticker,
			len(tpl.Statements["financials"].Statement), len(tpl.Statements["financials"].Preload),
			len(tpl.Statements["balance-sheet"].Statement), len(tpl.Statements["cash-flow"].Statement))
	}

	ev := matrixEvidence{
		CapturedAt: time.Now().UTC().Format(time.RFC3339),
		Templates:  representatives,
	}
	if err := writeJSON(*out, ev); err != nil {
		fatalf("matrix: write %s: %v", *out, err)
	}
	if *matrixOut != "-" {
		if err := writeApplicabilityMarkdown(*matrixOut, representatives); err != nil {
			fatalf("matrix: write %s: %v", *matrixOut, err)
		}
	}
}

// discoverStatementCodes fetches one statement page and returns the codes from
// the smallest embedded type list (the statement list), the codes from every
// other embedded group (the combined preload, income page only), and the
// statement list's raw token count. The token count is the page's own
// 154/108/74 signature and is checked before the codes are trusted.
func discoverStatementCodes(ctx context.Context, c *yahoo.Client, ticker, statement string) (statementCodes, preloadCodes []string, tokens int, err error) {
	list, err := c.DiscoverTypeList(ctx, ticker, statement)
	if err != nil {
		return nil, nil, 0, err
	}
	if len(list.Groups) == 0 {
		return nil, nil, 0, yahoo.ErrNoTypeList
	}
	best := 0
	for i, g := range list.Groups {
		if len(g.Types) < len(list.Groups[best].Types) {
			best = i
		}
	}
	preload := make([]string, 0)
	for i, g := range list.Groups {
		if i == best {
			continue
		}
		preload = append(preload, g.Types...)
	}
	return codeSet(list.Groups[best].Types), codeSet(preload), len(list.Groups[best].Types), nil
}

// codeSet strips _12M/_3M/_TTM, deduplicates and sorts.
func codeSet(tokens []string) []string {
	seen := make(map[string]struct{}, len(tokens))
	out := make([]string, 0, len(tokens))
	for _, token := range tokens {
		code := strings.TrimSpace(token)
		for _, suffix := range []string{"_12M", "_3M", "_TTM"} {
			code = strings.TrimSuffix(code, suffix)
		}
		if code == "" {
			continue
		}
		if _, ok := seen[code]; ok {
			continue
		}
		seen[code] = struct{}{}
		out = append(out, code)
	}
	sort.Strings(out)
	return out
}

func writeJSON(path string, v interface{}) error {
	var w io.Writer = os.Stdout
	if path != "-" {
		f, err := os.Create(path)
		if err != nil {
			return err
		}
		defer f.Close()
		w = f
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func writeApplicabilityMarkdown(path string, templates []matrixTemplate) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	byTaxonomy := make(map[financials.Taxonomy]map[string]bool, len(templates))
	for _, tpl := range templates {
		union := make(map[string]bool)
		for _, stmt := range tpl.Statements {
			for _, c := range stmt.Statement {
				union[c] = true
			}
			for _, c := range stmt.Preload {
				union[c] = true
			}
		}
		byTaxonomy[tpl.Taxonomy] = union
	}

	var b strings.Builder
	b.WriteString("# Yahoo taxonomy applicability matrix\n\n")
	b.WriteString("Generated by `financials-taxonomy matrix` from the statement pages' own type lists.\n")
	b.WriteString("A concept is applicable when any of its data-ID aliases appears in the union of the\n")
	b.WriteString("income, balance-sheet and cash-flow type lists for that template. `N/A` means the\n")
	b.WriteString("line does not exist by design; it is not an issuer omission. `unmapped` means the\n")
	b.WriteString("mapped code is absent, but the template may carry the same business line under a\n")
	b.WriteString("bank/insurance-specific code that is not in the concept map yet — the runtime\n")
	b.WriteString("coverage stays `unknown`, never `not_applicable`.\n\n")
	b.WriteString("| Concept | general_154 | bank_108 | insurance_74 | Codes |\n")
	b.WriteString("|---|---|---|---|---|\n")
	for _, c := range financials.CoreConcepts {
		fmt.Fprintf(&b, "| `%s` | %s | %s | %s | `%s` |\n",
			c,
			applicabilityCell(financials.TaxonomyGeneral154, byTaxonomy[financials.TaxonomyGeneral154], c),
			applicabilityCell(financials.TaxonomyBank108, byTaxonomy[financials.TaxonomyBank108], c),
			applicabilityCell(financials.TaxonomyInsurance74, byTaxonomy[financials.TaxonomyInsurance74], c),
			strings.Join(yahoo.AliasesFor(c), ","))
	}
	b.WriteString("\nRepresentatives: ")
	parts := make([]string, 0, len(templates))
	for _, tpl := range templates {
		parts = append(parts, fmt.Sprintf("%s = %s", tpl.Taxonomy, tpl.Ticker))
	}
	b.WriteString(strings.Join(parts, ", "))
	b.WriteString(fmt.Sprintf("; captured %s UTC.\n", time.Now().UTC().Format("2006-01-02")))

	_, err = f.WriteString(b.String())
	return err
}

// applicabilityCell reports one concept in one template. A derived-only concept
// has no codes and is applicable by construction. An absent mapped code is only
// N/A when financials.Applicable says the template cannot carry the concept;
// otherwise it is unmapped, because bank/insurance templates use their own
// codes for the same business lines.
func applicabilityCell(taxonomy financials.Taxonomy, codes map[string]bool, concept financials.Concept) string {
	aliases := yahoo.AliasesFor(concept)
	if len(aliases) == 0 {
		return "yes (derived)"
	}
	for _, code := range aliases {
		if codes[code] {
			return "yes"
		}
	}
	if !financials.Applicable(taxonomy, concept) {
		return "N/A"
	}
	return "unmapped"
}

func sleepCtx(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
