package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/config"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/storage"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
)

// cmd/measure is the Measurement Harness — the offline, read-only grader of the
// pipeline's claims. It replays named filter sets as of past Trading Days,
// scores the survivors against the Liquidity Baseline and the Big-Cap Basket at
// 5/10/20-day Forward Returns, and writes a sealed JSON + markdown report to R2
// (ADR-0016). It never writes to the database and the MCP server never depends
// on it. The named filter sets come from the Active Filter Set's Config Document
// on R2; the Decision Journal comes from the measurement_journal table. `migrate`
// is the one-shot upload of the pre-R2 artifacts.
func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}
	switch args[0] {
	case "grade":
		runGrade(args[1:])
	case "migrate":
		runMigrate(args[1:])
	case "help", "-h", "--help":
		usage()
	default:
		// A bare `measure --as-of ...` is the screen grader, so ticket 01's
		// shipped invocation keeps working beside `grade screen`.
		if strings.HasPrefix(args[0], "-") {
			runGradeScreen(args)
			return
		}
		fatalf("measure: unknown command %q; expected `grade screen` or `migrate`", args[0])
	}
}

// newHarness builds the shared Measurement Harness (ADR-0017) around an R2 store
// and a database. db may be nil for the R2-only `migrate` command.
func newHarness(vip *viper.Viper, log *logrus.Logger, db *sqlx.DB, revision string) *usecase.MeasurementHarness {
	client := config.NewR2Client(vip, log)
	objects := storage.NewR2Store(client, vip.GetString("r2.bucket"))
	return usecase.NewMeasurementHarness(db, log, objects, revision)
}

// resolveRevision returns the flag value when set, else the working-tree
// revision. The harness is a local, in-repo tool, so a missing git binary
// degrades to "unknown" rather than failing a measurement.
func resolveRevision(flagValue string) string {
	if rev := strings.TrimSpace(flagValue); rev != "" {
		return rev
	}
	return gitRevision()
}

// publishReport seals a rendered report to R2, updating the bare latest keys.
func publishReport(store *usecase.MeasurementReportStore, artifact usecase.ReportArtifact, log *logrus.Logger) {
	jsonKey, mdKey, err := store.Sealed(context.Background(), artifact)
	if err != nil {
		log.Fatalf("measure: write report: %v", err)
	}
	log.Infof("measure: wrote %s and %s", jsonKey, mdKey)
}

// runGrade dispatches the `grade <grader>` group. The screen, anomaly, journal,
// flow and disclosure graders are built.
func runGrade(args []string) {
	if len(args) == 0 {
		fatalf("measure: grade requires a grader; expected one of: screen, anomalies, journal, flow, disclosures")
	}
	switch args[0] {
	case "screen":
		runGradeScreen(args[1:])
	case "anomalies":
		runGradeAnomalies(args[1:])
	case "journal":
		runGradeJournal(args[1:])
	case "flow":
		runGradeFlow(args[1:])
	case "disclosures":
		runGradeDisclosures(args[1:])
	default:
		fatalf("measure: unknown grader %q; known: screen, anomalies, journal, flow, disclosures", args[0])
	}
}

// runGradeScreen parses and runs the screen grader: one or more anchors, one
// named filter set, and optionally a second to compare against. The filter sets
// and the Config Hash come from the Active Filter Set on R2.
func runGradeScreen(args []string) {
	flags := flag.NewFlagSet("grade screen", flag.ExitOnError)
	var asOfs stringList
	flags.Var(&asOfs, "as-of", "Replay anchor Trading Day, YYYY-MM-DD (repeatable, required)")
	filters := flags.String("filters", "", "named filter set to grade (default: the Active Filter Set)")
	compare := flags.String("compare", "", "optional second named filter set to compare against")
	holdoutStartStr := flags.String("holdout-start", "", "seal date, YYYY-MM-DD: restrict anchors to on/after it (optional)")
	revision := flags.String("revision", "", "git revision to stamp (default: derived from the working tree)")
	_ = flags.Parse(args)

	vip := config.NewViper()
	log := config.NewLogger(vip)

	opts, err := parseGradeScreenOptions(asOfs, *filters, *compare, *holdoutStartStr)
	if err != nil {
		log.Fatalf("measure: %v", err)
	}

	db := config.NewDatabase(vip, log)
	defer db.Close()

	h := newHarness(vip, log, db, resolveRevision(*revision))

	grade, err := h.GradeScreen(context.Background(), opts)
	if err != nil {
		log.Fatalf("measure: %v", err)
	}
	if grade.ConfigHash == "" {
		log.Infof("measure: no Active Filter Set published on R2; graded the built-in default set")
	} else {
		log.Infof("measure: graded the Active Filter Set %q (config %s)", grade.ActiveFilterSet, grade.ConfigHash)
	}

	artifact, err := grade.Artifact()
	if err != nil {
		log.Fatalf("measure: render report: %v", err)
	}
	publishReport(h.Reports, artifact, log)

	// The markdown summary also goes to stdout: the tool computes, the reader
	// interprets — no need to open the object to see the headline.
	fmt.Println(usecase.RenderScreenGradeMarkdown(grade))
}

// runGradeAnomalies parses and runs the anomaly grader: one or more anchors, an
// optional holdout seal, and the Flow Lead-in Window length.
func runGradeAnomalies(args []string) {
	flags := flag.NewFlagSet("grade anomalies", flag.ExitOnError)
	var asOfs stringList
	flags.Var(&asOfs, "as-of", "Replay anchor Trading Day, YYYY-MM-DD (repeatable, required)")
	flowWindow := flags.Int("flow-window-days", 0, "Flow Lead-in Window length in trading days (default: 20)")
	holdoutStartStr := flags.String("holdout-start", "", "seal date, YYYY-MM-DD: restrict anchors to on/after it (optional)")
	revision := flags.String("revision", "", "git revision to stamp (default: derived from the working tree)")
	_ = flags.Parse(args)

	vip := config.NewViper()
	log := config.NewLogger(vip)

	opts, err := parseGradeAnomalyOptions(asOfs, *flowWindow, *holdoutStartStr)
	if err != nil {
		log.Fatalf("measure: %v", err)
	}

	db := config.NewDatabase(vip, log)
	defer db.Close()

	h := newHarness(vip, log, db, resolveRevision(*revision))

	grade, err := h.GradeAnomalies(context.Background(), opts)
	if err != nil {
		log.Fatalf("measure: %v", err)
	}

	artifact, err := grade.Artifact()
	if err != nil {
		log.Fatalf("measure: render report: %v", err)
	}
	publishReport(h.Reports, artifact, log)

	fmt.Println(usecase.RenderAnomalyGradeMarkdown(grade))
}

// runGradeFlow parses and runs the flow grader: one or more anchors, an optional
// holdout seal, and the Flow Column window length. It needs no screener — its
// Liquidity Baseline is the hard-filter stage read directly — but it does need
// the foreign-net read behind the screener's Flow Column.
func runGradeFlow(args []string) {
	flags := flag.NewFlagSet("grade flow", flag.ExitOnError)
	var asOfs stringList
	flags.Var(&asOfs, "as-of", "Replay anchor Trading Day, YYYY-MM-DD (repeatable, required)")
	flowWindow := flags.Int("flow-window-days", 0, "Flow Column window length in trading days (default: 20)")
	holdoutStartStr := flags.String("holdout-start", "", "seal date, YYYY-MM-DD: restrict anchors to on/after it (optional)")
	revision := flags.String("revision", "", "git revision to stamp (default: derived from the working tree)")
	_ = flags.Parse(args)

	vip := config.NewViper()
	log := config.NewLogger(vip)

	opts, err := parseGradeFlowOptions(asOfs, *flowWindow, *holdoutStartStr)
	if err != nil {
		log.Fatalf("measure: %v", err)
	}

	db := config.NewDatabase(vip, log)
	defer db.Close()

	h := newHarness(vip, log, db, resolveRevision(*revision))

	grade, err := h.GradeFlow(context.Background(), opts)
	if err != nil {
		log.Fatalf("measure: %v", err)
	}

	artifact, err := grade.Artifact()
	if err != nil {
		log.Fatalf("measure: render report: %v", err)
	}
	publishReport(h.Reports, artifact, log)

	fmt.Println(usecase.RenderFlowGradeMarkdown(grade))
}

// runGradeDisclosures parses and runs the disclosure grader: one or more Filing
// Days, the fixed Reject Sample size, and an optional holdout seal. It replays
// no Screen (the Liquidity Baseline is read directly) but reads disclosures.
func runGradeDisclosures(args []string) {
	flags := flag.NewFlagSet("grade disclosures", flag.ExitOnError)
	var asOfs stringList
	flags.Var(&asOfs, "as-of", "Filing Day (disclosure announcement date), YYYY-MM-DD (repeatable, required)")
	rejectSample := flags.Int("reject-sample", 0, "manual Reject Sample size in rejected filings (default: 20)")
	holdoutStartStr := flags.String("holdout-start", "", "seal date, YYYY-MM-DD: restrict filing days to on/after it (optional)")
	revision := flags.String("revision", "", "git revision to stamp (default: derived from the working tree)")
	_ = flags.Parse(args)

	vip := config.NewViper()
	log := config.NewLogger(vip)

	opts, err := parseGradeDisclosuresOptions(asOfs, *rejectSample, *holdoutStartStr)
	if err != nil {
		log.Fatalf("measure: %v", err)
	}

	db := config.NewDatabase(vip, log)
	defer db.Close()

	h := newHarness(vip, log, db, resolveRevision(*revision))

	grade, err := h.GradeDisclosures(context.Background(), opts)
	if err != nil {
		log.Fatalf("measure: %v", err)
	}

	artifact, err := grade.Artifact()
	if err != nil {
		log.Fatalf("measure: render report: %v", err)
	}
	publishReport(h.Reports, artifact, log)

	fmt.Println(usecase.RenderDisclosureGradeMarkdown(grade))
}

// runGradeJournal parses and runs the journal grader: the optional decision-date
// window, and nothing else — the journal grader replays no Screen and reads no
// Liquidity Baseline, only the logged Verdicts in measurement_journal and stored
// prices.
func runGradeJournal(args []string) {
	flags := flag.NewFlagSet("grade journal", flag.ExitOnError)
	fromStr := flags.String("from", "", "earliest decision date to grade, YYYY-MM-DD (inclusive, optional)")
	untilStr := flags.String("until", "", "latest decision date to grade, YYYY-MM-DD (inclusive, optional)")
	revision := flags.String("revision", "", "git revision to stamp (default: derived from the working tree)")
	_ = flags.Parse(args)

	vip := config.NewViper()
	log := config.NewLogger(vip)

	opts, err := parseGradeJournalOptions(*fromStr, *untilStr)
	if err != nil {
		log.Fatalf("measure: %v", err)
	}

	db := config.NewDatabase(vip, log)
	defer db.Close()

	h := newHarness(vip, log, db, resolveRevision(*revision))

	grade, err := h.GradeJournal(context.Background(), opts)
	if err != nil {
		log.Fatalf("measure: %v", err)
	}

	artifact, err := grade.Artifact()
	if err != nil {
		log.Fatalf("measure: render report: %v", err)
	}
	publishReport(h.Reports, artifact, log)

	fmt.Println(usecase.RenderJournalGradeMarkdown(grade))
}

// runMigrate is the one-shot upload of the pre-R2 artifacts (ticket 07b): every
// `measurement/reports/<base>.{json,md}` pair is sealed to R2, and the local
// filter sets are published as the Active Filter Set's Config Document. It is
// idempotent — report versions are content-addressed and the config version is
// hashed — so it can be re-run safely from any checkout that still has the files.
func runMigrate(args []string) {
	flags := flag.NewFlagSet("migrate", flag.ExitOnError)
	reportsDir := flags.String("reports-dir", "measurement/reports", "directory of pre-R2 report files to seal")
	filtersetsDir := flags.String("filtersets-dir", usecase.MeasurementFilterSetDir, "directory of pre-R2 filter sets to seed the Config Document")
	active := flags.String("active", "default", "the filter set to mark active in the seeded Config Document")
	_ = flags.Parse(args)

	vip := config.NewViper()
	log := config.NewLogger(vip)

	h := newHarness(vip, log, nil, "unknown")
	ctx := context.Background()

	reports, err := migrateReports(ctx, h.Reports, *reportsDir)
	if err != nil {
		log.Fatalf("measure: migrate reports: %v", err)
	}
	log.Infof("measure: sealed %d report(s) from %s", reports, *reportsDir)

	hash, err := h.Config.SeedFromDir(ctx, *filtersetsDir, *active)
	if err != nil {
		log.Fatalf("measure: migrate config: %v", err)
	}
	log.Infof("measure: seeded Active Filter Set from %s, active %q, config %s", *filtersetsDir, *active, hash)
}

// migrateReports seals every <base>.{json,md} pair under dir to R2.
func migrateReports(ctx context.Context, store *usecase.MeasurementReportStore, dir string) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}

	sealed := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(strings.ToLower(name), ".json") {
			continue
		}
		base := strings.TrimSuffix(name, filepath.Ext(name))
		payload, err := os.ReadFile(filepath.Join(dir, base+".json"))
		if err != nil {
			return sealed, err
		}
		markdown, err := os.ReadFile(filepath.Join(dir, base+".md"))
		if err != nil {
			return sealed, fmt.Errorf("report %s has no .md pair: %w", base, err)
		}
		if _, _, err := store.Sealed(ctx, usecase.ReportArtifact{Base: base, JSON: payload, Markdown: markdown}); err != nil {
			return sealed, err
		}
		sealed++
	}
	return sealed, nil
}

// parseGradeJournalOptions validates the `grade journal` window flags. Both are
// optional; when both are given, from must not be after until.
func parseGradeJournalOptions(fromStr, untilStr string) (usecase.JournalGradeOptions, error) {
	var opts usecase.JournalGradeOptions
	if strings.TrimSpace(fromStr) != "" {
		from, err := parseMeasurementDay(fromStr, "--from")
		if err != nil {
			return usecase.JournalGradeOptions{}, err
		}
		opts.From = &from
	}
	if strings.TrimSpace(untilStr) != "" {
		until, err := parseMeasurementDay(untilStr, "--until")
		if err != nil {
			return usecase.JournalGradeOptions{}, err
		}
		opts.Until = &until
	}
	if opts.From != nil && opts.Until != nil && opts.From.After(*opts.Until) {
		return usecase.JournalGradeOptions{}, fmt.Errorf("%w: --from %s is after --until %s",
			usecase.ErrInvalidArgument, opts.From.Format("2006-01-02"), opts.Until.Format("2006-01-02"))
	}
	return opts, nil
}

// parseGradeAnomalyOptions validates the `grade anomalies` flags. The
// flow-window bound is checked by the engine, so a CLI parse never diverges from
// the grader's own rule.
func parseGradeAnomalyOptions(asOfs []string, flowWindow int, holdoutStartStr string) (usecase.AnomalyGradeOptions, error) {
	if len(asOfs) == 0 {
		return usecase.AnomalyGradeOptions{}, fmt.Errorf("%w: --as-of is required (YYYY-MM-DD)", usecase.ErrInvalidArgument)
	}
	anchors := make([]time.Time, 0, len(asOfs))
	for _, raw := range asOfs {
		day, err := parseMeasurementDay(raw, "--as-of")
		if err != nil {
			return usecase.AnomalyGradeOptions{}, err
		}
		anchors = append(anchors, day)
	}

	opts := usecase.AnomalyGradeOptions{Anchors: anchors, FlowWindowDays: flowWindow}
	if strings.TrimSpace(holdoutStartStr) != "" {
		seal, err := parseMeasurementDay(holdoutStartStr, "--holdout-start")
		if err != nil {
			return usecase.AnomalyGradeOptions{}, err
		}
		opts.HoldoutStart = &seal
	}
	return opts, nil
}

// parseGradeFlowOptions validates the `grade flow` flags. The flow-window bound
// is checked by the engine, so a CLI parse never diverges from the grader's rule.
func parseGradeFlowOptions(asOfs []string, flowWindow int, holdoutStartStr string) (usecase.FlowGradeOptions, error) {
	if len(asOfs) == 0 {
		return usecase.FlowGradeOptions{}, fmt.Errorf("%w: --as-of is required (YYYY-MM-DD)", usecase.ErrInvalidArgument)
	}
	anchors := make([]time.Time, 0, len(asOfs))
	for _, raw := range asOfs {
		day, err := parseMeasurementDay(raw, "--as-of")
		if err != nil {
			return usecase.FlowGradeOptions{}, err
		}
		anchors = append(anchors, day)
	}

	opts := usecase.FlowGradeOptions{Anchors: anchors, FlowWindowDays: flowWindow}
	if strings.TrimSpace(holdoutStartStr) != "" {
		seal, err := parseMeasurementDay(holdoutStartStr, "--holdout-start")
		if err != nil {
			return usecase.FlowGradeOptions{}, err
		}
		opts.HoldoutStart = &seal
	}
	return opts, nil
}

// parseGradeDisclosuresOptions validates the `grade disclosures` flags. The
// reject-sample bound is checked by the engine, so a CLI parse never diverges
// from the grader's rule.
func parseGradeDisclosuresOptions(asOfs []string, rejectSample int, holdoutStartStr string) (usecase.DisclosureGradeOptions, error) {
	if len(asOfs) == 0 {
		return usecase.DisclosureGradeOptions{}, fmt.Errorf("%w: --as-of is required (YYYY-MM-DD)", usecase.ErrInvalidArgument)
	}
	anchors := make([]time.Time, 0, len(asOfs))
	for _, raw := range asOfs {
		day, err := parseMeasurementDay(raw, "--as-of")
		if err != nil {
			return usecase.DisclosureGradeOptions{}, err
		}
		anchors = append(anchors, day)
	}

	opts := usecase.DisclosureGradeOptions{Anchors: anchors, RejectSampleN: rejectSample}
	if strings.TrimSpace(holdoutStartStr) != "" {
		seal, err := parseMeasurementDay(holdoutStartStr, "--holdout-start")
		if err != nil {
			return usecase.DisclosureGradeOptions{}, err
		}
		opts.HoldoutStart = &seal
	}
	return opts, nil
}

// parseGradeScreenOptions validates the `grade screen` flags. Errors are
// structured (usecase.ErrInvalidArgument) so a scripting caller can tell a bad
// request from a read failure.
func parseGradeScreenOptions(asOfs []string, filters, compare, holdoutStartStr string) (usecase.ScreenGradeOptions, error) {
	if len(asOfs) == 0 {
		return usecase.ScreenGradeOptions{}, fmt.Errorf("%w: --as-of is required (YYYY-MM-DD)", usecase.ErrInvalidArgument)
	}
	anchors := make([]time.Time, 0, len(asOfs))
	for _, raw := range asOfs {
		day, err := parseMeasurementDay(raw, "--as-of")
		if err != nil {
			return usecase.ScreenGradeOptions{}, err
		}
		anchors = append(anchors, day)
	}

	opts := usecase.ScreenGradeOptions{Anchors: anchors, FilterSet: filters, Compare: compare}
	if strings.TrimSpace(holdoutStartStr) != "" {
		seal, err := parseMeasurementDay(holdoutStartStr, "--holdout-start")
		if err != nil {
			return usecase.ScreenGradeOptions{}, err
		}
		opts.HoldoutStart = &seal
	}
	return opts, nil
}

// parseMeasurementOptions validates the legacy single-anchor flags. Retained so
// the flat invocation's validation is unchanged and directly testable.
func parseMeasurementOptions(asOfStr, filters, holdoutStartStr string) (usecase.MeasurementOptions, error) {
	if strings.TrimSpace(asOfStr) == "" {
		return usecase.MeasurementOptions{}, fmt.Errorf("%w: --as-of is required (YYYY-MM-DD)", usecase.ErrInvalidArgument)
	}
	asOf, err := parseMeasurementDay(asOfStr, "--as-of")
	if err != nil {
		return usecase.MeasurementOptions{}, err
	}

	opts := usecase.MeasurementOptions{AsOf: asOf, FilterSet: filters}
	if strings.TrimSpace(holdoutStartStr) != "" {
		seal, err := parseMeasurementDay(holdoutStartStr, "--holdout-start")
		if err != nil {
			return usecase.MeasurementOptions{}, err
		}
		opts.HoldoutStart = &seal
	}
	return opts, nil
}

// parseMeasurementDay parses a YYYY-MM-DD flag, wrapping the failure in the
// structured invalid-argument error.
func parseMeasurementDay(value, flagName string) (time.Time, error) {
	day, err := time.Parse("2006-01-02", strings.TrimSpace(value))
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: %s must be YYYY-MM-DD, got %q", usecase.ErrInvalidArgument, flagName, value)
	}
	return day, nil
}

// stringList is a repeatable string flag: each occurrence appends one value.
type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }

func (s *stringList) Set(value string) error {
	*s = append(*s, value)
	return nil
}

// gitRevision stamps the report with the working tree's revision. The harness is
// a local, in-repo tool, so a missing git binary degrades to "unknown" rather
// than failing a measurement.
func gitRevision() string {
	out, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}

// fatalf prints a usage error to stderr and exits nonzero.
func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(2)
}

// usage prints the top-level command shape.
func usage() {
	fmt.Fprint(os.Stderr, `measure — the Measurement Harness (read-only grader)

Usage:
  measure grade screen --as-of <date> [--as-of <date> ...] [--filters <name>] [--compare <name>] [flags]
  measure grade anomalies --as-of <date> [--as-of <date> ...] [--flow-window-days <n>] [flags]
  measure grade flow --as-of <date> [--as-of <date> ...] [--flow-window-days <n>] [flags]
  measure grade disclosures --as-of <date> [--as-of <date> ...] [--reject-sample <n>] [flags]
  measure grade journal [--from <date>] [--until <date>] [flags]
  measure migrate [--reports-dir <dir>] [--filtersets-dir <dir>] [--active <name>]

The bare form `+"`measure --as-of <date>`"+` is an alias for `+"`grade screen`"+`.

Reports are sealed to R2 (measurement/reports/<as-of>_<name>.{json,md} plus an
immutable version); the named filter sets come from the Active Filter Set's
Config Document on R2 and the Decision Journal from the measurement_journal
table. `+"`migrate`"+` uploads the pre-R2 files once.

Flags:
  --as-of <date>        Replay anchor Trading Day, or a disclosure Filing Day, YYYY-MM-DD (repeatable, required)
  --filters <name>      named filter set to grade (default: the Active Filter Set)
  --compare <name>      optional second named filter set to compare against
  --flow-window-days <n> Flow window length in trading days: Flow Lead-in Window for the anomaly grader, Flow Column window for the flow grader (default: 20)
  --reject-sample <n>   manual Reject Sample size for the disclosure grader (default: 20)
  --from <date>         earliest decision date for the journal grader, YYYY-MM-DD (optional)
  --until <date>        latest decision date for the journal grader, YYYY-MM-DD (optional)
  --holdout-start <date> seal date: restrict anchors to on/after it
  --reports-dir <dir>   migrate: directory of pre-R2 report files (default: measurement/reports)
  --filtersets-dir <dir> migrate: directory of pre-R2 filter sets (default: measurement/filtersets)
  --active <name>       migrate: filter set to mark active (default: default)
  --revision <rev>      git revision to stamp (default: derived from the tree)
`)
}
