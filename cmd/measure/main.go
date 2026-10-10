package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/config"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
)

// cmd/measure is the Measurement Harness — the offline, read-only grader of the
// pipeline's claims. It replays named filter sets as of past Trading Days,
// scores the survivors against the Liquidity Baseline and the Big-Cap Basket at
// 5/10/20-day Forward Returns, and writes a JSON + markdown report under
// measurement/reports/. It never writes to the database and the MCP server never
// depends on it.
func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}
	switch args[0] {
	case "grade":
		runGrade(args[1:])
	case "help", "-h", "--help":
		usage()
	default:
		// A bare `measure --as-of ...` is the screen grader, so ticket 01's
		// shipped invocation keeps working beside `grade screen`.
		if strings.HasPrefix(args[0], "-") {
			runGradeScreen(args)
			return
		}
		fatalf("measure: unknown command %q; expected `grade screen`", args[0])
	}
}

// runGrade dispatches the `grade <grader>` group. Only the screen grader is
// built; the other graders are named so a caller gets a clear refusal.
func runGrade(args []string) {
	if len(args) == 0 {
		fatalf("measure: grade requires a grader; expected one of: screen, anomalies, journal")
	}
	switch args[0] {
	case "screen":
		runGradeScreen(args[1:])
	case "anomalies":
		runGradeAnomalies(args[1:])
	case "journal":
		runGradeJournal(args[1:])
	case "flow", "disclosures":
		fatalf("measure: grade %s is not implemented yet", args[0])
	default:
		fatalf("measure: unknown grader %q; known: screen, anomalies, journal", args[0])
	}
}

// runGradeScreen parses and runs the screen grader: one or more anchors, one
// named filter set, and optionally a second to compare against.
func runGradeScreen(args []string) {
	flags := flag.NewFlagSet("grade screen", flag.ExitOnError)
	var asOfs stringList
	flags.Var(&asOfs, "as-of", "Replay anchor Trading Day, YYYY-MM-DD (repeatable, required)")
	filters := flags.String("filters", "default", "named filter set to grade")
	compare := flags.String("compare", "", "optional second named filter set to compare against")
	filtersetsDir := flags.String("filtersets-dir", usecase.MeasurementFilterSetDir, "directory of <name>.json filter sets")
	holdoutStartStr := flags.String("holdout-start", "", "seal date, YYYY-MM-DD: restrict anchors to on/after it (optional)")
	reportsDir := flags.String("reports-dir", usecase.MeasurementReportDir, "directory for the JSON + markdown report")
	revision := flags.String("revision", "", "git revision to stamp (default: derived from the working tree)")
	_ = flags.Parse(args)

	vip := config.NewViper()
	log := config.NewLogger(vip)

	opts, err := parseGradeScreenOptions(asOfs, *filters, *compare, *holdoutStartStr)
	if err != nil {
		log.Fatalf("measure: %v", err)
	}

	filterSets, err := usecase.NewMeasurementFilterRegistry(*filtersetsDir)
	if err != nil {
		log.Fatalf("measure: %v", err)
	}

	db := config.NewDatabase(vip, log)
	defer db.Close()

	rev := strings.TrimSpace(*revision)
	if rev == "" {
		rev = gitRevision()
	}

	priceRepo := repository.NewDailyPriceRepository(log)
	screen := usecase.NewScreenStocksUseCase(db, log, priceRepo, repository.NewBrokerStockSummaryRepository(log))
	engine := usecase.NewMeasurementEngine(db, log, screen, priceRepo, filterSets, rev)

	grade, err := engine.GradeScreen(context.Background(), opts)
	if err != nil {
		log.Fatalf("measure: %v", err)
	}

	jsonPath, mdPath, err := grade.WriteFiles(*reportsDir)
	if err != nil {
		log.Fatalf("measure: write report: %v", err)
	}
	log.Infof("measure: wrote %s and %s", jsonPath, mdPath)

	// The markdown summary also goes to stdout: the tool computes, the reader
	// interprets — no need to open the file to see the headline.
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
	reportsDir := flags.String("reports-dir", usecase.MeasurementReportDir, "directory for the JSON + markdown report")
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

	rev := strings.TrimSpace(*revision)
	if rev == "" {
		rev = gitRevision()
	}

	// The anomaly grader needs no screener: its Liquidity Baseline is the
	// hard-filter stage read directly, so the structural funnel never runs.
	priceRepo := repository.NewDailyPriceRepository(log)
	engine := usecase.NewMeasurementEngine(db, log, nil, priceRepo, nil, rev)
	engine.Anomalies = repository.NewAnomalyRepository(log)
	engine.Disclosures = repository.NewDisclosureRepository(log)
	engine.BrokerNet = repository.NewBrokerStockSummaryRepository(log)

	grade, err := engine.GradeAnomalies(context.Background(), opts)
	if err != nil {
		log.Fatalf("measure: %v", err)
	}

	jsonPath, mdPath, err := grade.WriteFiles(*reportsDir)
	if err != nil {
		log.Fatalf("measure: write report: %v", err)
	}
	log.Infof("measure: wrote %s and %s", jsonPath, mdPath)

	fmt.Println(usecase.RenderAnomalyGradeMarkdown(grade))
}

// runGradeJournal parses and runs the journal grader: the optional decision-date
// window, and nothing else — the journal grader replays no Screen and reads no
// Liquidity Baseline, only the agent-written verdicts and stored prices.
func runGradeJournal(args []string) {
	flags := flag.NewFlagSet("grade journal", flag.ExitOnError)
	fromStr := flags.String("from", "", "earliest decision date to grade, YYYY-MM-DD (inclusive, optional)")
	untilStr := flags.String("until", "", "latest decision date to grade, YYYY-MM-DD (inclusive, optional)")
	journalDir := flags.String("journal-dir", usecase.MeasurementJournalDir, "directory of <YYYY-MM>.jsonl Decision Journal files")
	reportsDir := flags.String("reports-dir", usecase.MeasurementReportDir, "directory for the JSON + markdown report")
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

	rev := strings.TrimSpace(*revision)
	if rev == "" {
		rev = gitRevision()
	}

	priceRepo := repository.NewDailyPriceRepository(log)
	engine := usecase.NewMeasurementEngine(db, log, nil, nil, nil, rev)
	engine.Journal = usecase.NewJournalFileReader(*journalDir)
	engine.JournalPrices = priceRepo

	grade, err := engine.GradeJournal(context.Background(), opts)
	if err != nil {
		log.Fatalf("measure: %v", err)
	}

	jsonPath, mdPath, err := grade.WriteFiles(*reportsDir)
	if err != nil {
		log.Fatalf("measure: write report: %v", err)
	}
	log.Infof("measure: wrote %s and %s", jsonPath, mdPath)

	fmt.Println(usecase.RenderJournalGradeMarkdown(grade))
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
  measure grade journal [--from <date>] [--until <date>] [flags]

The bare form `+"`measure --as-of <date>`"+` is an alias for `+"`grade screen`"+`.

Flags:
  --as-of <date>        Replay anchor Trading Day, YYYY-MM-DD (repeatable, required)
  --filters <name>      named filter set to grade (default: "default")
  --compare <name>      optional second named filter set to compare against
  --flow-window-days <n> Flow Lead-in Window length for the anomaly grader (default: 20)
  --from <date>         earliest decision date for the journal grader, YYYY-MM-DD (optional)
  --until <date>        latest decision date for the journal grader, YYYY-MM-DD (optional)
  --holdout-start <date> seal date: restrict anchors to on/after it
  --filtersets-dir <dir> directory of <name>.json filter sets
  --journal-dir <dir>   directory of <YYYY-MM>.jsonl Decision Journal files
  --reports-dir <dir>   directory for the JSON + markdown report
  --revision <rev>      git revision to stamp (default: derived from the tree)
`)
}
