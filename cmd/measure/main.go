package main

import (
	"context"
	"flag"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/config"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
)

// cmd/measure is the Measurement Harness — the offline, read-only grader of the
// pipeline's claims. It replays a named filter set as of a past Trading Day,
// scores the survivors against the Liquidity Baseline and the Big-Cap Basket at
// 5/10/20-day Forward Returns, and writes a JSON + markdown report under
// measurement/reports/. It never writes to the database and the MCP server never
// depends on it.
func main() {
	asOfStr := flag.String("as-of", "", "Replay anchor Trading Day, YYYY-MM-DD (required)")
	filters := flag.String("filters", "default", "named filter set to grade")
	filtersetsDir := flag.String("filtersets-dir", usecase.MeasurementFilterSetDir, "directory of <name>.json filter sets")
	holdoutStartStr := flag.String("holdout-start", "", "seal date, YYYY-MM-DD: restrict anchors to on/after it (optional)")
	reportsDir := flag.String("reports-dir", usecase.MeasurementReportDir, "directory for the JSON + markdown report")
	revision := flag.String("revision", "", "git revision to stamp (default: derived from the working tree)")
	flag.Parse()

	vip := config.NewViper()
	log := config.NewLogger(vip)

	opts, err := parseMeasurementOptions(*asOfStr, *filters, *holdoutStartStr)
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

	report, err := engine.Measure(context.Background(), opts)
	if err != nil {
		log.Fatalf("measure: %v", err)
	}

	jsonPath, mdPath, err := report.WriteFiles(*reportsDir)
	if err != nil {
		log.Fatalf("measure: write report: %v", err)
	}
	log.Infof("measure: wrote %s and %s", jsonPath, mdPath)

	// The markdown summary also goes to stdout: the tool computes, the reader
	// interprets — no need to open the file to see the headline.
	fmt.Println(usecase.RenderMeasurementMarkdown(report))
}

// parseMeasurementOptions validates the CLI's date and filter-set flags. The
// errors are structured (usecase.ErrInvalidArgument) so a scripting caller can
// tell a bad request from a read failure.
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
