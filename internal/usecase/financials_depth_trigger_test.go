package usecase

import (
	"context"
	"testing"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

// fakeDepthRequester records the read path's verdict and returns a fixed
// status, standing in for the enqueueing requester.
type fakeDepthRequester struct {
	missing []bool
	err     error
	status  string
}

func (f *fakeDepthRequester) RequireDepth(_ context.Context, _ string, depthMissing bool) (string, error) {
	f.missing = append(f.missing, depthMissing)
	if f.err != nil {
		return "", f.err
	}
	if f.status != "" {
		return f.status, nil
	}
	if depthMissing {
		return FinancialDepthPending, nil
	}
	return FinancialDepthObserved, nil
}

func depthCoverageDoc(doc string) *string { return &doc }

// TestGetFinancialsDepthTrigger pins the read-path half of issue 07: the
// newest stored statement decides whether depth is missing, the requester is
// told, and its status is surfaced — without the read ever fetching.
func TestGetFinancialsDepthTrigger(t *testing.T) {
	t.Run("missing depth asks the requester once", func(t *testing.T) {
		rows := []entity.FinancialStatement{
			finStatement("TLKM", "2026-09-30", 9),
			finStatement("TLKM", "2026-06-30", 6),
		}
		req := &fakeDepthRequester{}
		uc := newFinancialsTestUseCase(rows, nil)
		uc.Depth = req

		resp, err := uc.GetFinancials(context.Background(), "TLKM", "")
		if err != nil {
			t.Fatalf("GetFinancials: %v", err)
		}
		if len(req.missing) != 1 {
			t.Fatalf("requester calls = %d, want 1", len(req.missing))
		}
		if !req.missing[0] {
			t.Error("depth_missing = false, want true for an unenriched newest row")
		}
		if resp.DepthStatus != FinancialDepthPending {
			t.Errorf("depth_status = %q, want pending", resp.DepthStatus)
		}
	})

	t.Run("enriched newest row is a store hit", func(t *testing.T) {
		newest := finStatement("TLKM", "2026-09-30", 9)
		newest.DepthCoverage = depthCoverageDoc(`{"cash":"observed","gross_profit":"observed"}`)
		rows := []entity.FinancialStatement{
			newest,
			finStatement("TLKM", "2026-06-30", 6), // older rows may lack depth
		}
		req := &fakeDepthRequester{}
		uc := newFinancialsTestUseCase(rows, nil)
		uc.Depth = req

		resp, err := uc.GetFinancials(context.Background(), "TLKM", "")
		if err != nil {
			t.Fatalf("GetFinancials: %v", err)
		}
		if len(req.missing) != 1 || req.missing[0] {
			t.Fatalf("requester missing = %v, want one call with depth present", req.missing)
		}
		if resp.DepthStatus != FinancialDepthObserved {
			t.Errorf("depth_status = %q, want observed", resp.DepthStatus)
		}
	})

	t.Run("nil requester derives the status from coverage", func(t *testing.T) {
		rows := []entity.FinancialStatement{finStatement("TLKM", "2026-09-30", 9)}
		uc := newFinancialsTestUseCase(rows, nil)
		resp, err := uc.GetFinancials(context.Background(), "TLKM", "")
		if err != nil {
			t.Fatalf("GetFinancials: %v", err)
		}
		if resp.DepthStatus != FinancialDepthPending {
			t.Errorf("depth_status = %q, want pending", resp.DepthStatus)
		}

		rows[0].DepthCoverage = depthCoverageDoc(`{"cash":"not_applicable"}`)
		resp, err = uc.GetFinancials(context.Background(), "TLKM", "")
		if err != nil {
			t.Fatalf("GetFinancials: %v", err)
		}
		if resp.DepthStatus != FinancialDepthPending {
			t.Errorf("depth_status = %q, want pending when no depth value is usable", resp.DepthStatus)
		}

		rows[0].DepthCoverage = depthCoverageDoc(`{"cash":"observed","gross_profit":"not_applicable"}`)
		resp, err = uc.GetFinancials(context.Background(), "TLKM", "")
		if err != nil {
			t.Fatalf("GetFinancials: %v", err)
		}
		if resp.DepthStatus != FinancialDepthObserved {
			t.Errorf("depth_status = %q, want observed", resp.DepthStatus)
		}
	})

	t.Run("a requester error does not fail the read", func(t *testing.T) {
		rows := []entity.FinancialStatement{finStatement("TLKM", "2026-09-30", 9)}
		req := &fakeDepthRequester{err: context.DeadlineExceeded}
		uc := newFinancialsTestUseCase(rows, nil)
		uc.Depth = req

		resp, err := uc.GetFinancials(context.Background(), "TLKM", "")
		if err != nil {
			t.Fatalf("GetFinancials must not fail on a depth request error: %v", err)
		}
		if resp.DepthStatus != FinancialDepthPending {
			t.Errorf("depth_status = %q, want the coverage fallback pending", resp.DepthStatus)
		}
	})

	t.Run("no statements means no depth question", func(t *testing.T) {
		req := &fakeDepthRequester{}
		uc := newFinancialsTestUseCase(nil, nil)
		uc.Depth = req

		resp, err := uc.GetFinancials(context.Background(), "TLKM", "")
		if err != nil {
			t.Fatalf("GetFinancials: %v", err)
		}
		if len(req.missing) != 0 {
			t.Errorf("requester calls = %d, want 0 for a ticker with no statements", len(req.missing))
		}
		if resp.DepthStatus != "" {
			t.Errorf("depth_status = %q, want empty", resp.DepthStatus)
		}
	})
}
