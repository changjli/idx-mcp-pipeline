package main

import (
	"errors"
	"testing"
	"time"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
)

// TestParseMeasurementOptions — the CLI boundary validates its dates as
// structured errors (never a panic), and an omitted anchor is refused.
func TestParseMeasurementOptions(t *testing.T) {
	cases := []struct {
		name           string
		asOf           string
		filters        string
		holdoutStart   string
		wantErr        error
		wantAsOf       time.Time
		wantFilterSet  string
		wantHoldoutSet bool
	}{
		{
			name:          "valid anchor with the default set",
			asOf:          "2026-06-01",
			filters:       "default",
			wantAsOf:      time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
			wantFilterSet: "default",
		},
		{
			name:           "valid anchor with a holdout seal",
			asOf:           "2026-06-01",
			filters:        "default",
			holdoutStart:   "2026-04-01",
			wantAsOf:       time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
			wantFilterSet:  "default",
			wantHoldoutSet: true,
		},
		{
			name:    "missing anchor",
			asOf:    "",
			filters: "default",
			wantErr: usecase.ErrInvalidArgument,
		},
		{
			name:    "malformed anchor",
			asOf:    "01-06-2026",
			filters: "default",
			wantErr: usecase.ErrInvalidArgument,
		},
		{
			name:         "malformed holdout seal",
			asOf:         "2026-06-01",
			filters:      "default",
			holdoutStart: "April",
			wantErr:      usecase.ErrInvalidArgument,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts, err := parseMeasurementOptions(tc.asOf, tc.filters, tc.holdoutStart)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !opts.AsOf.Equal(tc.wantAsOf) {
				t.Fatalf("as_of = %s, want %s", opts.AsOf, tc.wantAsOf)
			}
			if opts.FilterSet != tc.wantFilterSet {
				t.Fatalf("filter set = %q, want %q", opts.FilterSet, tc.wantFilterSet)
			}
			if (opts.HoldoutStart != nil) != tc.wantHoldoutSet {
				t.Fatalf("holdout set = %t, want %t", opts.HoldoutStart != nil, tc.wantHoldoutSet)
			}
		})
	}
}

// TestParseGradeScreenOptions — the grade-screen boundary accepts repeated
// anchors, passes the comparison set through, and refuses missing or malformed
// dates as structured errors.
func TestParseGradeScreenOptions(t *testing.T) {
	opts, err := parseGradeScreenOptions([]string{"2026-06-01", "2026-07-01"}, "momentum", "rsi", "2026-04-01")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(opts.Anchors) != 2 ||
		!opts.Anchors[0].Equal(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)) ||
		!opts.Anchors[1].Equal(time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("anchors = %v, want 2026-06-01 and 2026-07-01", opts.Anchors)
	}
	if opts.FilterSet != "momentum" || opts.Compare != "rsi" {
		t.Fatalf("sets = %q/%q, want momentum/rsi", opts.FilterSet, opts.Compare)
	}
	if opts.HoldoutStart == nil || !opts.HoldoutStart.Equal(time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("holdout = %v, want 2026-04-01", opts.HoldoutStart)
	}

	if _, err := parseGradeScreenOptions(nil, "default", "", ""); !errors.Is(err, usecase.ErrInvalidArgument) {
		t.Fatalf("missing anchor err = %v, want ErrInvalidArgument", err)
	}
	if _, err := parseGradeScreenOptions([]string{"June"}, "default", "", ""); !errors.Is(err, usecase.ErrInvalidArgument) {
		t.Fatalf("malformed anchor err = %v, want ErrInvalidArgument", err)
	}
}
