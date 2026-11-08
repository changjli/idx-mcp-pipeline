package usecase

import (
	"testing"
	"time"
)

func bfTime(y int, m time.Month, d int) time.Time {
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// TestResolveFinancialsBackfillBounds pins the defaults and the two bounds
// guards: the newest available month is the previous calendar month, the floor
// is 2021-01, and neither an unpublished nor a pre-floor month is accepted.
func TestResolveFinancialsBackfillBounds(t *testing.T) {
	runDate := bfTime(2026, time.November, 8)
	floor := bfTime(2021, time.January, 1)

	t.Run("defaults walk back from the previous month to the floor", func(t *testing.T) {
		from, to, err := resolveFinancialsBackfillBounds(time.Time{}, time.Time{}, runDate)
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if got, want := from, bfTime(2026, time.October, 1); !got.Equal(want) {
			t.Errorf("from = %s, want %s", got.Format("2006-01"), want.Format("2006-01"))
		}
		if !to.Equal(floor) {
			t.Errorf("to = %s, want floor %s", to.Format("2006-01"), floor.Format("2006-01"))
		}
	})

	t.Run("explicit bounds are normalised to the first of the month", func(t *testing.T) {
		from, to, err := resolveFinancialsBackfillBounds(
			bfTime(2024, time.March, 17), bfTime(2021, time.January, 31), runDate)
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if got, want := from, bfTime(2024, time.March, 1); !got.Equal(want) {
			t.Errorf("from = %s, want %s", got.Format("2006-01-02"), want.Format("2006-01-02"))
		}
		if got, want := to, bfTime(2021, time.January, 1); !got.Equal(want) {
			t.Errorf("to = %s, want %s", got.Format("2006-01-02"), want.Format("2006-01-02"))
		}
	})

	t.Run("a month before the floor is rejected", func(t *testing.T) {
		if _, _, err := resolveFinancialsBackfillBounds(bfTime(2020, time.December, 1), time.Time{}, runDate); err == nil {
			t.Fatal("expected an error for a pre-floor from bound")
		}
		if _, _, err := resolveFinancialsBackfillBounds(time.Time{}, bfTime(2020, time.December, 1), runDate); err == nil {
			t.Fatal("expected an error for a pre-floor to bound")
		}
	})

	t.Run("an unpublished month is rejected", func(t *testing.T) {
		if _, _, err := resolveFinancialsBackfillBounds(bfTime(2026, time.November, 1), time.Time{}, runDate); err == nil {
			t.Fatal("expected an error for the current (unpublished) month")
		}
	})
}

// TestFinancialsMonthsBetween pins the walk order in both directions and the
// inclusive bounds.
func TestFinancialsMonthsBetween(t *testing.T) {
	tests := []struct {
		name string
		from time.Time
		to   time.Time
		want []string
	}{
		{
			name: "backwards is inclusive",
			from: bfTime(2022, time.March, 1),
			to:   bfTime(2022, time.January, 1),
			want: []string{"2022-03", "2022-02", "2022-01"},
		},
		{
			name: "forwards is inclusive",
			from: bfTime(2021, time.January, 1),
			to:   bfTime(2021, time.March, 1),
			want: []string{"2021-01", "2021-02", "2021-03"},
		},
		{
			name: "a single month",
			from: bfTime(2024, time.June, 1),
			to:   bfTime(2024, time.June, 1),
			want: []string{"2024-06"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := financialsMonthsBetween(tc.from, tc.to)
			if len(got) != len(tc.want) {
				t.Fatalf("months = %d, want %d", len(got), len(tc.want))
			}
			for i, m := range got {
				if key := m.Format("2006-01"); key != tc.want[i] {
					t.Errorf("month[%d] = %s, want %s", i, key, tc.want[i])
				}
			}
		})
	}
}

// TestFinancialsBackfillDefaultRangeSize proves the default walk covers every
// month from the newest available down to the floor.
func TestFinancialsBackfillDefaultRangeSize(t *testing.T) {
	from, to, err := resolveFinancialsBackfillBounds(time.Time{}, time.Time{}, bfTime(2026, time.November, 8))
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	months := financialsMonthsBetween(from, to)
	if len(months) != 70 {
		t.Fatalf("default range = %d months, want 70 (2021-01 to 2026-10)", len(months))
	}
	if got := months[0].Format("2006-01"); got != "2026-10" {
		t.Errorf("first month = %s, want 2026-10", got)
	}
	if got := months[len(months)-1].Format("2006-01"); got != "2021-01" {
		t.Errorf("last month = %s, want 2021-01", got)
	}
}
