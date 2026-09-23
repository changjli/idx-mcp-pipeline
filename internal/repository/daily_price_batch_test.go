package repository

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

func batchRow(i int) entity.DailyPrice {
	open, high, low, close := 100.0, 110.0, 99.0, 105.0
	vol, val, freq := int64(1_000), int64(100_000), int32(50)
	return entity.DailyPrice{
		Ticker:     fmt.Sprintf("TB%03d", i),
		TradingDay: time.Date(2026, 5, 25, 0, 0, 0, 0, time.UTC),
		Open:       &open, High: &high, Low: &low, Close: &close,
		Volume: &vol, Value: &val, Frequency: &freq, Source: "idx",
	}
}

func batchRows(n int) []entity.DailyPrice {
	rows := make([]entity.DailyPrice, 0, n)
	for i := 0; i < n; i++ {
		rows = append(rows, batchRow(i))
	}
	return rows
}

// TestChunkDailyPrices_Boundaries pins the chunking rule: an exact multiple
// splits evenly, a non-multiple leaves a short remainder chunk, and no rows
// means no statement at all.
func TestChunkDailyPrices_Boundaries(t *testing.T) {
	cases := []struct {
		name string
		rows int
		size int
		want []int // chunk lengths, in order
	}{
		{"empty", 0, 200, nil},
		{"smaller than size", 5, 200, []int{5}},
		{"exact multiple", 6, 3, []int{3, 3}},
		{"remainder", 8, 3, []int{3, 3, 2}},
		{"single row chunks", 3, 1, []int{1, 1, 1}},
		{"950 rows at the default size", 950, DefaultDailyPriceChunkSize, []int{200, 200, 200, 200, 150}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows := batchRows(tc.rows)
			chunks := chunkDailyPrices(rows, tc.size)
			if len(chunks) != len(tc.want) {
				t.Fatalf("chunks = %d, want %d", len(chunks), len(tc.want))
			}
			total := 0
			for i, want := range tc.want {
				if len(chunks[i]) != want {
					t.Errorf("chunk %d len = %d, want %d", i, len(chunks[i]), want)
				}
				total += len(chunks[i])
			}
			if total != tc.rows {
				t.Errorf("chunks cover %d rows, want %d (no row dropped or duplicated)", total, tc.rows)
			}
			// Order preserved: the flattened chunks are the input, in order.
			flat := make([]entity.DailyPrice, 0, total)
			for _, c := range chunks {
				flat = append(flat, c...)
			}
			for i := range flat {
				if flat[i].Ticker != rows[i].Ticker {
					t.Fatalf("row %d = %s, want %s (order not preserved)", i, flat[i].Ticker, rows[i].Ticker)
				}
			}
		})
	}
}

// TestChunkDailyPrices_ZeroSizeFallsBack guards the tunable: an unset chunk
// size must use the default rather than looping forever on a zero stride.
func TestChunkDailyPrices_ZeroSizeFallsBack(t *testing.T) {
	chunks := chunkDailyPrices(batchRows(3), 0)
	if len(chunks) != 1 || len(chunks[0]) != 3 {
		t.Errorf("zero size should fall back to %d, got %d chunks", DefaultDailyPriceChunkSize, len(chunks))
	}
}

// TestDailyPriceRepository_ChunkSizeFallback covers the repository-level
// fallback, including a repository built as a struct literal (no constructor).
func TestDailyPriceRepository_ChunkSizeFallback(t *testing.T) {
	if got := (&DailyPriceRepository{}).chunkSize(); got != DefaultDailyPriceChunkSize {
		t.Errorf("zero-value repo chunkSize = %d, want %d", got, DefaultDailyPriceChunkSize)
	}
	if got := (&DailyPriceRepository{ChunkSize: 7}).chunkSize(); got != 7 {
		t.Errorf("configured chunkSize = %d, want 7", got)
	}
	if got := NewDailyPriceRepository(nil).ChunkSize; got != DefaultDailyPriceChunkSize {
		t.Errorf("constructor chunkSize = %d, want %d", got, DefaultDailyPriceChunkSize)
	}
}

// TestBuildDailyPriceUpsert_PlaceholdersAndArgs verifies the generated
// statement binds every column positionally, in the entity's field order, and
// carries the same conflict-update semantics as the per-row Upsert.
func TestBuildDailyPriceUpsert_PlaceholdersAndArgs(t *testing.T) {
	rows := batchRows(3)
	query, args := buildDailyPriceUpsert(rows)

	if want := 3 * 10; len(args) != want {
		t.Fatalf("args = %d, want %d", len(args), want)
	}
	for _, placeholder := range []string{"($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)", "($21,$22,$23,$24,$25,$26,$27,$28,$29,$30)"} {
		if !strings.Contains(query, placeholder) {
			t.Errorf("query missing placeholder tuple %s", placeholder)
		}
	}
	if strings.Contains(query, "($31") {
		t.Error("query has more placeholders than the 3 bound rows")
	}
	if got := strings.Count(query, "),("); got != 2 { // 3 tuples, 2 separators
		t.Errorf("value tuples = %d, want 3 (separators %d)", got+1, got)
	}
	if got := strings.Count(query, "$"); got != 30 {
		t.Errorf("placeholders = %d, want 30", got)
	}
	for _, want := range []string{
		"INSERT INTO daily_prices (ticker, trading_day, open, high, low, close, volume, value, frequency, source)",
		"ON CONFLICT (ticker, trading_day) DO UPDATE SET",
		"close = EXCLUDED.close",
		"fetched_at = NOW()",
	} {
		if !strings.Contains(query, want) {
			t.Errorf("query missing %q", want)
		}
	}

	// Target-column count must equal the expressions bound per tuple, or
	// Postgres rejects the statement outright (42601: more target columns than
	// expressions) — pinned because the batch builder is assembled by hand.
	colList := query[strings.Index(query, "daily_prices (")+len("daily_prices ("):]
	colList = colList[:strings.Index(colList, ")")]
	if got, want := strings.Count(colList, ",")+1, len(args)/len(rows); got != want {
		t.Errorf("INSERT target columns = %d, expressions per row = %d", got, want)
	}

	// Field order: first three args of row 2 are its ticker, day, open.
	base := 10
	if args[base] != rows[1].Ticker {
		t.Errorf("args[%d] = %v, want ticker %s", base, args[base], rows[1].Ticker)
	}
	if day, ok := args[base+1].(time.Time); !ok || !day.Equal(rows[1].TradingDay) {
		t.Errorf("args[%d] = %v, want trading day %v", base+1, args[base+1], rows[1].TradingDay)
	}
	if src := args[base+9]; src != "idx" {
		t.Errorf("args[%d] = %v, want source idx", base+9, src)
	}
}

// TestBuildDailyPriceUpsert_NullsPassThrough pins that a row with no stored
// value binds a real NULL rather than a zero — a zero close/volume would read
// as a traded-at-zero day in every downstream read.
func TestBuildDailyPriceUpsert_NullsPassThrough(t *testing.T) {
	row := entity.DailyPrice{
		Ticker:     "TBNIL",
		TradingDay: time.Date(2026, 5, 25, 0, 0, 0, 0, time.UTC),
		Source:     "idx",
	}
	_, args := buildDailyPriceUpsert([]entity.DailyPrice{row})
	for i, name := range []string{"open", "high", "low", "close", "volume", "value", "frequency"} {
		ptr := args[2+i] // after ticker, trading_day
		if ptr == nil {
			t.Errorf("arg for %s is a nil interface, want a typed nil pointer", name)
			continue
		}
		switch v := ptr.(type) {
		case *float64:
			if v != nil {
				t.Errorf("%s = %v, want nil pointer", name, *v)
			}
		case *int64:
			if v != nil {
				t.Errorf("%s = %v, want nil pointer", name, *v)
			}
		case *int32:
			if v != nil {
				t.Errorf("%s = %v, want nil pointer", name, *v)
			}
		default:
			t.Errorf("arg for %s has unexpected type %T", name, ptr)
		}
	}
}
