package ksei

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// balanceposColumns is the count of pipe-separated fields per data row — the
// header layout is fixed (Date, Code, Type, Sec. Num, Price, then Local and
// Foreign blocks of nine investor types plus a Total, and the grand Total).
const balanceposColumns = 25

// equityType is the Type value for listed shares; bonds, warrants and other
// security classes are skipped.
const equityType = "EQUITY"

// balanceposDateLayout is the file's date format ("31-AUG-2026"). Go's month
// token ("Jan") matches abbreviations case-insensitively, so AUG parses.
const balanceposDateLayout = "02-Jan-2006"

// ParseBalancepos parses the pipe-delimited balance-position txt and returns
// the EQUITY rows. The header line is validated for shape (not exact text) so
// cosmetic KSEI renames don't break ingestion; a structurally wrong header
// still fails. Empty numeric fields parse as 0. CRLF line endings are fine —
// the scanner trims them.
func ParseBalancepos(r io.Reader) ([]Holding, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 256*1024), 1024*1024)

	// Header: expect the fixed column count and the Date|Code|Type opening.
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return nil, fmt.Errorf("read header: %w", err)
		}
		return nil, fmt.Errorf("balancepos file is empty")
	}
	header := strings.TrimSpace(scanner.Text())
	fields := strings.Split(header, "|")
	if len(fields) != balanceposColumns || fields[0] != "Date" || fields[1] != "Code" || fields[2] != "Type" {
		return nil, fmt.Errorf("unexpected balancepos header: %q", truncate(header, 120))
	}

	var holdings []Holding
	line := 1
	for scanner.Scan() {
		line++
		raw := strings.TrimSpace(scanner.Text())
		if raw == "" {
			continue
		}
		h, err := parseBalanceposRow(raw)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", line, err)
		}
		if h != nil {
			holdings = append(holdings, *h)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan balancepos rows: %w", err)
	}
	return holdings, nil
}

// parseBalanceposRow converts one pipe-delimited line. Non-EQUITY rows return
// (nil, nil) — skipped, not errors. A row with the wrong field count or an
// unparseable date is an error: a malformed line means the layout changed and
// silently keeping partial data would corrupt the table.
func parseBalanceposRow(line string) (*Holding, error) {
	f := strings.Split(line, "|")
	if len(f) != balanceposColumns {
		return nil, fmt.Errorf("expected %d columns, got %d", balanceposColumns, len(f))
	}
	if strings.TrimSpace(f[2]) != equityType {
		return nil, nil
	}
	date, err := time.Parse(balanceposDateLayout, strings.TrimSpace(f[0]))
	if err != nil {
		return nil, fmt.Errorf("parse date %q: %w", f[0], err)
	}
	code := strings.TrimSpace(f[1])
	if code == "" {
		return nil, fmt.Errorf("empty Code")
	}

	// Column indexes: 3=Sec. Num, 4=Price, 5-13=Local IS..OT, 14=Local Total,
	// 15-23=Foreign IS..OT, 24=Foreign Total. The grand Total column seen in
	// raw files is LocalTotal+ForeignTotal; the file's trailing column IS the
	// Foreign Total block's Total (the 25-column layout ends with it).
	local, err := parseInts(f[5:15])
	if err != nil {
		return nil, fmt.Errorf("local block: %w", err)
	}
	foreign, err := parseInts(f[15:25])
	if err != nil {
		return nil, fmt.Errorf("foreign block: %w", err)
	}
	secNum, err := parseInt(f[3])
	if err != nil {
		return nil, fmt.Errorf("sec num: %w", err)
	}
	price, err := parseInt(f[4])
	if err != nil {
		return nil, fmt.Errorf("price: %w", err)
	}

	return &Holding{
		Date:         date,
		Code:         code,
		Type:         equityType,
		SecNum:       secNum,
		Price:        price,
		LocalIS:      local[0],
		LocalCP:      local[1],
		LocalPF:      local[2],
		LocalIB:      local[3],
		LocalID:      local[4],
		LocalMF:      local[5],
		LocalSC:      local[6],
		LocalFD:      local[7],
		LocalOT:      local[8],
		LocalTotal:   local[9],
		ForeignIS:    foreign[0],
		ForeignCP:    foreign[1],
		ForeignPF:    foreign[2],
		ForeignIB:    foreign[3],
		ForeignID:    foreign[4],
		ForeignMF:    foreign[5],
		ForeignSC:    foreign[6],
		ForeignFD:    foreign[7],
		ForeignOT:    foreign[8],
		ForeignTotal: foreign[9],
	}, nil
}

// parseInts parses a block of 10 numeric columns (nine investor types plus
// Total).
func parseInts(fields []string) ([]int64, error) {
	out := make([]int64, len(fields))
	for i, f := range fields {
		v, err := parseInt(f)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

// parseInt parses a non-negative integer field; empty parses as 0. A value
// with a decimal point (defensive — current files are integers) parses as a
// float and truncates.
func parseInt(field string) (int64, error) {
	s := strings.ReplaceAll(strings.TrimSpace(field), ",", "")
	if s == "" {
		return 0, nil
	}
	if v, err := strconv.ParseInt(s, 10, 64); err == nil {
		return v, nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("parse int %q: %w", field, err)
	}
	return int64(f), nil
}

// truncate shortens a string for error messages.
func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max]
}
