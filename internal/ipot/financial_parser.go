package ipot

import (
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/net/html"
)

// FinancialStatement is one period column of the IPOT fundamental table,
// normalized. Money fields are raw IDR (abbreviated source values are expanded:
// "54.6 T" → 5.46e13). Ratio fields keep the source's own unit (ROE/ROA in
// percent, PER/PBV/DebtToEquity as plain multiples). A nil field means the
// source did not report the line for that period (e.g. telcos report
// GrossProfit as 0 — that is parsed as 0, not nil).
type FinancialStatement struct {
	Label          string `json:"period_label"`          // e.g. "3M 2026", "[6M] 2026"
	PeriodEnd      string `json:"period_end"`            // YYYY-MM-DD
	DurationMonths int    `json:"duration_months"`       // 3, 6, 9 or 12
	IsForecast     bool   `json:"is_forecast,omitempty"` // the "Anlz YYYY" consensus column
	IsInterim      bool   `json:"is_interim,omitempty"`  // bracketed label, e.g. "[6M] 2026"

	Revenue           *float64 `json:"revenue,omitempty"`
	GrossProfit       *float64 `json:"gross_profit,omitempty"`
	OperatingProfit   *float64 `json:"operating_profit,omitempty"`
	NetProfit         *float64 `json:"net_profit,omitempty"`
	EBITDA            *float64 `json:"ebitda,omitempty"`
	InterestExpense   *float64 `json:"interest_expense,omitempty"`
	Cash              *float64 `json:"cash,omitempty"`
	TotalAssets       *float64 `json:"total_assets,omitempty"`
	ShortTermDebt     *float64 `json:"short_term_borrowing,omitempty"`
	LongTermDebt      *float64 `json:"long_term_borrowing,omitempty"`
	TotalDebt         *float64 `json:"total_debt,omitempty"` // short + long term
	TotalEquity       *float64 `json:"total_equity,omitempty"`
	GrossProfitMargin *float64 `json:"gross_profit_margin,omitempty"` // gross_profit / revenue, percent
	EPS               *float64 `json:"eps,omitempty"`                 // IDR per share
	ROE               *float64 `json:"roe,omitempty"`                 // percent
	ROA               *float64 `json:"roa,omitempty"`                 // percent
	PER               *float64 `json:"per,omitempty"`
	PBV               *float64 `json:"pbv,omitempty"`
	DebtToEquity      *float64 `json:"debt_to_equity,omitempty"`

	// Extra holds line items without a dedicated field (e.g. "Deviden",
	// "BVPS", "EV/EBITDA", "Share Out", "Market Cap"), keyed by the source
	// label with the source's own unit. The catch-all guarantees the parser
	// never silently drops a row it doesn't recognize.
	Extra map[string]float64 `json:"extra,omitempty"`
}

// addToExtra records a value under label, initializing the map lazily.
func (s *FinancialStatement) addToExtra(label string, v float64) {
	if s.Extra == nil {
		s.Extra = make(map[string]float64)
	}
	s.Extra[label] = v
}

// Financials is the parsed fundamental table for one ticker.
type Financials struct {
	Ticker    string
	Currency  string               // always "IDR"
	Periods   []FinancialStatement // column order, newest first
	LastPrice *float64             // IDR per share, latest trade

	// done stops the row walk once the legacy stale copy of the table begins
	// (a second period-header row lower on the page).
	done bool
}

// fundamentalLabels maps source line-item labels to a statement field setter.
var fundamentalLabels = map[string]func(*FinancialStatement, float64){
	"Revenue":          func(s *FinancialStatement, v float64) { s.Revenue = &v },
	"Gross Profit":     func(s *FinancialStatement, v float64) { s.GrossProfit = &v },
	"Operating Profit": func(s *FinancialStatement, v float64) { s.OperatingProfit = &v },
	"Net.Profit":       func(s *FinancialStatement, v float64) { s.NetProfit = &v },
	"EBITDA":           func(s *FinancialStatement, v float64) { s.EBITDA = &v },
	"Interest Expense": func(s *FinancialStatement, v float64) { s.InterestExpense = &v },
	"Cash":             func(s *FinancialStatement, v float64) { s.Cash = &v },
	"Total Asset":      func(s *FinancialStatement, v float64) { s.TotalAssets = &v },
	"S.T.Borrowing":    func(s *FinancialStatement, v float64) { s.ShortTermDebt = &v },
	"L.T.Borrowing":    func(s *FinancialStatement, v float64) { s.LongTermDebt = &v },
	"Total Equity":     func(s *FinancialStatement, v float64) { s.TotalEquity = &v },
	"EPS":              func(s *FinancialStatement, v float64) { s.EPS = &v },
	"ROE":              func(s *FinancialStatement, v float64) { s.ROE = &v },
	"ROA":              func(s *FinancialStatement, v float64) { s.ROA = &v },
	"PER":              func(s *FinancialStatement, v float64) { s.PER = &v },
	"PBV":              func(s *FinancialStatement, v float64) { s.PBV = &v },
	"Debt/Equity":      func(s *FinancialStatement, v float64) { s.DebtToEquity = &v },
}

// ParseFundamental parses the IPOT fundamental.php HTML response into
// normalized statements. Only the first table on the page is read — the page
// renders a second, stale legacy copy (older periods, possibly a different
// instrument) below the live one. The "Anlz YYYY" analyst-forecast column is
// parsed and tagged IsForecast; callers filter it out when they only want
// reported periods.
func ParseFundamental(data []byte) (*Financials, error) {
	doc, err := html.Parse(strings.NewReader(string(data)))
	if err != nil {
		return nil, fmt.Errorf("ipot: parse fundamental html: %w", err)
	}

	var fin Financials
	fin.Currency = "IDR"

	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if fin.done {
			return
		}
		if n.Type == html.ElementNode && n.Data == "tr" {
			fin.consumeRow(rowCellsAll(n))
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(doc)

	if fin.Periods == nil {
		return nil, fmt.Errorf("ipot: fundamental table not found (no period header row)")
	}
	finalizeFinancials(&fin)
	return &fin, nil
}

// consumeRow ingests one table row: either a period-header row (<th> cells),
// a section header ("BALANCE SHEET", ...), or a label row. Once periods are
// known, label rows are matched against fundamentalLabels.
func (fin *Financials) consumeRow(cells []string) {
	// Non-empty cell list.
	vals := make([]string, 0, len(cells))
	for _, c := range cells {
		c = strings.TrimSpace(c)
		if c != "" {
			vals = append(vals, c)
		}
	}
	if len(vals) == 0 {
		return
	}

	// Period header row: leading cells (a control <th> holding the Go button
	// and its select options) then all period tokens ("Anlz YYYY", "3M 2026",
	// "[6M] 2026", ...). A second header row means the legacy stale copy of
	// the table started — stop parsing so it can't overwrite live values.
	if hdr := trimLeadingNonPeriod(vals); len(hdr) >= 2 && allPeriodCells(hdr) {
		if fin.Periods != nil {
			fin.done = true
			return
		}
		fin.Periods = make([]FinancialStatement, 0, len(hdr))
		for _, v := range hdr {
			fin.Periods = append(fin.Periods, newPeriod(v))
		}
		return
	}

	// Section header row (single cell) — resets nothing, just context. Meta
	// rows before the first section header are ignored by the label map.
	if len(vals) == 1 {
		return
	}

	if fin.Periods == nil {
		return
	}
	label := vals[0]
	if label == "Last Price" {
		// Meta row: values align with the period columns; take the first.
		if v, ok := parseFinancialValue(vals[1]); ok && len(fin.Periods) > 0 {
			fin.LastPrice = &v
		}
	}
	setter, ok := fundamentalLabels[label]
	if !ok {
		// Unknown line item: park it in Extra so nothing is silently dropped.
		for i, raw := range vals[1:] {
			if i >= len(fin.Periods) {
				break
			}
			v, ok := parseFinancialValue(raw)
			if !ok {
				continue
			}
			fin.Periods[i].addToExtra(label, v)
		}
		return
	}
	// Values align with period columns by index.
	for i, raw := range vals[1:] {
		if i >= len(fin.Periods) {
			break
		}
		v, ok := parseFinancialValue(raw)
		if !ok {
			continue
		}
		setter(&fin.Periods[i], v)
	}
}

// trimLeadingNonPeriod drops leading cells that aren't period tokens — the
// period header row starts with a control cell (the Go button + its select
// options) before the "Anlz YYYY" / "3M YYYY" columns.
func trimLeadingNonPeriod(cells []string) []string {
	for i, c := range cells {
		if isPeriodCell(c) {
			return cells[i:]
		}
	}
	return nil
}

// allPeriodCells reports whether every cell looks like a period column header
// ("3M 2026", "[6M] 2026", "12M 2025", "Anlz 2026").
func allPeriodCells(cells []string) bool {
	if len(cells) < 2 {
		return false
	}
	for _, c := range cells {
		if !isPeriodCell(c) {
			return false
		}
	}
	return true
}

// isPeriodCell matches a single period column header.
func isPeriodCell(c string) bool {
	parts := strings.Fields(c)
	if len(parts) != 2 {
		return false
	}
	if parts[0] == "Anlz" {
		return len(parts[1]) == 4
	}
	dur := strings.Trim(parts[0], "[]")
	switch dur {
	case "3M", "6M", "9M", "12M":
		return len(parts[1]) == 4
	}
	return false
}

// newPeriod builds a FinancialStatement from a period header cell.
func newPeriod(cell string) FinancialStatement {
	parts := strings.Fields(cell)
	fs := FinancialStatement{Label: cell}
	if parts[0] == "Anlz" {
		fs.IsForecast = true
		fs.DurationMonths = 12
	} else {
		dur := strings.Trim(parts[0], "[]")
		fs.IsInterim = strings.HasPrefix(parts[0], "[")
		switch dur {
		case "3M":
			fs.DurationMonths = 3
		case "6M":
			fs.DurationMonths = 6
		case "9M":
			fs.DurationMonths = 9
		case "12M":
			fs.DurationMonths = 12
		}
	}
	fs.PeriodEnd = periodEnd(parts[1], fs.DurationMonths)
	return fs
}

// periodEnd maps "YYYY" + duration-in-months to the period's end date
// (3M→Mar 31, 6M→Jun 30, 9M→Sep 30, 12M→Dec 31).
func periodEnd(year string, months int) string {
	day := map[int]string{3: "03-31", 6: "06-30", 9: "09-30", 12: "12-31"}[months]
	if day == "" {
		return year
	}
	return year + "-" + day
}

// rowCellsAll returns the trimmed text of each td/th cell in a table row.
// Unlike rowCells it includes th cells — the fundamental table's period
// header row is made of th.
func rowCellsAll(tr *html.Node) []string {
	var cells []string
	for c := tr.FirstChild; c != nil; c = c.NextSibling {
		if c.Type == html.ElementNode && (c.Data == "td" || c.Data == "th") {
			cells = append(cells, strings.TrimSpace(collectText(c)))
		}
	}
	return cells
}

// finalizeFinancials computes derived fields (total debt, GPM) per period.
func finalizeFinancials(fin *Financials) {
	for i := range fin.Periods {
		p := &fin.Periods[i]
		if p.ShortTermDebt != nil && p.LongTermDebt != nil {
			t := *p.ShortTermDebt + *p.LongTermDebt
			p.TotalDebt = &t
		}
		if p.GrossProfit != nil && p.Revenue != nil && *p.Revenue != 0 {
			g := *p.GrossProfit / *p.Revenue * 100
			p.GrossProfitMargin = &g
		}
	}
}

// parseFinancialValue parses one table cell: an abbreviated or plain number.
// Handles "54.6 T", "27,256B", "2,610", "12.17 x", "7.01 %", "(123)", "-".
func parseFinancialValue(s string) (float64, bool) {
	s = strings.TrimSpace(s)
	if s == "" || s == "-" {
		return 0, false
	}
	neg := false
	if strings.HasPrefix(s, "(") && strings.HasSuffix(s, ")") {
		neg = true
		s = s[1 : len(s)-1]
	}
	s = strings.TrimSpace(s)

	// Unit suffix (K/M/B/T) may trail with or without a space.
	mult := 1.0
	if s != "" {
		last := s[len(s)-1]
		switch last {
		case 'T', 't':
			mult, s = 1e12, s[:len(s)-1]
		case 'B', 'b':
			mult, s = 1e9, s[:len(s)-1]
		case 'M', 'm':
			mult, s = 1e6, s[:len(s)-1]
		case 'K', 'k':
			mult, s = 1e3, s[:len(s)-1]
		}
	}
	// Strip ratio decorations ("x", "%") and thousands separators.
	s = strings.TrimSpace(strings.Map(func(r rune) rune {
		switch r {
		case 'x', 'X', '%', ',':
			return -1
		}
		return r
	}, s))
	if s == "" {
		return 0, false
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	v *= mult
	if neg {
		v = -v
	}
	return v, true
}
