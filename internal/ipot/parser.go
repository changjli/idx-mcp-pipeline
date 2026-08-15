package ipot

import (
	"fmt"
	"strings"

	"golang.org/x/net/html"
)

// Row is one broker's buy or sell activity for a ticker+day.
type Row struct {
	BrokerCode string
	Lot        int64
	Value      int64
	AvgPrice   int64
	Rank       int
}

// Totals is the footer summary line of the IPOT broker summary table.
type Totals struct {
	TVal  int64 // total value (rupiah)
	FNVal int64 // foreign net value (rupiah)
	TLot  int64 // total lots
	Avg   int64 // average price
}

// Result is the parsed broker summary for one ticker+day.
type Result struct {
	Buyers  []Row
	Sellers []Row
	Totals  Totals
}

// ParseBrokerSummary parses the IPOT data-brokersummary.php HTML response.
// The table pairs top-10 buyers with top-10 sellers per row (9 columns:
// Buyer, B.Lot, B.Val, B.Avg, #, Seller, S.Lot, S.Val, S.Avg) and carries a
// footer summary line. A non-trading day returns an empty table (rows with
// empty cells) — that parses to an empty Result with zero totals, not an error.
func ParseBrokerSummary(data []byte) (*Result, error) {
	doc, err := html.Parse(strings.NewReader(string(data)))
	if err != nil {
		return nil, fmt.Errorf("parse html: %w", err)
	}

	res := &Result{}
	found := false

	var walk func(*html.Node) error
	walk = func(n *html.Node) error {
		if n.Type == html.ElementNode && n.Data == "table" && isSummaryTable(n) {
			if err := parseTable(n, res); err != nil {
				return err
			}
			found = true
			return nil
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			if err := walk(c); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(doc); err != nil {
		return nil, err
	}

	if !found {
		return nil, fmt.Errorf("no broker summary table found")
	}
	return res, nil
}

// isSummaryTable reports whether a table is the IPOT broker summary table.
// The page may carry other tables (nav, related stocks) above it, so the
// summary table is identified by its distinguishing class rather than position.
func isSummaryTable(n *html.Node) bool {
	for _, attr := range n.Attr {
		if attr.Key != "class" {
			continue
		}
		for _, cls := range strings.Fields(attr.Val) {
			if cls == "table-summary" {
				return true
			}
		}
	}
	return false
}

// parseTable extracts tbody rows and the tfoot totals from a summary table.
func parseTable(n *html.Node, res *Result) error {
	var tbody, tfoot *html.Node
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		if c.Type != html.ElementNode {
			continue
		}
		switch c.Data {
		case "tbody":
			tbody = c
		case "tfoot":
			tfoot = c
		}
	}
	if tbody == nil {
		return fmt.Errorf("broker summary table has no tbody")
	}

	rank := 0
	for tr := tbody.FirstChild; tr != nil; tr = tr.NextSibling {
		if tr.Type != html.ElementNode || tr.Data != "tr" {
			continue
		}
		cells := rowCells(tr)
		if len(cells) < 9 {
			continue
		}

		buyerCode := cells[0]
		sellerCode := cells[5]
		if buyerCode == "" && sellerCode == "" {
			continue // empty row — non-trading day or not yet published
		}
		rank++

		if buyerCode != "" {
			row, err := parseSide(cells, 0, rank)
			if err != nil {
				return fmt.Errorf("buyer row %d: %w", rank, err)
			}
			res.Buyers = append(res.Buyers, row)
		}
		if sellerCode != "" {
			row, err := parseSide(cells, 5, rank)
			if err != nil {
				return fmt.Errorf("seller row %d: %w", rank, err)
			}
			res.Sellers = append(res.Sellers, row)
		}
	}

	if tfoot != nil {
		totals, err := parseTotals(tfoot)
		if err != nil {
			return err
		}
		res.Totals = totals
	}
	return nil
}

// parseSide builds a Row from cells at the given offset.
// offset 0 = buyer columns (code, lot, value, avg); offset 5 = seller columns.
func parseSide(cells []string, offset, rank int) (Row, error) {
	lot, err := parseValue(cells[offset+1])
	if err != nil {
		return Row{}, fmt.Errorf("lot %q: %w", cells[offset+1], err)
	}
	val, err := parseValue(cells[offset+2])
	if err != nil {
		return Row{}, fmt.Errorf("value %q: %w", cells[offset+2], err)
	}
	avg, err := parseValue(cells[offset+3])
	if err != nil {
		return Row{}, fmt.Errorf("avg %q: %w", cells[offset+3], err)
	}
	return Row{
		BrokerCode: cells[offset],
		Lot:        lot,
		Value:      val,
		AvgPrice:   avg,
		Rank:       rank,
	}, nil
}

// parseTotals extracts the footer summary line from the tfoot.
// Footer spans look like "T. Val : 71.2 B", "F. NVal : 11.1 B",
// "T.Lot : 808,975 ", "Avg : 880 ".
func parseTotals(tfoot *html.Node) (Totals, error) {
	text := collectText(tfoot)
	var t Totals
	var err error
	if t.TVal, err = footerValue(text, "T. Val"); err != nil {
		return t, err
	}
	if t.FNVal, err = footerValue(text, "F. NVal"); err != nil {
		return t, err
	}
	if t.TLot, err = footerValue(text, "T.Lot"); err != nil {
		return t, err
	}
	if t.Avg, err = footerValue(text, "Avg"); err != nil {
		return t, err
	}
	return t, nil
}

// footerValue extracts the numeric value following a footer label.
// The value runs from the label's colon to the next known label or end of text.
func footerValue(text, label string) (int64, error) {
	idx := strings.Index(text, label)
	if idx < 0 {
		return 0, fmt.Errorf("footer label %q not found", label)
	}
	rest := text[idx+len(label):]
	rest = strings.TrimSpace(rest)
	rest = strings.TrimPrefix(rest, ":")
	rest = strings.TrimSpace(rest)

	end := len(rest)
	for _, next := range []string{"T. Val", "F. NVal", "T.Lot", "Avg"} {
		if j := strings.Index(rest, next); j >= 0 && j < end {
			end = j
		}
	}
	return parseValue(strings.TrimSpace(rest[:end]))
}

// rowCells returns the trimmed text of each td cell in a table row.
func rowCells(tr *html.Node) []string {
	var cells []string
	for c := tr.FirstChild; c != nil; c = c.NextSibling {
		if c.Type == html.ElementNode && c.Data == "td" {
			cells = append(cells, strings.TrimSpace(collectText(c)))
		}
	}
	return cells
}

// collectText gathers all descendant text of a node.
func collectText(n *html.Node) string {
	var sb strings.Builder
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.TextNode {
			sb.WriteString(n.Data)
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(n)
	return sb.String()
}
