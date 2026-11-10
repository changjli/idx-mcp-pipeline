package yahoo

import (
	"context"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/financials"
)

// ClassifyDepth is the source-neutral classification seam the monthly depth
// sweep consumes. It returns the taxonomy and the method that produced it
// (discriminant_probe or page_type_list), so the store can record why.
func (c *Client) ClassifyDepth(ctx context.Context, ticker string) (financials.Taxonomy, string, error) {
	cls, err := c.Classify(ctx, ticker)
	if err != nil {
		return "", "", err
	}
	return cls.Taxonomy, cls.Method, nil
}

// FetchDepth is the source-neutral fetch seam: one small timeseries request for
// the core concepts at 12M, 3M and TTM, using the cached taxonomy instead of
// re-probing. It never reads the ~1 MB statement page.
func (c *Client) FetchDepth(ctx context.Context, ticker string, taxonomy financials.Taxonomy) ([]financials.DepthStatement, error) {
	return c.FetchStatements(ctx, ticker, WithTaxonomy(taxonomy))
}

// compile-time check: the adapter satisfies the depth usecase's source seam.
var (
	_ interface {
		ClassifyDepth(ctx context.Context, ticker string) (financials.Taxonomy, string, error)
		FetchDepth(ctx context.Context, ticker string, taxonomy financials.Taxonomy) ([]financials.DepthStatement, error)
	} = (*Client)(nil)
)
