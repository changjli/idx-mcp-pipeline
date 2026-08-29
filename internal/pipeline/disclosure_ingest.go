package pipeline

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// Truncate truncates a string to maxLen chars, appending "..." if truncated.
// Shared log/error-string helper across the ingest usecases.
func Truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// DisclosureStore upserts disclosure rows (pdf_url-UNIQUE idempotent).
// Consumer-side interface (ADR-0006): satisfied by the sqlx-backed
// DisclosureRepository; tests provide the second adapter.
type DisclosureSink interface {
	Upsert(disclosure *entity.Disclosure) error
}

// DisclosureIngest persists flattened announcement rows. Batch error policy:
// fail-fast per the package storage-error policy (see policy.go) — any ticker
// or disclosure upsert failure aborts the batch and returns the error.
type DisclosureIngest struct {
	disclosures DisclosureSink
	tickers     TickerRegistrar
	log         *logrus.Logger
}

// TickerRegistrar ensures the disclosure's ticker exists (FK dependency for
// disclosures). Announcements don't carry issuer names, so a new ticker is
// seeded with name=code and enriched later by the stock summary upsert.
type TickerRegistrar interface {
	Upsert(ticker *entity.Ticker) error
}

// SQLDisclosureSink adapts DisclosureRepository to DisclosureSink.
type SQLDisclosureSink struct {
	repo *repository.DisclosureRepository
	db   *sqlx.DB
}

// NewSQLDisclosureSink binds a disclosure repository to its database.
func NewSQLDisclosureSink(repo *repository.DisclosureRepository, db *sqlx.DB) *SQLDisclosureSink {
	return &SQLDisclosureSink{repo: repo, db: db}
}

// Upsert writes one disclosure row (idempotent via pdf_url).
func (s *SQLDisclosureSink) Upsert(disclosure *entity.Disclosure) error {
	return s.repo.Upsert(s.db, disclosure)
}

// SQLTickerRegistrar adapts TickerRepository to TickerRegistrar.
type SQLTickerRegistrar struct {
	repo *repository.TickerRepository
	db   *sqlx.DB
}

// NewSQLTickerRegistrar binds a ticker repository to its database.
func NewSQLTickerRegistrar(repo *repository.TickerRepository, db *sqlx.DB) *SQLTickerRegistrar {
	return &SQLTickerRegistrar{repo: repo, db: db}
}

// Upsert writes one ticker row.
func (s *SQLTickerRegistrar) Upsert(ticker *entity.Ticker) error {
	return s.repo.Upsert(s.db, ticker)
}

// NewDisclosureIngest wires the disclosure ingest usecase over its stores.
func NewDisclosureIngest(disclosures DisclosureSink, tickers TickerRegistrar, log *logrus.Logger) *DisclosureIngest {
	return &DisclosureIngest{disclosures: disclosures, tickers: tickers, log: log}
}

// UpsertRows persists flattened announcement rows: each row's ticker is
// ensured first, then the disclosure upserted (idempotent via pdf_url).
// Returns rows upserted and the first error — fail-fast per the package
// storage-error policy (see policy.go).
func (n *DisclosureIngest) UpsertRows(rows []*entity.Disclosure) (int, error) {
	upserted := 0
	for _, d := range rows {
		if d.Ticker != nil {
			if err := n.tickers.Upsert(&entity.Ticker{
				Code:   *d.Ticker,
				Name:   *d.Ticker,
				Active: true,
			}); err != nil {
				n.log.Warnf("announcements: ticker upsert failed for %s: %v", *d.Ticker, err)
				return upserted, fmt.Errorf("ticker upsert %s: %w", *d.Ticker, err)
			}
		}
		if err := n.disclosures.Upsert(d); err != nil {
			n.log.Warnf("announcements: disclosure upsert failed for %s: %v", d.PdfURL, err)
			return upserted, fmt.Errorf("disclosure upsert %s: %w", d.PdfURL, err)
		}
		upserted++
	}
	return upserted, nil
}
