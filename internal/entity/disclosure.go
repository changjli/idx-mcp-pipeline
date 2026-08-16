package entity

import (
	"time"

	"github.com/lib/pq"
)

type Disclosure struct {
	ID               int64          `db:"id"`
	Ticker           *string        `db:"ticker"`
	AnnouncementDate time.Time      `db:"announcement_date"`
	Title            string         `db:"title"`
	PdfURL           string         `db:"pdf_url"`
	AttachmentIdx    int32          `db:"attachment_idx"`
	Categories       pq.StringArray `db:"categories"`
	// PassedFilter is nil until the filter task (ticket 11) evaluates the
	// disclosure: NULL = pending, true = passed (sticky), false = rejected.
	PassedFilter     *bool      `db:"passed_filter"`
	ExtractionStatus string     `db:"extraction_status"`
	TextR2Key        *string    `db:"text_r2_key"`
	ExtractionError  *string    `db:"extraction_error"`
	ExtractedAt      *time.Time `db:"extracted_at"`
	FetchedAt        time.Time  `db:"fetched_at"`
}

func (Disclosure) TableName() string { return "disclosures" }
