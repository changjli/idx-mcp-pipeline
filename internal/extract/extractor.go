// Package extract turns binary documents into text and owns the disclosure
// PDF fetch + persist core shared by the extract:disclosure task and the
// fetch_disclosure_pdf tool. The Extractor seam keeps callers independent of
// the concrete engine so an OCR fallback (e.g. gosseract/Tesseract, ticket 16)
// can slot in later without touching them.
package extract

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/ledongthuc/pdf"
)

// Extractor extracts plain text from a binary document.
type Extractor interface {
	Extract(ctx context.Context, data []byte) (string, error)
}

// PDFExtractor extracts the text layer of a PDF via ledongthuc/pdf (pure Go,
// no cgo). Image-scanned PDFs yield empty text — the caller decides how to
// treat that (ticket 11 marks it failed with extraction_error='empty_text').
type PDFExtractor struct{}

// Extract reads every page's text layer and concatenates the pages. The
// context is checked between pages so a slow/huge document is cancelled by
// the task's timeout instead of pinning a worker.
func (PDFExtractor) Extract(ctx context.Context, data []byte) (string, error) {
	r := bytes.NewReader(data)
	pr, err := pdf.NewReader(r, int64(len(data)))
	if err != nil {
		return "", fmt.Errorf("pdf open: %w", err)
	}

	var sb strings.Builder
	for i := 1; i <= pr.NumPage(); i++ {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		text, err := pr.Page(i).GetPlainText(nil)
		if err != nil {
			return "", fmt.Errorf("pdf page %d: %w", i, err)
		}
		sb.WriteString(text)
	}
	return sb.String(), nil
}
