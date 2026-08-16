package extract

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
)

// buildMinimalPDF constructs a single-page PDF with one text-layer line.
// Offsets are computed at build time so the xref table is valid.
func buildMinimalPDF(t *testing.T, text string) []byte {
	t.Helper()

	var b bytes.Buffer
	write := func(s string) { b.WriteString(s) }

	write("%PDF-1.4\n")
	var offsets []int
	obj := func(n int, body string) {
		offsets = append(offsets, b.Len())
		write(fmt.Sprintf("%d 0 obj\n%s\nendobj\n", n, body))
	}

	obj(1, "<< /Type /Catalog /Pages 2 0 R >>")
	obj(2, "<< /Type /Pages /Kids [3 0 R] /Count 1 >>")
	obj(3, "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>")

	stream := fmt.Sprintf("BT /F1 12 Tf 72 720 Td (%s) Tj ET\n", text)
	obj(4, fmt.Sprintf("<< /Length %d >>\nstream\n%sendstream", len(stream), stream))
	obj(5, "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")

	xrefOffset := b.Len()
	write("xref\n0 6\n")
	write("0000000000 65535 f \n")
	for _, off := range offsets {
		write(fmt.Sprintf("%010d 00000 n \n", off))
	}
	write(fmt.Sprintf("trailer\n<< /Size 6 /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n", xrefOffset))
	return b.Bytes()
}

func TestPDFExtractor_ExtractsTextLayer(t *testing.T) {
	data := buildMinimalPDF(t, "Hello Disclosure")

	var e PDFExtractor
	text, err := e.Extract(context.Background(), data)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if !strings.Contains(text, "Hello Disclosure") {
		t.Errorf("expected extracted text to contain %q, got %q", "Hello Disclosure", text)
	}
}

func TestPDFExtractor_RejectsGarbage(t *testing.T) {
	var e PDFExtractor
	if _, err := e.Extract(context.Background(), []byte("not a pdf")); err == nil {
		t.Error("expected error for non-PDF input")
	}
}

func TestPDFExtractor_ContextCancelled(t *testing.T) {
	data := buildMinimalPDF(t, "Hello Disclosure")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var e PDFExtractor
	if _, err := e.Extract(ctx, data); err == nil {
		t.Error("expected error on cancelled context")
	}
}
