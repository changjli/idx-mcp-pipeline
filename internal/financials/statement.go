// Package financials holds the source-neutral data shapes for reported
// financial statements.
//
// It is the seam between the statement adapters (internal/client for the IDX
// ratio snapshot; the legacy internal/ipot live path is retired by the read
// path) and the usecases, repository and (later) the MCP read path: adapters
// import this package, this package imports no adapter, so the dependency
// direction is adapter → financials ← usecase. Nothing here knows which vendor
// produced a row.
package financials

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"
)

// Snapshot statuses recorded in financial_snapshot_runs. They are also the
// values written to the run ledger, so the strings are part of the store
// contract.
const (
	// StatusOK: the payload passed both guards and derived statements.
	StatusOK = "ok"
	// StatusSuspect: the snapshot-level quarter-end share is below the floor
	// (the 2025-12 snapshot stamps filing dates). No statements are derived.
	StatusSuspect = "suspect"
	// StatusEmpty: the month is not published yet (or has no rows). Not an
	// error; the daily check retries.
	StatusEmpty = "empty"
	// StatusFailed: the payload was malformed or the run failed before
	// deriving statements. Not terminal; the daily check retries.
	StatusFailed = "failed"
)

// The provenance stamped on every Tier-1 row. The payload has no currency
// field and no FX rate: IDX converts USD reporters at an undisclosed rate
// (~16,999 IDR/USD in the AADI Q1 2026 check), so the absolute values are
// "IDR after IDX's conversion" and must never claim a raw original currency.
const (
	SourceRatioSnapshot = "idx_ratio_snapshot"
	ValueBasisIDX       = "IDR (IDX-converted at an undisclosed rate)"
)

// Statement is one canonical (ticker, period_end) statement. The money fields
// are raw rupiah (the snapshot's 2-dp billions multiplied by 1e9 and rounded to
// an integer), never float billions. The ratio fields are stored exactly as
// IDX reports them: TTM-derived and NOT recomputable from the YTD columns.
// A nil field means the source did not report it; zero is a reported zero.
type Statement struct {
	Ticker string
	// PeriodEnd is the statement's period end (normally the wire fsDate).
	PeriodEnd time.Time
	// FiscalYearEnd is the fiscal year-end date this period accumulates to
	// (e.g. 2026-12-31 for a Dec-FY issuer's 2026-09-30 period).
	FiscalYearEnd  time.Time
	DurationMonths int

	Assets          *int64
	Liabilities     *int64
	Equity          *int64
	Revenue         *int64
	EBT             *int64
	NetProfit       *int64 // profitPeriod: includes non-controlling interests
	NetProfitParent *int64 // profitAttrOwner: attributable to owners

	EPS       *float64
	BookValue *float64
	ROA       *float64 // percent, TTM
	ROE       *float64 // percent, TTM
	NPM       *float64 // percent, TTM profit over YTD sales (IDX's mixed basis)
	DER       *float64 // liabilities / equity

	AuditStatus *string // U | A | LR | nil
	Opinion     *string // WTM | WDP | TW | TMP | nil

	Source       string
	ValueBasis   string
	SourceFSDate time.Time // raw wire fsDate, for debugging
}

// ContentHash hash covers the fundamental and assurance fields only. It
// deliberately excludes last_seen_month, updated_at, provenance and the price
// ratios (per/pbv): a price move or a monthly sweep of an unchanged statement
// must not look like a revision. The field order is fixed. Nil and zero are
// distinct.
func (s Statement) ContentHash() string {
	h := sha256.New()
	writeStr := func(v string) {
		h.Write([]byte(v))
		h.Write([]byte{0})
	}
	writeDate := func(v time.Time) { writeStr(v.Format("2006-01-02")) }
	writeInt := func(v *int64) {
		if v == nil {
			writeStr("-")
			return
		}
		writeStr(strconv.FormatInt(*v, 10))
	}
	writeFloat := func(v *float64) {
		if v == nil {
			writeStr("-")
			return
		}
		writeStr(strconv.FormatFloat(*v, 'f', 4, 64))
	}
	writeStrPtr := func(v *string) {
		if v == nil {
			writeStr("-")
			return
		}
		writeStr(*v)
	}

	writeStr(s.Ticker)
	writeDate(s.PeriodEnd)
	writeDate(s.FiscalYearEnd)
	writeStr(strconv.Itoa(s.DurationMonths))
	writeInt(s.Assets)
	writeInt(s.Liabilities)
	writeInt(s.Equity)
	writeInt(s.Revenue)
	writeInt(s.EBT)
	writeInt(s.NetProfit)
	writeInt(s.NetProfitParent)
	writeFloat(s.EPS)
	writeFloat(s.BookValue)
	writeFloat(s.ROA)
	writeFloat(s.ROE)
	writeFloat(s.NPM)
	writeFloat(s.DER)
	writeStrPtr(s.AuditStatus)
	writeStrPtr(s.Opinion)

	return hex.EncodeToString(h.Sum(nil))
}

// PeriodLabel renders the response label used by the read path, e.g.
// "9M 2026". It is here (not in the read usecase) so both the store and the
// response agree on what a duration means.
func (s Statement) PeriodLabel() string {
	if s.DurationMonths <= 0 {
		return s.PeriodEnd.Format("2006")
	}
	return fmt.Sprintf("%dM %d", s.DurationMonths, s.PeriodEnd.Year())
}
