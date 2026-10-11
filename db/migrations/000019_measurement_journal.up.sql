-- Decision Journal: the AI's Verdicts, one row per write (ADR-0016). Append-only;
-- a correction is a new row, never an edit. `session` identifies the agent run
-- that wrote the row, so a same-session double-write is distinguishable from a
-- cross-session Verdict Conflict. `committed` marks the Verdict the Grader
-- scores when a (decision_day, ticker) holds more than one; `supersedes` records
-- which row a deliberate correction replaces (provenance, set alongside
-- committed by the MCP writer). The CHECK constraints make the table itself
-- reject a malformed Verdict, so a bad row never reaches the Grader; the Grader
-- keeps its own validation as a net for out-of-band inserts.
CREATE TABLE measurement_journal (
    id           BIGSERIAL PRIMARY KEY,
    decision_day DATE NOT NULL,
    ticker       TEXT NOT NULL,
    verdict      TEXT NOT NULL,
    entry_low    DOUBLE PRECISION,
    entry_high   DOUBLE PRECISION,
    cl_low       DOUBLE PRECISION,
    cl_high      DOUBLE PRECISION,
    tp_low       DOUBLE PRECISION,
    tp_high      DOUBLE PRECISION,
    rationale    TEXT NOT NULL,
    session      TEXT NOT NULL,
    supersedes   BIGINT REFERENCES measurement_journal (id),
    committed    BOOLEAN NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Tickers are canonical uppercase (Ticker in CONTEXT.md); storing a mixed
    -- case key would silently split one (decision_day, ticker) group in two.
    CONSTRAINT measurement_journal_ticker_upper CHECK (ticker = UPPER(ticker)),
    CONSTRAINT measurement_journal_verdict_known CHECK (verdict IN ('SETUP', 'WATCH', 'PASS')),
    -- A SETUP carries three positive, ordered zones (cl <= entry <= tp, each
    -- low <= high, low == high being a point); a WATCH/PASS carries none.
    CONSTRAINT measurement_journal_zones CHECK (
        (verdict = 'SETUP'
            AND entry_low IS NOT NULL AND entry_high IS NOT NULL
            AND cl_low IS NOT NULL AND cl_high IS NOT NULL
            AND tp_low IS NOT NULL AND tp_high IS NOT NULL
            AND cl_low > 0 AND cl_high > 0
            AND entry_low > 0 AND entry_high > 0
            AND tp_low > 0 AND tp_high > 0
            AND cl_low <= cl_high
            AND entry_low <= entry_high
            AND tp_low <= tp_high
            AND cl_high <= entry_low
            AND entry_high <= tp_low)
        OR
        (verdict <> 'SETUP'
            AND entry_low IS NULL AND entry_high IS NULL
            AND cl_low IS NULL AND cl_high IS NULL
            AND tp_low IS NULL AND tp_high IS NULL)
    )
);

-- The Grader scans the journal by decision-date window.
CREATE INDEX idx_measurement_journal_decision_day ON measurement_journal (decision_day);

-- At most one Committed Verdict per (decision_day, ticker); every other row on
-- the key is retained as audit.
CREATE UNIQUE INDEX idx_measurement_journal_committed
    ON measurement_journal (decision_day, ticker)
    WHERE committed;
