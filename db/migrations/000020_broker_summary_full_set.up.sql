-- Broker Summary v2 (issues 05/06/08): the full broker set replaces the top-10 tail.
--
-- Others Net was Σ sell₁₀ − Σ buy₁₀, and Net was buy − sell + others_net, so every
-- aggregate net the flow tools reported was identically zero. With every broker
-- stored, Σbuy == Σsell for a ticker-day and there is no tail to estimate: the
-- column has no meaning, so it is dropped (its historical values go with it).
ALTER TABLE broker_stock_summary_totals DROP COLUMN others_net;

-- Provenance: which writer produced a row. Legacy rows are the scraped top-10
-- set; new writes are stamped live (on-demand), sweep (daily anchor day) or
-- backfill (range/backfill task). Ticket 08 uses the distinction to tell a
-- backfilled row from a sweep-era one.
ALTER TABLE broker_stock_summaries ADD COLUMN source TEXT NOT NULL DEFAULT 'legacy_ipot'
    CHECK (source IN ('legacy_ipot', 'live', 'sweep', 'backfill'));
ALTER TABLE broker_stock_summary_totals ADD COLUMN source TEXT NOT NULL DEFAULT 'legacy_ipot'
    CHECK (source IN ('legacy_ipot', 'live', 'sweep', 'backfill'));

-- The scraped footer foreign net was proven unreliable (it understates both t_val
-- and f_nval by the same ~0.71% factor). NULL is the honest unobserved state —
-- never zero, never silently trusted — until the foreign-flow pass refills it.
UPDATE broker_stock_summary_totals SET f_nval = NULL;
