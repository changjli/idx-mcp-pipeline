-- Restore NOT NULL DEFAULT false: pending rows collapse to rejected.
ALTER TABLE disclosures ALTER COLUMN passed_filter SET DEFAULT false;
UPDATE disclosures SET passed_filter = false WHERE passed_filter IS NULL;
ALTER TABLE disclosures ALTER COLUMN passed_filter SET NOT NULL;
