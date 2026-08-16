-- Make passed_filter nullable: NULL = not yet filtered (pending), true/false =
-- filtered. The filter task (ticket 11) owns the transition; the announcements
-- upsert no longer defaults new rows to false, which was indistinguishable
-- from "filtered and rejected".
ALTER TABLE disclosures ALTER COLUMN passed_filter DROP NOT NULL;
ALTER TABLE disclosures ALTER COLUMN passed_filter DROP DEFAULT;
