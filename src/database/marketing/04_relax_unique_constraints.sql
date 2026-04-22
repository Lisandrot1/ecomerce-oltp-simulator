ALTER TABLE LEADS DROP CONSTRAINT IF EXISTS leads_email_key;

-- Drop NOT NULL constraint on event_date to allow null simulation
ALTER TABLE EMAIL_CAMPAIGN_EVENTS ALTER COLUMN event_date DROP NOT NULL;
