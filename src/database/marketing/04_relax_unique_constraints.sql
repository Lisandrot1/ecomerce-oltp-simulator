-- Drop unique constraint on email to allow duplicate simulation
ALTER TABLE LEADS DROP CONSTRAINT IF EXISTS leads_email_key;
