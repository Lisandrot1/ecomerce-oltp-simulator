CREATE INDEX idx_leads_campaign ON LEADS(campaign_id);
CREATE INDEX idx_leads_email ON LEADS(email);

CREATE INDEX idx_assignment_user ON CUSTOMER_SEGMENT_ASSIGNMENT(user_id);
CREATE INDEX idx_assignment_segment ON CUSTOMER_SEGMENT_ASSIGNMENT(segment_id);

CREATE INDEX idx_events_campaign ON EMAIL_CAMPAIGN_EVENTS(campaign_id);
CREATE INDEX idx_events_user ON EMAIL_CAMPAIGN_EVENTS(user_id);
