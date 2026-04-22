from utils.db import get_connection
from sqlalchemy import text
import generators.marketing as mkt

def test_marketing():
    try:
        with get_connection('marketing').connect() as conn:
            # Check if we can insert campaigns
            print("Testing campaign insertion...")
            camp_ids = mkt.insert_campaigns(conn, employee_ids=[1, 2, 3])
            print(f"Campaign IDs: {camp_ids}")
            
            # Check if we can insert leads
            print("Testing lead insertion...")
            lead_ids = mkt.insert_leads(conn, camp_ids, user_ids=[1, 2, 3], volume=10)
            print(f"Lead IDs: {lead_ids}")
            
            # Check if we can insert events
            print("Testing event insertion...")
            mkt.insert_campaign_events(conn, camp_ids, user_ids=[1, 2, 3], volume=10)
            print("Event insertion completed.")
            
            # Verify counts
            res = conn.execute(text("SELECT count(*) FROM LEADS"))
            print(f"Leads count: {res.fetchone()[0]}")
            
            res = conn.execute(text("SELECT count(*) FROM EMAIL_CAMPAIGN_EVENTS"))
            print(f"Events count: {res.fetchone()[0]}")
            
    except Exception as e:
        print(f"Test failed: {e}")

if __name__ == "__main__":
    test_marketing()
