import json
import happybase
from pymongo import MongoClient

from config import config
from logger import get_logger

logger = get_logger("integrated_analytics")

class IntegratedAnalytics:
    """
    Demonstrates cross-database integration by combining completed 
    transaction data from MongoDB with chronological session clickstream 
    data from HBase to reconstruct a user's path to purchase.
    """
    def __init__(self):
        # Initialize MongoDB Connection (Source of Truth for Transactions)
        logger.info("Connecting to MongoDB...")
        self.mongo_client = MongoClient(config.MONGO_URI)
        self.db = self.mongo_client[config.MONGO_DB_NAME]
        
        # Initialize HBase Connection (Source of Truth for Clickstream)
        logger.info(f"Connecting to HBase Thrift on {config.HBASE_HOST}:{config.HBASE_PORT}...")
        self.hbase_conn = happybase.Connection(host=config.HBASE_HOST, port=config.HBASE_PORT)
        self.sessions_table = self.hbase_conn.table('user_sessions')
        
    def get_sample_transaction(self):
        """Helper to fetch a valid, existing transaction ID from MongoDB."""
        # Find one random transaction that has a 'completed' status
        sample_txn = self.db.transactions.find_one({"status": "completed"})
        if sample_txn:
            return sample_txn["user_id"], sample_txn["_id"]
        return None, None
    
    def get_user_journey_to_purchase(self, user_id, transaction_id):
        """
        Track a specific user's journey from their first page view 
        to their final completed purchase.
        """
        logger.info(f"Fetching transaction {transaction_id} for user {user_id} from MongoDB...")
        
        # 1. Get the completed transaction from MongoDB
        transaction = self.db.transactions.find_one({"_id": transaction_id})
        
        if not transaction:
            logger.error(f"Transaction {transaction_id} not found in MongoDB.")
            return None
            
        logger.info(f"Fetching session {transaction['session_id']} from HBase...")
        
        # 2. Setup the HBase prefix scan to find the exact session
        session_prefix = f"{user_id}|".encode()
        
        # 3. Scan HBase for the user's sessions
        for key, data in self.sessions_table.scan(row_prefix=session_prefix):
            
            # Check if this HBase row matches the MongoDB session_id
            if data.get(b'session:session_id', b'').decode() == transaction['session_id']:
                
                page_views = []
                # 4. Extract only the sparse page view columns
                for col, value in data.items():
                    if col.startswith(b'activity:pv_'):
                        pv_data = json.loads(value.decode())
                        
                        # Save the column qualifier to use as a chronological sorting key
                        pv_data['col_name'] = col.decode()
                        page_views.append(pv_data)
                
                # 5. Sort chronologically (Descending because HBase uses reversed timestamps)
                chronological_journey = sorted(page_views, key=lambda x: x['col_name'], reverse=True)
                
                logger.info(f"Successfully reconstructed journey with {len(chronological_journey)} page views.")
                
                return {
                    "user_id": user_id,
                    "transaction_total": transaction['financials']['total'],
                    "items_purchased": len(transaction['items']),
                    "journey": chronological_journey
                }
                
        logger.warning(f"No matching HBase session found for MongoDB transaction {transaction_id}.")
        return None

    def close_connections(self):
        """Cleanly close database connections."""
        self.mongo_client.close()
        self.hbase_conn.close()

if __name__ == "__main__":
    analytics = IntegratedAnalytics()
    
    # Dynamically grab a valid user_id and transaction_id that actually exists in your DB
    user_id, txn_id = analytics.get_sample_transaction()
    
    if not user_id:
        logger.error("Could not find any completed transactions in MongoDB. Is the data loaded?")
    else:
        # Run the integrated query
        journey_data = analytics.get_user_journey_to_purchase(user_id, txn_id)
        
        if journey_data:
            print(f"\n" + "="*50)
            print(f" USER JOURNEY FOR: {journey_data['user_id']} ")
            print(f" TRANSACTION ID:   {txn_id}")
            print(f"="*50)
            print(f"Total Spent: ${journey_data['transaction_total']}")
            print(f"Items Bought: {journey_data['items_purchased']}")
            print("\nChronological Page Views Leading to Purchase:")
            
            for step, view in enumerate(journey_data['journey'], 1):
                # Extract page_type from the column name (e.g., activity:pv_12345_home_000)
                col_name = view.get('col_name', '')
                try:
                    # Split by '_' and grab the 3rd element which is the page_type
                    page_type = col_name.split('_')[2].upper()
                except IndexError:
                    page_type = 'UNKNOWN'
                
                duration = view.get('duration', 0)
                product_id = view.get('product_id', 'N/A')
                
                print(f"  Step {step}: {page_type:<10} | Duration: {duration}s | Product ID: {product_id}")
            print("="*50 + "\n")
            
    analytics.close_connections()