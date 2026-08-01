import json
import happybase
from pymongo import MongoClient

from config import config
from logger import get_logger

logger = get_logger("integrated_analytics")


def parse_pv_qualifier(col_name):
    """
    Decompose a page-view column qualifier into its parts.

    Written by 1_load_to_hbase.py as:
        activity:pv_{reversed_ts}_{page_type}_{sequence:03d}

    The page_type itself may contain underscores ('category_listing',
    'product_detail'), so it must be rejoined from the middle segments
    rather than read from a fixed index.
    """
    parts = col_name.split('_')
    # parts[0] == 'activity:pv', parts[1] == reversed_ts, parts[-1] == sequence
    if len(parts) < 4:
        return {"page_type": "unknown", "reversed_ts": 0, "sequence": 0}

    try:
        reversed_ts = int(parts[1])
    except ValueError:
        reversed_ts = 0
    try:
        sequence = int(parts[-1])
    except ValueError:
        sequence = 0

    page_type = "_".join(parts[2:-1]) or "unknown"
    return {
        "page_type": page_type,
        "reversed_ts": reversed_ts,
        "sequence": sequence,
    }

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
                cart_items = {}
                # 4a. Cart columns are a separate sparse family member; collect
                #     them so cart/checkout steps can name their contents.
                for col, value in data.items():
                    if col.startswith(b'activity:cart_'):
                        pid = col.decode().split('activity:cart_', 1)[1]
                        try:
                            cart_items[pid] = json.loads(value.decode())
                        except json.JSONDecodeError:
                            cart_items[pid] = {}

                # 4b. Extract only the sparse page view columns
                for col, value in data.items():
                    if col.startswith(b'activity:pv_'):
                        pv_data = json.loads(value.decode())

                        col_name = col.decode()
                        pv_data['col_name'] = col_name
                        parsed = parse_pv_qualifier(col_name)
                        pv_data.update(parsed)
                        page_views.append(pv_data)

                # 5. Sort chronologically. The qualifier embeds a REVERSED
                #    timestamp, so descending numeric order on that value gives
                #    ascending real time. Sorting on the parsed integer rather
                #    than the raw string avoids lexicographic errors when
                #    reversed timestamps differ in digit length. The sequence
                #    index breaks ties within the same second.
                chronological_journey = sorted(
                    page_views,
                    key=lambda x: (-x['reversed_ts'], x['sequence'])
                )
                
                logger.info(f"Successfully reconstructed journey with {len(chronological_journey)} page views.")
                
                return {
                    "user_id": user_id,
                    "transaction_total": transaction['financials']['total'],
                    "items_purchased": len(transaction['items']),
                    "journey": chronological_journey,
                    "cart_items": cart_items
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
            
            cart_items = journey_data.get('cart_items', {})
            cart_summary = ", ".join(
                f"{pid} x{item.get('quantity', '?')}" for pid, item in cart_items.items()
            ) or "empty"

            for step, view in enumerate(journey_data['journey'], 1):
                # page_type was parsed at extraction time; multi-word types such
                # as 'category_listing' and 'product_detail' survive intact.
                page_type = view.get('page_type', 'unknown').upper()
                duration = view.get('duration', 0)

                # Build the most specific context available for this page type.
                # Only product_detail carries a product_id; category_listing
                # carries a category_id; cart/checkout are described by the
                # separate activity:cart_* columns. Anything else genuinely has
                # no entity attached, which is why sparse storage suits it.
                if view.get('product_id'):
                    context = f"product={view['product_id']}"
                elif view.get('category_id'):
                    context = f"category={view['category_id']}"
                elif page_type in ('CART', 'CHECKOUT', 'CONFIRMATION'):
                    context = f"cart=[{cart_summary}]"
                else:
                    context = "-"

                print(f"  Step {step:>2}: {page_type:<18} | {duration:>4}s | {context}")

            print("="*50 + "\n")
            
    analytics.close_connections()