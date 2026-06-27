# enhanced_load_to_hbase.py - FIXED VERSION
import json
import os
import happybase
from datetime import datetime
from config import config
from logger import get_logger

logger = get_logger("load_to_hbase")

def encode_timestamp_reversed(timestamp_str):
    """Convert timestamp to reverse-encoded format for recent-first queries"""
    dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
    timestamp_ms = int(dt.timestamp() * 1000)
    # Reverse: Long.MAX_VALUE - timestamp_ms
    reversed_ts = 9223372036854775807 - timestamp_ms
    return reversed_ts

def load_hbase_enhanced():
    logger.info(f"Connecting to HBase Thrift on {config.HBASE_HOST}:{config.HBASE_PORT}")

    try:
        connection = happybase.Connection(
            host=config.HBASE_HOST,
            port=config.HBASE_PORT,
            timeout=60000,
            autoconnect=True
        )
        
        # Test connection by listing tables
        tables = connection.tables()
        logger.info(f"Successfully connected! Existing tables: {list(tables)}")
        
        # Table 1: user_sessions (time-series browsing data)
        user_sessions_table = "user_sessions"
        families = {
            'session': dict(),
            'activity': dict(),
            'geo': dict(),
            'device': dict(),
        }

        # Drop and recreate table if exists
        if user_sessions_table.encode() in connection.tables():
            logger.info(f"Dropping existing table: {user_sessions_table}")
            connection.disable_table(user_sessions_table)
            connection.delete_table(user_sessions_table)

        logger.info(f"Creating table: {user_sessions_table}")
        connection.create_table(user_sessions_table, families)
        table = connection.table(user_sessions_table)

        # Table 2: product_metrics (time-series product performance)
        product_metrics_table = "product_metrics"
        metrics_families = {
            'daily': dict(),
            'aggregates': dict(),
        }

        if product_metrics_table.encode() in connection.tables():
            logger.info(f"Dropping existing table: {product_metrics_table}")
            connection.disable_table(product_metrics_table)
            connection.delete_table(product_metrics_table)

        logger.info(f"Creating table: {product_metrics_table}")
        connection.create_table(product_metrics_table, metrics_families)
        product_table = connection.table(product_metrics_table)

        data_dir = "data/"

        # Track metrics
        total_sessions_loaded = 0
        product_metrics = {}

        # Load session files (0 to 19 as per your dataset)
        for i in range(20):
            filename = f"sessions_{i}.json"
            filepath = os.path.join(data_dir, filename)

            if not os.path.exists(filepath):
                logger.warning(f"File not found: {filename}. Skipping.")
                continue

            logger.info(f"Processing partition: {filename}")

            try:
                with open(filepath, "r") as f:
                    sessions = json.load(f)

                # Use batch for better performance
                with table.batch(batch_size=1000) as batch:
                    for session in sessions:
                        # Build row key: user_id|reverse_timestamp
                        reversed_ts = encode_timestamp_reversed(session["start_time"])
                        row_key = f"{session['user_id']}|{reversed_ts}".encode()

                        # Session metadata
                        batch.put(
                            row_key,
                            {
                                b"session:session_id": session["session_id"].encode(),
                                b"session:duration_seconds": str(
                                    session["duration_seconds"]
                                ).encode(),
                                b"session:conversion_status": session[
                                    "conversion_status"
                                ].encode(),
                                b"session:referrer": session["referrer"].encode(),
                                b"session:start_time": session["start_time"].encode(),
                                b"session:end_time": session["end_time"].encode(),
                            },
                        )

                        # Geo data
                        if "geo_data" in session:
                            batch.put(
                                row_key,
                                {
                                    b"geo:city": session["geo_data"]
                                    .get("city", "")
                                    .encode(),
                                    b"geo:state": session["geo_data"]
                                    .get("state", "")
                                    .encode(),
                                    b"geo:country": session["geo_data"]
                                    .get("country", "")
                                    .encode(),
                                    b"geo:ip_address": session["geo_data"]
                                    .get("ip_address", "")
                                    .encode(),
                                },
                            )

                        # Device profile
                        if "device_profile" in session:
                            batch.put(
                                row_key,
                                {
                                    b"device:type": session["device_profile"]
                                    .get("type", "")
                                    .encode(),
                                    b"device:os": session["device_profile"]
                                    .get("os", "")
                                    .encode(),
                                    b"device:browser": session["device_profile"]
                                    .get("browser", "")
                                    .encode(),
                                },
                            )

                        # Activity data (page views as sparse columns)
                        if "page_views" in session:
                            for idx, pv in enumerate(session["page_views"]):
                                # Column qualifier: pv_{timestamp}_{page_type}_{sequence}
                                pv_timestamp = encode_timestamp_reversed(
                                    pv["timestamp"]
                                )
                                col_qualifier = (
                                    f"pv_{pv_timestamp}_{pv['page_type']}_{idx:03d}"
                                )

                                batch.put(
                                    row_key,
                                    {
                                        f"activity:{col_qualifier}".encode(): json.dumps(
                                            {
                                                "duration": pv["view_duration"],
                                                "product_id": pv.get("product_id"),
                                                "category_id": pv.get("category_id"),
                                            }
                                        ).encode()
                                    },
                                )

                                # Track product views for metrics
                                if pv.get("product_id"):
                                    product_id = pv["product_id"]
                                    date_key = session["start_time"][:10].replace(
                                        "-", ""
                                    )

                                    if product_id not in product_metrics:
                                        product_metrics[product_id] = {}
                                    if date_key not in product_metrics[product_id]:
                                        product_metrics[product_id][date_key] = {
                                            "views": 0,
                                            "carts": 0,
                                            "purchases": 0,
                                        }
                                    product_metrics[product_id][date_key]["views"] += 1

                        # Cart contents
                        if "cart_contents" in session and session["cart_contents"]:
                            for product_id, cart_item in session[
                                "cart_contents"
                            ].items():
                                batch.put(
                                    row_key,
                                    {
                                        f"activity:cart_{product_id}".encode(): json.dumps(
                                            cart_item
                                        ).encode()
                                    },
                                )

                                # Track cart additions
                                date_key = session["start_time"][:10].replace("-", "")
                                if product_id not in product_metrics:
                                    product_metrics[product_id] = {}
                                if date_key not in product_metrics[product_id]:
                                    product_metrics[product_id][date_key] = {
                                        "views": 0,
                                        "carts": 0,
                                        "purchases": 0,
                                    }
                                product_metrics[product_id][date_key]["carts"] += 1

                        # If session converted, track purchase
                        if (
                            session["conversion_status"] == "converted"
                            and "cart_contents" in session
                        ):
                            for product_id, cart_item in session[
                                "cart_contents"
                            ].items():
                                date_key = session["start_time"][:10].replace("-", "")
                                if product_id not in product_metrics:
                                    product_metrics[product_id] = {}
                                if date_key not in product_metrics[product_id]:
                                    product_metrics[product_id][date_key] = {
                                        "views": 0,
                                        "carts": 0,
                                        "purchases": 0,
                                    }
                                product_metrics[product_id][date_key][
                                    "purchases"
                                ] += cart_item.get("quantity", 1)

                total_sessions_loaded += len(sessions)
                logger.info(f"Loaded {len(sessions)} sessions from {filename}")

            except Exception as e:
                logger.error(f"Failed to process {filename}: {str(e)}")

        logger.info(f"Total sessions loaded: {total_sessions_loaded}")

        # Step 2: Load product metrics into HBase
        logger.info("Loading product metrics into HBase...")

        with product_table.batch(batch_size=1000) as batch:
            for product_id, date_metrics in product_metrics.items():
                for date_key, metrics in date_metrics.items():
                    row_key = f"{product_id}|{date_key}".encode()

                    # Calculate conversion rate
                    views = metrics["views"]
                    purchases = metrics["purchases"]
                    conversion_rate = (purchases / views * 100) if views > 0 else 0

                    # Store daily aggregate
                    batch.put(
                        row_key,
                        {
                            b"daily:views": str(metrics["views"]).encode(),
                            b"daily:carts": str(metrics["carts"]).encode(),
                            b"daily:purchases": str(metrics["purchases"]).encode(),
                            b"daily:conversion_rate": str(round(conversion_rate, 2)).encode(),
                        },
                    )

        logger.info(f"Loaded product metrics for {len(product_metrics)} products")

        # Step 3: Verify data was loaded
        logger.info("Verifying data...")
        
        # Count rows in user_sessions
        count = 0
        for _ in table.scan(limit=10):
            count += 1
        logger.info(f"Verified: at least {count} sessions in HBase (showing first 10)")
        
        # Show sample row
        for key, data in table.scan(limit=1):
            logger.info(f"Sample row key: {key.decode()}")
            logger.info(f"  Session ID: {data.get(b'session:session_id', b'N/A').decode()}")
            break

        connection.close()
        logger.info("HBase loading complete!")

    except Exception as e:
        logger.error(f"HBase connection/loading failed: {str(e)}")
        raise

if __name__ == "__main__":
    load_hbase_enhanced()