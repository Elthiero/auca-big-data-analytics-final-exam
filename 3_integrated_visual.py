import os
import happybase
from pymongo import MongoClient
import matplotlib.pyplot as plt

from config import config
from logger import get_logger

logger = get_logger("generate_report")


def run_integrated_funnel_analysis():
    logger.info("Starting Integrated Funnel Analysis (HBase + MongoDB)...")

    # STEP 1: HBASE - Top of Funnel (Sessions & Intent)
    logger.info(f"Connecting to HBase on {config.HBASE_HOST}:{config.HBASE_PORT}...")
    hbase_conn = happybase.Connection(host=config.HBASE_HOST, port=config.HBASE_PORT)
    sessions_table = hbase_conn.table("user_sessions")

    total_sessions = 0
    sessions_with_cart = 0

    logger.info("Scanning HBase user_sessions to calculate cart abandonment...")

    # We iterate through the HBase rows. In a massive production cluster,
    # this would be offloaded to Spark, but this fulfills the integration requirement.
    for key, data in sessions_table.scan():
        total_sessions += 1

        # Check if the user added anything to their cart by looking for
        # the 'activity:cart_' prefix in the sparse column qualifiers.
        has_cart = any(col.startswith(b"activity:cart_") for col in data.keys())
        if has_cart:
            sessions_with_cart += 1

        if total_sessions % 500000 == 0:
            logger.info(f"Scanned {total_sessions} sessions from HBase...")

    hbase_conn.close()

    # STEP 2: MONGODB - Bottom of Funnel (Conversion)
    logger.info("Connecting to MongoDB...")
    mongo_client = MongoClient(config.MONGO_URI)
    db = mongo_client[config.MONGO_DB_NAME]

    logger.info("Querying MongoDB for completed transactions...")
    completed_purchases = db.transactions.count_documents({"status": "completed"})
    mongo_client.close()

    # STEP 3: CALCULATE METRICS & VISUALIZE
    logger.info("Calculating Funnel Metrics...")

    # Guard against zero division
    if total_sessions == 0:
        logger.error("No sessions found in HBase. Did the data load correctly?")
        return

    view_to_cart_rate = (sessions_with_cart / total_sessions) * 100
    cart_to_purchase_rate = (
        (completed_purchases / sessions_with_cart) * 100
        if sessions_with_cart > 0
        else 0
    )
    overall_conversion = (completed_purchases / total_sessions) * 100

    logger.info(f"--- FUNNEL METRICS ---")
    logger.info(f"Total Sessions (HBase): {total_sessions:,}")
    logger.info(
        f"Sessions with Cart Adds (HBase): {sessions_with_cart:,} ({view_to_cart_rate:.2f}% of total)"
    )
    logger.info(
        f"Completed Purchases (MongoDB): {completed_purchases:,} ({cart_to_purchase_rate:.2f}% of carts)"
    )
    logger.info(f"Overall Conversion Rate: {overall_conversion:.2f}%")

    # Create Visualization (Part 4 Requirement)
    stages = [
        "Total Sessions\n(HBase)",
        "Added to Cart\n(HBase)",
        "Completed Purchases\n(MongoDB)",
    ]
    values = [total_sessions, sessions_with_cart, completed_purchases]

    plt.figure(figsize=(10, 6))

    # Create the bar chart
    bars = plt.bar(stages, values, color=["#3498db", "#f39c12", "#2ecc71"])

    # Add exact numbers on top of each bar
    for bar in bars:
        yval = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width() / 2,
            yval + (max(values) * 0.02),
            f"{int(yval):,}",
            ha="center",
            va="bottom",
            fontweight="bold",
        )

    plt.title(
        "E-Commerce Conversion Funnel\nIntegrated Cross-Database Analysis",
        fontsize=14,
        pad=20,
    )
    plt.ylabel("Volume of Events", fontsize=12)
    plt.grid(axis="y", linestyle="--", alpha=0.3)
    
    # Ensure the visualizations directory exists
    vis_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "visualizations"))
    os.makedirs(vis_dir, exist_ok=True)
    
    # Save the plot inside the new folder
    output_path = os.path.join(vis_dir, "conversion_funnel.png")
    plt.savefig(output_path, bbox_inches="tight", dpi=300)

    plt.savefig(output_path, bbox_inches="tight", dpi=300)
    logger.info(f"Visualization successfully saved to: {output_path}")


if __name__ == "__main__":
    run_integrated_funnel_analysis()
