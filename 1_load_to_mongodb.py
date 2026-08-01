import json
import os
from datetime import datetime
from pymongo import MongoClient, ASCENDING, DESCENDING
from config import config
from logger import get_logger

logger = get_logger("load_to_mongodb")


def transform_product_with_category(product, categories_map):
    category_id = product.get("category_id")
    category_info = categories_map.get(category_id, {})

    # Find subcategory if it exists in the product
    subcategory_id = product.get("subcategory_id")
    subcategory_info = {}

    if subcategory_id and "subcategories" in category_info:
        for subcat in category_info["subcategories"]:
            if subcat["subcategory_id"] == subcategory_id:
                subcategory_info = subcat
                break

    # Add performance metrics placeholder (will be updated by aggregation)
    transformed = {
        "_id": product["product_id"],
        "name": product["name"],
        "base_price": product["base_price"],
        "current_stock": product["current_stock"],
        "is_active": product["is_active"],
        "creation_date": datetime.fromisoformat(
            product["creation_date"].replace("Z", "+00:00")
        ),
        "price_history": [
            {
                "price": ph["price"],
                "date": datetime.fromisoformat(ph["date"].replace("Z", "+00:00")),
            }
            for ph in product.get("price_history", [])
        ],
        "category": {
            "id": category_id,
            "name": category_info.get("name", "Unknown"),
            "subcategory": {
                "id": subcategory_id,
                "name": subcategory_info.get("name", "Unknown"),
                "profit_margin": subcategory_info.get("profit_margin", 0),
            },
        },
        "performance_metrics": {
            "total_views": 0,
            "total_cart_adds": 0,
            "total_purchases": 0,
            "conversion_rate": 0.0,
        },
    }
    return transformed


def transform_user(user):
    transformed = {
        "_id": user["user_id"],
        "geo_data": user["geo_data"],
        "registration_date": datetime.fromisoformat(
            user["registration_date"].replace("Z", "+00:00")
        ),
        "last_active": datetime.fromisoformat(
            user["last_active"].replace("Z", "+00:00")
        ),
        "lifetime_summary": {
            "total_sessions": 0,
            "total_purchases": 0,
            "total_spent": 0.0,
            "avg_order_value": 0.0,
            "preferred_category": None,
            "first_purchase_date": None,
            "last_purchase_date": None,
        },
        "segmentation_tags": [],
        "recent_sessions": [],
    }
    return transformed


def transform_transaction(transaction):
    transformed = {
        "_id": transaction["transaction_id"],
        "session_id": transaction["session_id"],
        "user_id": transaction["user_id"],
        "timestamp": datetime.fromisoformat(
            transaction["timestamp"].replace("Z", "+00:00")
        ),
        "items": [
            {
                "product_id": item["product_id"],
                "product_name": None,  # Will be enriched from products
                "quantity": item["quantity"],
                "unit_price": item["unit_price"],
                "subtotal": item["subtotal"],
                "category_id": None,  # Will be enriched
                "category_name": None,
                "profit_margin": None,
            }
            for item in transaction["items"]
        ],
        "financials": {
            "subtotal": transaction["subtotal"],
            "discount": transaction["discount"],
            "discount_percentage": (
                round(transaction["discount"] / transaction["subtotal"] * 100, 2)
                if transaction["subtotal"] > 0
                else 0
            ),
            "tax": 0,  # Calculate if needed
            "total": transaction["total"],
        },
        "payment_method": transaction["payment_method"],
        "status": transaction["status"],
        "session_context": {},  # Will be enriched from sessions
    }
    return transformed


def load_mongodb():
    logger.info(f"Connecting to MongoDB at {config.MONGO_URI}")
    client = MongoClient(config.MONGO_URI)
    db = client[config.MONGO_DB_NAME]

    # Step 1: Load and transform categories
    logger.info("Loading categories...")
    categories_path = "data/categories.json"
    categories_map = {}

    # Create a map of category_id to category info for easy lookup during product transformation
    with open(categories_path, "r") as f:
        categories = json.load(f)
        for cat in categories:
            categories_map[cat["category_id"]] = cat

    # Load categories into MongoDB
    db.categories.drop()
    db.categories.insert_many(categories)
    logger.info(f"{len(categories)} categories loaded into MongoDB")

    # Step 2: Load and transform products with category embedding
    logger.info("Loading and transforming products...")
    products_path = "data/products.json"

    # Create a product lookup map for enrichment
    with open(products_path, "r") as f:
        products = json.load(f)

    # Transform products with embedded category and subcategory information
    transformed_products = [
        transform_product_with_category(p, categories_map) for p in products
    ]

    # Load transformed products into MongoDB
    db.products.drop()
    db.products.insert_many(transformed_products)

    # Create indexes for products
    db.products.create_index([("category.id", ASCENDING)])
    db.products.create_index([("base_price", ASCENDING)])
    db.products.create_index([("current_stock", ASCENDING)])
    db.products.create_index([("performance_metrics.total_views", DESCENDING)])

    logger.info(f"{len(transformed_products)} products loaded into MongoDB")

    # Step 3: Load users
    logger.info("Loading users...")
    users_path = "data/users.json"

    # Read Users from json file
    with open(users_path, "r") as f:
        users = json.load(f)

    # Transform users with embedded lifetime summaries and segmentation tags
    transformed_users = [transform_user(u) for u in users]

    # Load transformed users into MongoDB
    db.users.drop()
    db.users.insert_many(transformed_users)

    # Create indexes for users
    db.users.create_index([("geo_data.country", ASCENDING)])
    db.users.create_index([("geo_data.state", ASCENDING)])
    db.users.create_index([("registration_date", DESCENDING)])
    db.users.create_index([("lifetime_summary.total_spent", DESCENDING)])
    db.users.create_index([("segmentation_tags", ASCENDING)])

    logger.info(f"{len(transformed_users)} users loaded into MongoDB")

    # Step 4: Load transactions
    logger.info("Loading transactions...")
    transactions_path = "data/transactions.json"

    # Load transactions from json file
    with open(transactions_path, "r") as f:
        transactions = json.load(f)

    # Create a product lookup map for enrichment
    product_map = {p["_id"]: p for p in transformed_products}

    # Enrich transactions with product and category info
    enriched_transactions = []
    for txn in transactions:
        transformed_txn = transform_transaction(txn)

        # Enrich each item with product details
        for item in transformed_txn["items"]:
            product = product_map.get(item["product_id"])
            if product:
                item["product_name"] = product["name"]
                item["category_id"] = product["category"]["id"]
                item["category_name"] = product["category"]["name"]
                item["profit_margin"] = product["category"]["subcategory"][
                    "profit_margin"
                ]

        enriched_transactions.append(transformed_txn)

    # Load enriched transactions into MongoDB
    db.transactions.drop()
    db.transactions.insert_many(enriched_transactions)

    # Create indexes for transactions
    db.transactions.create_index([("timestamp", DESCENDING)])
    db.transactions.create_index([("user_id", ASCENDING)])
    db.transactions.create_index([("session_id", ASCENDING)])
    db.transactions.create_index([("status", ASCENDING)])
    db.transactions.create_index([("financials.total", DESCENDING)])
    db.transactions.create_index([("items.category_id", ASCENDING)])

    logger.info(f"{len(enriched_transactions)} transactions loaded into MongoDB")

    # Step 5: Update user lifetime summaries from transactions
    logger.info("Updating user lifetime summaries...")

    # Aggregate transactions to calculate lifetime summaries for each user
    pipeline = [
        {"$match": {"status": "completed"}},
        {
            "$group": {
                "_id": "$user_id",
                "total_spent": {"$sum": "$financials.total"},
                "transaction_count": {"$sum": 1},
                "last_purchase": {"$max": "$timestamp"},
                "first_purchase": {"$min": "$timestamp"},
                # Find most purchased category
                "categories": {"$push": "$items.category_name"},
            }
        },
        {
            "$project": {
                "total_spent": 1,
                "transaction_count": 1,
                "last_purchase": 1,
                "first_purchase": 1,
                "avg_order_value": {"$divide": ["$total_spent", "$transaction_count"]},
            }
        },
    ]

    user_stats = {}
    for result in db.transactions.aggregate(pipeline):
        user_stats[result["_id"]] = result

    # Derive segment boundaries from the observed distribution before tagging
    thresholds = compute_segmentation_thresholds(user_stats)

    # Update users with their lifetime summaries
    for user_id, stats in user_stats.items():
        db.users.update_one(
            {"_id": user_id},
            {
                "$set": {
                    "lifetime_summary.total_spent": stats["total_spent"],
                    "lifetime_summary.total_purchases": stats["transaction_count"],
                    "lifetime_summary.avg_order_value": stats["avg_order_value"],
                    "lifetime_summary.first_purchase_date": stats["first_purchase"],
                    "lifetime_summary.last_purchase_date": stats["last_purchase"],
                    # Add segmentation tag based on spending
                    "segmentation_tags": get_segmentation_tags(stats, thresholds),
                }
            },
        )

    logger.info("User lifetime summaries updated")

    # Step 6: Create additional materialized views for analytics
    create_analytics_views(db)

    logger.info("MongoDB loading complete!")
    client.close()


def compute_segmentation_thresholds(user_stats):
    """
    Derive segment boundaries from the observed distribution rather than from
    fixed dollar amounts.

    Fixed thresholds are only meaningful relative to a known average order
    value. On this dataset the mean lifetime spend is roughly $26,000 across
    ~32 completed purchases per user, so absolute cut-offs of $1,000 and 10
    purchases place every single user in the top tier and leave the lower
    tiers empty. Quantiles adapt to whatever distribution is present and keep
    the segments populated as catalogue and order value shift.
    """
    spends = sorted(s["total_spent"] for s in user_stats.values())
    counts = sorted(s["transaction_count"] for s in user_stats.values())

    def pct(sorted_vals, q):
        if not sorted_vals:
            return 0
        idx = min(int(len(sorted_vals) * q), len(sorted_vals) - 1)
        return sorted_vals[idx]

    thresholds = {
        "spend_high": pct(spends, 0.80),    # top 20% by spend
        "spend_mid": pct(spends, 0.40),     # middle 40%
        "count_high": pct(counts, 0.80),    # top 20% by frequency
        "count_mid": pct(counts, 0.40),
    }

    logger.info(
        "Segmentation thresholds (quantile-derived): "
        f"spend p80=${thresholds['spend_high']:,.0f}, "
        f"p40=${thresholds['spend_mid']:,.0f}; "
        f"count p80={thresholds['count_high']}, p40={thresholds['count_mid']}"
    )
    return thresholds


def get_segmentation_tags(stats, thresholds):
    """Assign value and frequency tags relative to the cohort distribution."""
    tags = []

    if stats["total_spent"] >= thresholds["spend_high"]:
        tags.append("high_value")
    elif stats["total_spent"] >= thresholds["spend_mid"]:
        tags.append("medium_value")
    elif stats["total_spent"] > 0:
        tags.append("low_value")

    if stats["transaction_count"] >= thresholds["count_high"]:
        tags.append("frequent_buyer")
    elif stats["transaction_count"] >= thresholds["count_mid"]:
        tags.append("regular_buyer")
    elif stats["transaction_count"] > 0:
        tags.append("new_buyer")

    return tags


def create_analytics_views(db):

    # Daily sales by category view
    db.daily_sales_by_category.drop()

    pipeline = [
        {"$match": {"status": "completed"}},
        {"$unwind": "$items"},
        {
            "$group": {
                "_id": {
                    "date": {
                        "$dateToString": {"format": "%Y-%m-%d", "date": "$timestamp"}
                    },
                    "category_id": "$items.category_id",
                    "category_name": "$items.category_name",
                },
                "revenue": {"$sum": "$items.subtotal"},
                "units_sold": {"$sum": "$items.quantity"},
                "transactions": {"$addToSet": "$_id"},
            }
        },
        {
            "$project": {
                "date": "$_id.date",
                "category_id": "$_id.category_id",
                "category_name": "$_id.category_name",
                "revenue": 1,
                "units_sold": 1,
                "transaction_count": {"$size": "$transactions"},
            }
        },
        {"$out": "daily_sales_by_category"},
    ]

    db.transactions.aggregate(pipeline)
    logger.info("Created daily_sales_by_category materialized view")


if __name__ == "__main__":
    load_mongodb()