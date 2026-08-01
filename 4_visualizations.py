"""
Static chart generation for the analytics layer.

All figures are derived from persisted pipeline outputs -- no values are
hard-coded. Charts 1 and 2 read the CSVs written by 2_spark_analysis.py;
charts 3 and 4 read the materialised views and enriched user documents in
MongoDB. Each chart degrades gracefully (logs and skips) if its upstream
source has not been produced yet.
"""

import os
import json

import matplotlib

matplotlib.use("Agg")  # headless-safe backend
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from pymongo import MongoClient

from config import config
from logger import get_logger

logger = get_logger("visualizations")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
RESULTS_DIR = os.path.join(BASE_DIR, "results")
OUTPUT_DIR = os.path.join(BASE_DIR, "visualizations")

STATE_NAMES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota",
    "MS": "Mississippi", "MO": "Missouri", "MT": "Montana", "NE": "Nebraska",
    "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey",
    "NM": "New Mexico", "NY": "New York", "NC": "North Carolina",
    "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma", "OR": "Oregon",
    "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington",
    "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
    "DC": "District of Columbia", "PR": "Puerto Rico", "GU": "Guam",
    "AS": "American Samoa", "VI": "U.S. Virgin Islands",
    "MP": "Northern Mariana Islands",
}


def load_product_names():
    """Map product IDs to display names, truncated to keep axes readable."""
    product_map = {}
    filepath = os.path.join(DATA_DIR, "products.json")
    if not os.path.exists(filepath):
        logger.warning("products.json not found; affinity chart will show raw IDs.")
        return product_map
    try:
        with open(filepath, "r") as f:
            for p in json.load(f):
                name = p.get("name", "Unknown")
                if len(name) > 25:
                    name = name[:22] + "..."
                product_map[p["product_id"]] = name
    except Exception as e:
        logger.error(f"Failed to load product names: {e}")
    return product_map


def _require(path, chart_name):
    """Return True if an upstream artefact exists, else log and skip."""
    if os.path.exists(path):
        return True
    logger.warning(
        f"Skipping '{chart_name}': missing {os.path.relpath(path, BASE_DIR)}. "
        f"Run the upstream script first."
    )
    return False


# ---------------------------------------------------------------------------
# CHART 1 -- Revenue by region (source: Spark SQL)
# ---------------------------------------------------------------------------
def chart_state_revenue():
    path = os.path.join(RESULTS_DIR, "state_revenue.csv")
    if not _require(path, "state revenue"):
        return

    df = pd.read_csv(path).sort_values("total_revenue", ascending=False)
    df["region"] = df["state"].map(lambda s: STATE_NAMES.get(s, s))
    df["revenue_m"] = df["total_revenue"] / 1_000_000

    spread = (df["total_revenue"].max() - df["total_revenue"].min()) / df[
        "total_revenue"
    ].min() * 100

    plt.figure(figsize=(14, 6))
    ax = sns.barplot(
        data=df, x="region", y="revenue_m",
        hue="region", palette="viridis", legend=False,
    )
    for i, p in enumerate(ax.patches):
        ax.annotate(
            f"${df['revenue_m'].iloc[i]:.2f}M",
            (p.get_x() + p.get_width() / 2.0, p.get_height()),
            ha="center", va="bottom", fontsize=10,
            xytext=(0, 5), textcoords="offset points",
        )

    plt.title(
        "Top 10 Regions by Completed Transaction Revenue\n"
        f"Spread across top 10 is only {spread:.1f}% -- consistent with the "
        "generator's uniform state assignment",
        fontsize=14, pad=15,
    )
    plt.xlabel("Region", fontsize=12)
    plt.ylabel("Total Revenue (Millions USD)", fontsize=12)
    plt.xticks(rotation=20, ha="right")
    plt.tight_layout()

    out = os.path.join(OUTPUT_DIR, "state_revenue.png")
    plt.savefig(out, dpi=300)
    plt.close()
    logger.info(f"Saved: {out}")


# ---------------------------------------------------------------------------
# CHART 2 -- Product affinity, annotated with its random baseline
# ---------------------------------------------------------------------------
def chart_product_affinity():
    path = os.path.join(RESULTS_DIR, "product_affinity.csv")
    if not _require(path, "product affinity"):
        return

    df = pd.read_csv(path)
    # Spark emits two columns both named product_id; pandas suffixes them.
    id_cols = [c for c in df.columns if c.startswith("product_id")]
    if len(id_cols) < 2:
        logger.error(f"Unexpected affinity CSV columns: {list(df.columns)}")
        return

    product_map = load_product_names()
    df["pair"] = [
        f"{product_map.get(a, a)}\n+\n{product_map.get(b, b)}"
        for a, b in zip(df[id_cols[0]], df[id_cols[1]])
    ]

    subtitle = ""
    base_path = os.path.join(RESULTS_DIR, "affinity_baseline.csv")
    if os.path.exists(base_path):
        base = pd.read_csv(base_path).iloc[0]
        subtitle = (
            f"\nExpected count per pair under random viewing: "
            f"{base['expected_count_per_pair']:.2f} "
            f"across {base['possible_pairs']:,.0f} candidate pairs"
        )

    plt.figure(figsize=(12, 7))
    sns.barplot(
        data=df, x="co_occurrence_count", y="pair",
        hue="pair", palette="magma", orient="h", legend=False,
    )
    plt.title(
        "Top Product Affinity Pairs (Frequently Viewed Together)" + subtitle,
        fontsize=14, pad=15,
    )
    plt.xlabel("Co-occurrence Count (Sessions)", fontsize=12)
    plt.ylabel("Product Combinations", fontsize=12)
    plt.xlim(0, max(df["co_occurrence_count"]) * 1.15)
    plt.tight_layout()

    out = os.path.join(OUTPUT_DIR, "product_affinity.png")
    plt.savefig(out, dpi=300)
    plt.close()
    logger.info(f"Saved: {out}")


# ---------------------------------------------------------------------------
# CHART 3 -- Daily revenue by category (source: MongoDB materialised view)
# ---------------------------------------------------------------------------
def chart_category_revenue_over_time(db, top_n=5):
    rows = list(
        db.daily_sales_by_category.find(
            {}, {"_id": 0, "date": 1, "category_name": 1, "revenue": 1}
        )
    )
    if not rows:
        logger.warning(
            "Skipping 'category revenue over time': daily_sales_by_category is "
            "empty. Run 1_load_to_mongodb.py first."
        )
        return

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])

    top = (
        df.groupby("category_name")["revenue"].sum()
        .nlargest(top_n).index.tolist()
    )
    df = df[df["category_name"].isin(top)]
    pivot = df.pivot_table(
        index="date", columns="category_name", values="revenue", aggfunc="sum"
    ).sort_index()

    # 7-day rolling mean: daily series is noisy at this transaction volume
    smoothed = pivot.rolling(window=7, min_periods=1).mean()

    plt.figure(figsize=(14, 6))
    for col in smoothed.columns:
        plt.plot(smoothed.index, smoothed[col] / 1000, label=col, linewidth=2)

    plt.title(
        f"Daily Revenue by Category -- Top {top_n} Categories\n"
        "(7-day rolling mean, MongoDB daily_sales_by_category view)",
        fontsize=14, pad=15,
    )
    plt.xlabel("Date", fontsize=12)
    plt.ylabel("Revenue (Thousands USD)", fontsize=12)
    plt.legend(title="Category", fontsize=9, title_fontsize=10)
    plt.grid(axis="y", linestyle="--", alpha=0.3)
    plt.tight_layout()

    out = os.path.join(OUTPUT_DIR, "category_revenue_trend.png")
    plt.savefig(out, dpi=300)
    plt.close()
    logger.info(f"Saved: {out}")


# ---------------------------------------------------------------------------
# CHART 4 -- Customer segmentation (source: MongoDB users.segmentation_tags)
# ---------------------------------------------------------------------------
def chart_user_segmentation(db):
    pipeline = [
        {"$match": {"segmentation_tags": {"$ne": []}}},
        {"$unwind": "$segmentation_tags"},
        {
            "$group": {
                "_id": "$segmentation_tags",
                "user_count": {"$sum": 1},
                "avg_spend": {"$avg": "$lifetime_summary.total_spent"},
            }
        },
        {"$sort": {"user_count": -1}},
    ]
    rows = list(db.users.aggregate(pipeline))
    if not rows:
        logger.warning(
            "Skipping 'user segmentation': no segmentation_tags found. "
            "Run 1_load_to_mongodb.py first."
        )
        return

    df = pd.DataFrame(rows).rename(columns={"_id": "segment"})
    df["segment"] = df["segment"].str.replace("_", " ").str.title()

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    sns.barplot(
        data=df, x="segment", y="user_count",
        hue="segment", palette="crest", legend=False, ax=ax1,
    )
    ax1.set_title("Users per Segment", fontsize=13)
    ax1.set_xlabel("")
    ax1.set_ylabel("Number of Users", fontsize=11)
    ax1.tick_params(axis="x", rotation=25)
    for p in ax1.patches:
        ax1.annotate(
            f"{int(p.get_height()):,}",
            (p.get_x() + p.get_width() / 2.0, p.get_height()),
            ha="center", va="bottom", fontsize=9,
            xytext=(0, 4), textcoords="offset points",
        )

    sns.barplot(
        data=df, x="segment", y="avg_spend",
        hue="segment", palette="flare", legend=False, ax=ax2,
    )
    ax2.set_title("Average Lifetime Spend per Segment", fontsize=13)
    ax2.set_xlabel("")
    ax2.set_ylabel("Avg Lifetime Spend (USD)", fontsize=11)
    ax2.tick_params(axis="x", rotation=25)
    for p in ax2.patches:
        ax2.annotate(
            f"${p.get_height():,.0f}",
            (p.get_x() + p.get_width() / 2.0, p.get_height()),
            ha="center", va="bottom", fontsize=9,
            xytext=(0, 4), textcoords="offset points",
        )

    fig.suptitle(
        "Customer Segmentation (MongoDB embedded lifetime summaries)",
        fontsize=15,
    )
    plt.tight_layout()

    out = os.path.join(OUTPUT_DIR, "user_segmentation.png")
    plt.savefig(out, dpi=300)
    plt.close()
    logger.info(f"Saved: {out}")


def create_visualizations():
    logger.info("Starting visualization generation...")
    sns.set_theme(style="whitegrid")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    chart_state_revenue()
    chart_product_affinity()

    try:
        client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        db = client[config.MONGO_DB_NAME]
        chart_category_revenue_over_time(db)
        chart_user_segmentation(db)
        client.close()
    except Exception as e:
        logger.error(f"MongoDB unavailable, skipping charts 3-4: {e}")

    logger.info("Visualization generation finished.")


if __name__ == "__main__":
    create_visualizations()