import os
import json
import matplotlib.pyplot as plt
import seaborn as sns

from logger import get_logger

# Initialize professional logging
logger = get_logger("visualizations")


def load_product_names(data_dir):
    """Helper function to dynamically map Product IDs to their actual names."""
    product_map = {}
    filepath = os.path.join(data_dir, "products.json")
    try:
        if os.path.exists(filepath):
            with open(filepath, "r") as f:
                products = json.load(f)
                for p in products:
                    # Truncate extremely long names to keep the chart clean
                    name = p.get("name", "Unknown")
                    if len(name) > 25:
                        name = name[:22] + "..."
                    product_map[p["product_id"]] = name
    except Exception as e:
        logger.error(f"Failed to load product names from JSON: {e}")
    return product_map


def create_visualizations():
    logger.info("Starting visualization generation...")

    # Set the style for professional-looking plots
    sns.set_theme(style="whitegrid")

    # Setup directories
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, "data")
    output_dir = os.path.join(base_dir, "visualizations")
    os.makedirs(output_dir, exist_ok=True)

    # CHART 1: Top 10 States by Revenue
    logger.info("Generating Geographic Sales Performance chart...")

    states_abbr = ["NY", "NH", "IL", "OR", "TN", "GU", "SD", "AS", "CA", "WY"]
    revenue = [
        5771707,
        5369863,
        5234465,
        5176177,
        5128650,
        5110240,
        4982029,
        4949543,
        4848740,
        4846171,
    ]

    # Map abbreviations to full names for a professional look
    state_mapping = {
        "NY": "New York",
        "NH": "New Hampshire",
        "IL": "Illinois",
        "OR": "Oregon",
        "TN": "Tennessee",
        "GU": "Guam",
        "SD": "South Dakota",
        "AS": "American Samoa",
        "CA": "California",
        "WY": "Wyoming",
    }
    full_states = [state_mapping.get(s, s) for s in states_abbr]
    revenue_millions = [r / 1000000 for r in revenue]

    plt.figure(figsize=(14, 6))

    # FIX: Added 'hue' and 'legend=False' to resolve Seaborn FutureWarning
    ax = sns.barplot(
        x=full_states,
        y=revenue_millions,
        hue=full_states,
        palette="viridis",
        legend=False,
    )

    plt.title("Top 10 Regions by Completed Transaction Revenue", fontsize=16, pad=15)
    plt.xlabel("Region", fontsize=12)
    plt.ylabel("Total Revenue (Millions USD)", fontsize=12)

    # Add value labels on top of bars
    for i, p in enumerate(ax.patches):
        ax.annotate(
            f"${revenue_millions[i]:.2f}M",
            (p.get_x() + p.get_width() / 2.0, p.get_height()),
            ha="center",
            va="bottom",
            fontsize=10,
            xytext=(0, 5),
            textcoords="offset points",
        )

    plt.tight_layout()
    chart1_path = os.path.join(output_dir, "state_revenue.png")
    plt.savefig(chart1_path, dpi=300)
    logger.info(f"Successfully saved: {chart1_path}")
    plt.close()

    # CHART 2: Product Recommendations (Affinity)
    logger.info("Generating Product Affinity chart with dynamic names...")

    # Load dynamic names
    product_map = load_product_names(data_dir)

    raw_pairs = [
        ("prod_00146", "prod_03953"),
        ("prod_02922", "prod_03140"),
        ("prod_00891", "prod_03676"),
        ("prod_01866", "prod_03802"),
        ("prod_00115", "prod_02767"),
    ]
    co_occurrences = [14, 14, 14, 14, 14]

    # Map IDs to names, fallback to ID if not found
    formatted_pairs = []
    for p1, p2 in raw_pairs:
        name1 = product_map.get(p1, p1)
        name2 = product_map.get(p2, p2)
        formatted_pairs.append(f"{name1}\n+\n{name2}")

    plt.figure(figsize=(12, 7))

    # 'hue' and 'legend=False' to resolve Seaborn FutureWarning
    ax = sns.barplot(
        x=co_occurrences,
        y=formatted_pairs,
        hue=formatted_pairs,
        palette="magma",
        orient="h",
        legend=False,
    )

    plt.title(
        "Top Product Affinity Pairs (Frequently Viewed Together)", fontsize=16, pad=15
    )
    plt.xlabel("Co-occurrence Count (Sessions)", fontsize=12)
    plt.ylabel("Product Combinations", fontsize=12)
    plt.xlim(0, 16)

    plt.tight_layout()
    chart2_path = os.path.join(output_dir, "product_affinity.png")
    plt.savefig(chart2_path, dpi=300)
    logger.info(f"Successfully saved: {chart2_path}")
    plt.close()

    logger.info("All visualizations generated successfully.")


if __name__ == "__main__":
    create_visualizations()
