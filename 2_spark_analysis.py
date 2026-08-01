import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType
import pyspark.sql.functions as F

from config import config
from logger import get_logger

logger = get_logger("spark_analytics")

def create_spark_session():
    """Initialize and return a Spark cluster connection."""
    logger.info(
        f"Starting Spark Session: {config.SPARK_APP_NAME} on {config.SPARK_MASTER}"
    )
    return (
        SparkSession.builder.appName(config.SPARK_APP_NAME)
        .master(config.SPARK_MASTER)
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .getOrCreate()
    )

def process_spark_analytics():
    spark = create_spark_session()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    
    # Prepend 'file://' to explicitly bypass HDFS and use the local hard drive
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))
    data_dir = f"file://{base_dir}"

    # REQUIREMENT 1: Clean and Normalize Data
    logger.info("Loading and cleaning raw datasets with explicit schemas...")

    # 1. Define explicit schemas (Column Pruning - only load what we need)
    txn_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("status", StringType(), True),
        StructField("total", DoubleType(), True)
    ])

    session_schema = StructType([
        StructField("session_id", StringType(), True),
        StructField("start_time", StringType(), True),
        StructField("end_time", StringType(), True),
        StructField("referrer", StringType(), True),
        StructField("viewed_products", ArrayType(StringType()), True)
    ])

    user_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("registration_date", StringType(), True),
        StructField("geo_data", StructType([
            StructField("state", StringType(), True)
        ]), True)
    ])

    # Load Transactions
    raw_txns = spark.read.option("multiline", "true").schema(txn_schema).json(os.path.join(data_dir, "transactions.json"))
    clean_txns = raw_txns.withColumn(
        "timestamp", F.to_timestamp(raw_txns["timestamp"])
    ).dropna(subset=["transaction_id", "user_id"])

    # Load Sessions (reading all 20 parts at once using wildcard)
    raw_sessions = spark.read.option("multiline", "true").schema(session_schema).json(os.path.join(data_dir, "sessions_*.json"))
    clean_sessions = (
        raw_sessions.withColumn(
            "start_time", F.to_timestamp(raw_sessions["start_time"])
        )
        .withColumn("end_time", F.to_timestamp(raw_sessions["end_time"]))
        .fillna({"referrer": "direct"})
    )

    # REQUIREMENT 2: Batch Processing (Product Recommendations)
    # Goal: "Users who viewed X also viewed Y" (Co-occurrence Matrix)
    logger.info("Running Batch Job: Product Affinity / Recommendations...")

    # Step A: Filter out sessions where users viewed fewer than 2 products (no pairs to make)
    views_df = clean_sessions.select("session_id", "viewed_products").filter(
        F.size("viewed_products") > 1
    )

    # Step B: Explode the array so each product gets its own row with the session_id
    exploded_views = views_df.select(
        "session_id", F.explode("viewed_products").alias("product_id")
    )

    # Step C: Self-join to find products viewed in the same session
    product_pairs = exploded_views.alias("p1").join(
        exploded_views.alias("p2"),
        (F.col("p1.session_id") == F.col("p2.session_id"))
        & (F.col("p1.product_id") < F.col("p2.product_id")),
    )

    # Step D: Group by the pair and count occurrences to find the strongest affinities
    recommendations = (
        product_pairs.groupBy("p1.product_id", "p2.product_id")
        .agg(F.count("*").alias("co_occurrence_count"))
        .orderBy(F.desc("co_occurrence_count"))
    )

    logger.info("Top 5 Product Affinity Pairs (Frequently Viewed Together):")
    top_pairs = recommendations.limit(5)
    top_pairs.show(5, truncate=False)

    # Persist for the visualisation layer (removes hard-coded values downstream)
    results_dir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "results")
    )
    os.makedirs(results_dir, exist_ok=True)

    top_pairs.toPandas().to_csv(
        os.path.join(results_dir, "product_affinity.csv"), index=False
    )
    logger.info("Wrote results/product_affinity.csv")

    # ---------------------------------------------------------------
    # NULL-MODEL BASELINE
    # The generator selects viewed products with random.choice(), so there is
    # no latent affinity structure in the data. We quantify this rather than
    # asserting the top pairs are meaningful: under uniform random viewing the
    # expected count for any given pair is (total pair instances) / C(N, 2).
    # A top count close to this baseline's tail means the "affinity" is noise.
    # ---------------------------------------------------------------
    total_pair_instances = product_pairs.count()
    n_products = exploded_views.select("product_id").distinct().count()
    possible_pairs = n_products * (n_products - 1) / 2
    expected_per_pair = (
        total_pair_instances / possible_pairs if possible_pairs > 0 else 0
    )
    observed_max = top_pairs.first()["co_occurrence_count"] if top_pairs.count() else 0

    logger.info("--- AFFINITY SIGNIFICANCE CHECK ---")
    logger.info(f"Distinct products viewed:      {n_products:,}")
    logger.info(f"Total co-view pair instances:  {total_pair_instances:,}")
    logger.info(f"Expected count per pair:       {expected_per_pair:.4f}")
    logger.info(f"Observed maximum count:        {observed_max}")
    logger.info(
        "Interpretation: with ~%.0f candidate pairs, a maximum of %d is "
        "consistent with the upper tail of random co-occurrence."
        % (possible_pairs, observed_max)
    )

    import pandas as pd

    pd.DataFrame(
        [
            {
                "distinct_products": n_products,
                "total_pair_instances": total_pair_instances,
                "possible_pairs": possible_pairs,
                "expected_count_per_pair": expected_per_pair,
                "observed_max_count": observed_max,
            }
        ]
    ).to_csv(os.path.join(results_dir, "affinity_baseline.csv"), index=False)

    # REQUIREMENT 3: Spark SQL Analytics
    logger.info("Running Spark SQL Analytics: Revenue by Geographic Region...")

    # Load and clean users
    raw_users = spark.read.option("multiline", "true").schema(user_schema).json(os.path.join(data_dir, "users.json"))
    clean_users = raw_users.withColumn(
        "registration_date", F.to_timestamp(raw_users["registration_date"])
    )

    # Register DataFrames as SQL Temporary Views
    clean_users.createOrReplaceTempView("users")
    clean_txns.createOrReplaceTempView("transactions")

    # SQL Query: Calculate the total revenue and average order value for users,
    # grouped by the state they live in, but only for completed transactions.
    complex_sql_query = """
        SELECT 
            u.geo_data.state as state,
            COUNT(DISTINCT u.user_id) as total_users,
            SUM(t.total) as total_revenue,
            ROUND(AVG(t.total), 2) as avg_order_value
        FROM users u
        JOIN transactions t ON u.user_id = t.user_id
        WHERE t.status = 'completed'
        GROUP BY u.geo_data.state
        HAVING total_revenue > 10000
        ORDER BY total_revenue DESC
        LIMIT 10
    """

    sql_results = spark.sql(complex_sql_query)
    logger.info("Top 10 States by Completed Transaction Revenue:")
    sql_results.show()

    sql_pdf = sql_results.toPandas()
    sql_pdf.to_csv(os.path.join(results_dir, "state_revenue.csv"), index=False)
    logger.info("Wrote results/state_revenue.csv")

    # Dispersion check: the generator assigns state via fake.state_abbr(),
    # i.e. uniformly at random. If the spread across the top 10 is small,
    # the ranking reflects sampling variation rather than genuine regional
    # demand, and must not be read as a targeting recommendation.
    if not sql_pdf.empty:
        hi = sql_pdf["total_revenue"].max()
        lo = sql_pdf["total_revenue"].min()
        spread = (hi - lo) / lo * 100
        logger.info("--- GEOGRAPHIC SIGNIFICANCE CHECK ---")
        logger.info(f"Top-10 revenue range: ${lo:,.0f} - ${hi:,.0f}")
        logger.info(f"Relative spread:      {spread:.1f}%")
        logger.info(
            "A narrow spread across uniformly-assigned states indicates the "
            "ranking is not a demand signal."
        )

    logger.info("Spark Analytics processing complete!")
    spark.stop()

if __name__ == "__main__":
    process_spark_analytics()