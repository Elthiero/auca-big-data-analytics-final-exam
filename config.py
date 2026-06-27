import os
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

class Config:
    """Centralized configuration for the analytics system."""
    
    # App Config
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    
    # MongoDB Config
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "ecommerce_analytics")
    
    # HBase Config
    HBASE_HOST = os.getenv("HBASE_HOST", "localhost")
    HBASE_PORT = int(os.getenv("HBASE_PORT", 9090))
    
    # Spark Config
    SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "ECommerceAnalytics")
    SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

# Create a global config instance that can be imported across modules
config = Config()