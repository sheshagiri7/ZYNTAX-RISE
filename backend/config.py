"""
Configuration Management for ZYNTAX Pipeline
============================================
Handles environment variables with robust defaults for:
- API host & port
- Kafka bootstrap servers and topic
- Neo4j connection URI, credentials, and target database
"""

import os

# Server
PORT = int(os.getenv("PORT", 5000))
HOST = os.getenv("HOST", "0.0.0.0")
DEBUG = os.getenv("DEBUG", "False").lower() in ("true", "1", "yes")

# Kafka Settings
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "csv-rows")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "zyntax-loader-group")
KAFKA_RETRY_COUNT = int(os.getenv("KAFKA_RETRY_COUNT", 5))
KAFKA_RETRY_DELAY = float(os.getenv("KAFKA_RETRY_DELAY", 2.0))

# Neo4j Settings
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "csvgraphdb")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "CSV_Graph_DB")
NEO4J_RETRY_COUNT = int(os.getenv("NEO4J_RETRY_COUNT", 5))
NEO4J_RETRY_DELAY = float(os.getenv("NEO4J_RETRY_DELAY", 2.0))

# Ingestion Constraints
MAX_FILE_SIZE_BYTES = int(os.getenv("MAX_FILE_SIZE_BYTES", 50 * 1024 * 1024))  # 50MB
