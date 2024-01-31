import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, FloatType, ArrayType, LongType, DoubleType
from cassandra.cluster import Cluster
from elasticsearch import Elasticsearch


spark = SparkSession.builder \
    .appName("KafkaSparkIntegration") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0,"
            "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .config('spark.cassandra.connection.host', 'localhost') \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,"
            "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0") \
    .getOrCreate()



# Define the Kafka broker and topic to consume data from
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "product"

# Define the Kafka consumer configuration
kafka_consumer_options = {
    "kafka.bootstrap.servers": kafka_bootstrap_servers,
    "subscribe": kafka_topic,
    "startingOffsets": "latest",
    "failOnDataLoss": "false"
}

# Create the schema to parse the JSON data
json_schema = StructType([
    StructField("available", BooleanType(), nullable=True),
    StructField("catId", StringType(), nullable=True),
    StructField("description_images", StringType(), nullable=True),
    StructField("images", StringType(), nullable=True),
    StructField("itemId", StringType(), nullable=True),
    StructField("itemUrl", StringType(), nullable=True),
    StructField("packageDetail", StringType(), nullable=True),
    StructField("properties_list", StringType(), nullable=True),
    StructField("reviews_averageStarRate", DoubleType(), nullable=True),
    StructField("reviews_count", DoubleType(), nullable=True),
    StructField("sales", StringType(), nullable=True),
    StructField("seller_companyId", StringType(), nullable=True),
    StructField("seller_sellerId", StringType(), nullable=True),
    StructField("seller_storeId", StringType(), nullable=True),
    StructField("seller_storeRating", DoubleType(), nullable=True),
    StructField("seller_storeTitle", StringType(), nullable=True),
    StructField("service_desc", StringType(), nullable=True),
    StructField("service_id", StringType(), nullable=True),
    StructField("service_title", StringType(), nullable=True),
    StructField("shippingFrom", StringType(), nullable=True),
    StructField("shippingOutDays", DoubleType(), nullable=True),
    StructField("shippingTo", StringType(), nullable=True),
    StructField("shipping_id", StringType(), nullable=True),
    StructField("sku_bulk_discount", DoubleType(), nullable=True),
    StructField("sku_bulk_quantity", DoubleType(), nullable=True),
    StructField("sku_isBulk", BooleanType(), nullable=True),
    StructField("sku_price", StringType(), nullable=True),
    StructField("sku_promotionPrice", StringType(), nullable=True),
    StructField("sku_quantity", DoubleType(), nullable=True),
    StructField("sku_unit", StringType(), nullable=True),
    StructField("title", StringType(), nullable=True),
    StructField("wishCount", DoubleType(), nullable=True)
])

# Read data from Kafka
kafka_stream = spark.readStream.format("kafka") \
    .options(**kafka_consumer_options) \
    .load()

# Deserialize the value from Kafka as JSON and parse it using the defined schema
kafka_stream = kafka_stream.selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json("json_value", json_schema).alias("data"))


# Define the Cassandra keyspace and table
cassandra_keyspace = "product_keyspace_test"
cassandra_table = "product_test3"


cluster = Cluster(['localhost'])
session = cluster.connect()

# Create the keyspace
create_keyspace_query = f"CREATE KEYSPACE IF NOT EXISTS {cassandra_keyspace} WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }}"
session.execute(create_keyspace_query)

# Use the keyspace
session.set_keyspace(cassandra_keyspace)

# Create the table with a generated UUID as the primary key
create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {cassandra_keyspace}.{cassandra_table} (
        id UUID PRIMARY KEY,
        available BOOLEAN,
        itemId TEXT,
        catId TEXT,
        description_images TEXT,
        images TEXT,
        itemUrl TEXT,
        reviews_averageStarRate DOUBLE,
        reviews_count DOUBLE,
        sales DOUBLE,
        seller_sellerId TEXT,
        seller_storeId TEXT,
        seller_storeRating DOUBLE,
        seller_storeTitle TEXT,
        service_desc TEXT,
        service_id TEXT,
        service_title TEXT,
        shippingFrom TEXT,
        shippingOutDays DOUBLE,
        shippingTo TEXT,
        shipping_id TEXT,
        sku_bulk_discount DOUBLE,
        sku_bulk_quantity DOUBLE,
        sku_isBulk BOOLEAN,
        sku_price TEXT,
        sku_promotionPrice TEXT,
        sku_quantity DOUBLE,
        title TEXT,
        wishCount DOUBLE,
        packageDetail TEXT,
        properties_list TEXT,
        seller_companyId TEXT
    );
"""

session.execute(create_table_query)

# Close the Cassandra session
session.shutdown()


# Replace null values in nested columns
columns_to_fill = ["data.wishCount", "data.reviews_count", "data.reviews_averageStarRate",
                   "data.sku_quantity", "data.sku_bulk_quantity", "data.sku_bulk_discount",
                   "data.shippingOutDays"]

for column in columns_to_fill:
    kafka_stream = kafka_stream.na.fill(0, subset=[column])

# Cast float columns
float_columns = ["data.wishCount", "data.reviews_count", "data.reviews_averageStarRate",
                 "data.sku_quantity", "data.sku_bulk_quantity", "data.sku_bulk_discount"]

for column in float_columns:
    kafka_stream = kafka_stream.withColumn(column, col(column).cast("float"))



# Cast "sales" column
kafka_stream = kafka_stream.withColumn("data.sales", regexp_replace("data.sales", ",", "").cast("double"))

# Drop unwanted characters from "seller_storeTitle" column
kafka_stream = kafka_stream.withColumn("seller_storeTitle", regexp_replace("data.seller_storeTitle", "[\",\\[\\]]", ""))

# Drop unwanted characters from "service_desc" column
kafka_stream = kafka_stream.withColumn("service_desc", regexp_replace("data.service_desc", "[\",\\[\\]]", ""))

# Splitting "sku_price" into min and max columns
kafka_stream = kafka_stream.withColumn("price_min", split(col("data.sku_price"), " - ")[0].cast("double"))
kafka_stream = kafka_stream.withColumn("price_max", split(col("data.sku_price"), " - ")[1].cast("double"))

# Drop NaN values in "shippingFrom" and "shippingTo" columns
kafka_stream = kafka_stream.na.drop(subset=["data.shippingFrom", "data.shippingTo", "price_min", "price_max"])

# Splitting "sku_promotionPrice" into min and max columns
kafka_stream = kafka_stream.withColumn("promotionPrice_min", split(col("data.sku_promotionPrice"), " - ")[0].cast("double"))
kafka_stream = kafka_stream.withColumn("promotionPrice_max", split(col("data.sku_promotionPrice"), " - ")[1].cast("double"))

# Drop NaN values in "shippingFrom" and "shippingTo" columns
kafka_stream = kafka_stream.na.drop(subset=["data.shippingFrom", "data.shippingTo", "price_min", "price_max", "promotionPrice_min", "promotionPrice_max"])

# Calculate the Total Price for each item
kafka_stream = kafka_stream.withColumn("total_price", col("data.sku_quantity") * (col("price_max") + col("price_min")) / 2)

# # Calculate the Discount Percentage (assuming you have both original and promotional prices)
# kafka_stream = kafka_stream.withColumn("discount_percentage", when(col("promotionPrice_max").isNotNull(),
#                                                                    (col("price_max") - col("promotionPrice_max")) / col("price_max") * 100)
#                                        .otherwise(lit(0)))

# Categorize Items Based on Price Range
price_bins = [float('-inf'), 50, 200, float('inf')]
price_labels = ["Low", "Medium", "High"]
kafka_stream = kafka_stream.withColumn("price_category", expr("CASE WHEN total_price <= 50 THEN 'Low' " +
                                                               "WHEN total_price <= 200 THEN 'Medium' " +
                                                               "ELSE 'High' END"))

# Calculate Shipping Duration
kafka_stream = kafka_stream.withColumn("shipping_duration", col("data.shippingOutDays") + lit(3))

kafka_stream = kafka_stream.na.drop()

kafka_stream = kafka_stream.withColumn("id", expr("uuid()"))


# Select relevant columns for the aggregated_data variable
aggregated_data = kafka_stream.select(
    col("id"),
    col("data.wishCount"),
    col("data.reviews_count"),
    col("data.reviews_averageStarRate"),
    col("data.sku_quantity"),
    col("data.sku_bulk_quantity"),
    col("data.sku_bulk_discount"),
    col("data.sales"),
    col("seller_storeTitle"),
    col("service_desc"),
    col("price_min"),
    col("price_max"),
    col("promotionPrice_min"),
    col("promotionPrice_max"),
    col("total_price"),
    col("discount_percentage"),
    col("price_category"),
    col("shipping_duration")
)


# Define the transformation for Cassandra
transform_data_cassandra = kafka_stream.select(
    col("id"),
    col("data.available"),
    col("data.itemId").alias("itemid"),
    col("data.catId").alias("catid"),
    col("data.description_images"),
    col("data.images"),
    col("data.itemUrl").alias("itemurl"),
    col("data.reviews_averageStarRate").alias("reviews_averagestarrate"),
    col("data.reviews_count"),
    col("data.sales"),
    col("data.seller_sellerId").alias("seller_sellerid"),
    col("data.seller_storeId").alias("seller_storeid"),
    col("data.seller_storeRating").alias("seller_storerating"),
    col("data.seller_storeTitle").alias("seller_storetitle"),
    col("data.service_desc"),
    col("data.service_id"),
    col("data.service_title"),
    col("data.shippingFrom").alias("shippingfrom"),
    col("data.shippingOutDays").alias("shippingoutdays"),
    col("data.shippingTo").alias("shippingto"),
    col("data.shipping_id"),
    col("data.sku_bulk_discount"),
    col("data.sku_bulk_quantity"),
    col("data.sku_isBulk").alias("sku_isbulk"),
    col("data.sku_price"),
    col("data.sku_promotionPrice").alias("sku_promotionprice"),
    col("data.sku_quantity"),
    col("data.title"),
    col("data.wishCount").alias("wishcount"),
    col("data.packageDetail").alias("packagedetail"),
    col("data.properties_list"),
    col("data.seller_companyId").alias("seller_companyid")
)

# Write the data to Cassandra
cassandra_write_options = {
    "keyspace": cassandra_keyspace,
    "table": cassandra_table,
}

cassandra_query = transform_data_cassandra.writeStream \
    .outputMode("append") \
    .format("org.apache.spark.sql.cassandra") \
    .option("checkpointLocation", "./checkpoint/data_cassandra") \
    .options(**cassandra_write_options) \
    .start()

# Define the transformation for Elasticsearch
transform_data_elasticsearch = kafka_stream.select(
    col("id"),
    col("data.wishCount"),
    col("data.reviews_count"),
    col("data.reviews_averageStarRate"),
    col("data.sku_quantity"),
    col("data.sku_bulk_quantity"),
    col("data.sku_bulk_discount"),
    col("data.sales"),
    col("seller_storeTitle"),
    col("service_desc"),
    col("price_min"),
    col("price_max"),
    col("promotionPrice_min"),
    col("promotionPrice_max"),
    col("total_price"),
    col("discount_percentage"),
    col("price_category"),
    col("shipping_duration")
)

# Write the data to Elasticsearch
elasticsearch_query = transform_data_elasticsearch.writeStream \
    .format("org.elasticsearch.spark.sql") \
    .outputMode("append") \
    .option("es.resource", "product") \
    .option("es.nodes", "localhost") \
    .option("es.port", "9200") \
    .option("es.nodes.wan.only", "false")\
    .option("es.index.auto.create", "true")\
    .option("checkpointLocation", "./checkpointLocation/") \
    .start()

# Wait for both queries to terminate (optional)
cassandra_query.awaitTermination()
elasticsearch_query.awaitTermination()
