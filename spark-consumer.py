import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession

def create_keyspace(session):
    session().execute("""
        CREATE KEYSPACE IF NOT EXISTS real_estate
        WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
def create_table(session):
    session().execute("""
        CREATE TABLE IF NOT EXISTS real_estate.property(
            address TEXT,
            title TEXT,
            link TEXT,
            pictures TEXT,
            floor_plan TEXT,
            property_details TEXT,
            price TEXT,
            bedrooms TEXT,
            bathrooms TEXT,
            receptions TEXT,
            EPC_Rating TEXT,
            tenure TEXT,
            time_remaining_on_lease TEXT,
            service_charge TEXT,
            council_tax_band TEXT,
            ground_rent TEXT,
            PRIMARY KEY(address)
        )
    """)
    print("table created successfully.")

def insert_data(session, **kwargs):
    query = (f"""
        INSERT INTO real_estate.property(
            address,
            title,
            link,
            pictures,
            floor_plan,
            property_details,
            price,
            bedrooms,
            bathrooms,
            receptions,
            EPC_Rating,
            tenure,
            time_remaining_on_lease,
            service_charge,
            council_tax_band,
            ground_rent
        ) VALUES (
            '{kwargs["address"]}',
            '{kwargs["title"]}',
            '{kwargs["link"]}',
            '{kwargs["pictures"]}',
            '{kwargs["floor_plan"]}',
            '{kwargs["property_details"]}',
            '{kwargs["price"]}',
            '{kwargs["bedrooms"]}',
            '{kwargs["bathrooms"]}',
            '{kwargs["receptions"]}',
            '{kwargs["EPC_Rating"]}',
            '{kwargs["tenure"]}',
            '{kwargs["time_remaining_on_lease"]}',
            '{kwargs["service_charge"]}',
            '{kwargs["council_tax_band"]}',
            '{kwargs["ground_rent"]}'
        )
    """)
    session().execute(query)
    print("Data inserted successfully.")


def cassandra_session():
    session = Cluster(['127.0.0.1']).connect
    if session is not None:
        create_keyspace(session)
        create_table(session)
    return session

def main():
    logging.basicConfig(level=logging.INFO)
    spark = (SparkSession.builder.appName("RealEstateConsumer")
            .config("spark.cassandra.connection.host", "localhost")
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0","org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")
            .getOrCreate())
    kafka_df = (spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "real-estate")
                .load())  
    schema = StructType([
        StructField("address", StringType(), True),
        StructField("title", StringType(), True),
        StructField("link", StringType(), True),
        StructField("pictures", StringType(), True),
        StructField("floor_plan", StringType(), True),
        StructField("property_details", StringType(), True),
        StructField("price", StringType(), True),
        StructField("bedrooms", StringType(), True),
        StructField("bathrooms", StringType(), True),
        StructField("receptions", StringType(), True),
        StructField("EPC_Rating", StringType(), True),
        StructField("tenure", StringType(), True),
        StructField("time_remaining_on_lease", StringType(), True),
        StructField("service_charge", StringType(), True),
        StructField("council_tax_band", StringType(), True),
        StructField("ground_rent", StringType(), True)

    ])
    
    kafka_df = (kafka_df.selectExpr("CAST(value AS STRING)")
        .select(from_json("value", schema).alias("data"))
        .select("data.*"))
    cassandra_query = (kafka_df.writeStream
        .foreachBatch(lambda batch_df,batch_id : batch_df.foreach(
            lambda row : insert_data(cassandra_session(),**row.asDict())

        ))
        .outputMode("append")
        .start()
        .awaitTermination()
        )

    print("Waiting for batches to be written...")
    cassandra_query.awaitTerminate()
if __name__ == "__main__":
    main()