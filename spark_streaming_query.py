# spark_streaming_query.py
from pyspark.sql import SparkSession, functions as F, types as T

INPUT_DIR = "spark_demo/input"
OUT_DIR   = "spark_demo/output/stream_categories"
CHK_DIR   = "spark_demo/checkpoints/stream_categories"

# fiksna šema (da streaming ne "pogađa" header)
schema = T.StructType([
    T.StructField("id", T.StringType()),
    T.StructField("title", T.StringType()),
    T.StructField("url", T.StringType()),
    T.StructField("is_paid", T.StringType()),
    T.StructField("instructor_names", T.StringType()),
    T.StructField("category", T.StringType()),
    T.StructField("headline", T.StringType()),
    T.StructField("num_subscribers", T.LongType()),
    T.StructField("rating", T.DoubleType()),
    T.StructField("num_reviews", T.LongType()),
    T.StructField("instructional_level", T.StringType()),  
    T.StructField("objectives", T.StringType()),
    T.StructField("curriculum", T.StringType()),
])

spark = (SparkSession.builder
         .appName("UdemySparkStreaming")
         .master("local[*]")
         .getOrCreate())

# čitaj CSV fajlove koji SE POJAVLJUJU u folderu (bez headera!) + ispravno parsiranje
stream_df = (spark.readStream
             .option("header", False)
             .option("multiLine", True)   # polja sadrže \n
             .option("quote", '"')        # CSV quote
             .option("escape", '"')       # escape za ugnježdene navodnike
             .schema(schema)
             .csv(INPUT_DIR))

# filtriraj redove sa nevažećim/subscribers NULL (zbog eventualno polomljenih linija)
stream_df = stream_df.filter(F.col("num_subscribers").isNotNull())

# live agregacija (BEZ orderBy na streaming DF-u)
agg = (stream_df
       .groupBy("category")
       .agg(F.count("*").alias("n_courses"),
            F.sum("num_subscribers").alias("subs_total")))

# foreachBatch: za svaki trigger upiši SORTIRANO stanje u poseban podfolder
def write_batch(batch_df, batch_id: int):
    (batch_df.orderBy(F.desc("subs_total"))
            .coalesce(1)
            .write
            .mode("overwrite")
            .parquet(f"{OUT_DIR}/batch={int(batch_id):05d}"))
    print(f"[write_batch] wrote batch {batch_id} → {OUT_DIR}/batch={int(batch_id):05d}")

# 1) prikaz u konzoli (neće biti sortirano — to je OK)
query_console = (agg.writeStream
                 .outputMode("complete")
                 .format("console")
                 .option("truncate", False)
                 .trigger(processingTime="5 seconds")
                 .start())

# 2) fajl izlaz preko foreachBatch (sortirano)
query_files = (agg.writeStream
               .outputMode("update")  # za foreachBatch nije presudno
               .option("checkpointLocation", CHK_DIR)
               .trigger(processingTime="5 seconds")
               .foreachBatch(write_batch)
               .start())

print("Streaming started. Watching folder:", INPUT_DIR)
print("Press Ctrl+C to stop.")
query_console.awaitTermination()
query_files.stop()
spark.stop()
