# spark_batch_analize.py
from pyspark.sql import SparkSession, functions as F, types as T

CSV_PATH = "udemy_data.csv"   # ako nije u istom folderu, prilagodi putanju

spark = (SparkSession.builder
         .appName("UdemyBatchAnalize")
         .master("local[*]")      # lokalni Spark na svim jezgriam
         .getOrCreate())

# 1) Čitanje CSV → Spark DF (podešeno za zareze/novoredove u tekst poljima)
df = (spark.read
      .option("header", True)
      .option("inferSchema", True)
      .option("multiLine", True)
      .option("escape", '"')
      .csv(CSV_PATH))

# 2) Doterivanje tipova koje ćemo koristiti u analizama
df = (df
      .withColumn("is_paid", F.col("is_paid").cast("string"))
      .withColumn("rating", F.col("rating").cast("double"))
      .withColumn("num_reviews", F.col("num_reviews").cast("long"))
      .withColumn("num_subscribers", F.col("num_subscribers").cast("long"))
)

print("Schema:")
df.printSchema()

# ========== ANALIZA A: Top 10 kategorija po ukupnim polaznicima ==========
top_cat = (df.groupBy("category")
             .agg(F.sum("num_subscribers").alias("subs"),
                  F.avg("rating").alias("avg_rating"),
                  F.count("*").alias("n_courses"))
             .orderBy(F.desc("subs"))
             .limit(10))
print("\n[A] Top 10 kategorija po polaznicima:")
top_cat.show(truncate=False)

# ========== ANALIZA B: Korelacije ==========
corr_reviews_rating = df.stat.corr("num_reviews", "rating")
corr_reviews_subs   = df.stat.corr("num_reviews", "num_subscribers")
print(f"\n[B] korelacije: reviews~rating={corr_reviews_rating:.4f}, reviews~subscribers={corr_reviews_subs:.4f}")

# ========== ANALIZA C: Plaćeni vs. Besplatni ==========
paid = (df.groupBy("is_paid")
          .agg(F.count("*").alias("n"),
               F.avg("rating").alias("avg_rating"),
               F.avg("num_reviews").alias("avg_reviews"),
               F.avg("num_subscribers").alias("avg_subscribers"))
          .orderBy(F.desc("n")))
print("\n[C] Paid vs Free:")
paid.show(truncate=False)

# ========== ANALIZA D: Nivo znanja ==========
level_stats = (df.groupBy("instructional_level")
                 .agg(F.count("*").alias("n"),
                      F.avg("rating").alias("avg_rating"),
                      F.avg("num_reviews").alias("avg_reviews"),
                      F.avg("num_subscribers").alias("avg_subscribers"))
                 .orderBy(F.desc("n")))
print("\n[D] Instructional level statistika:")
level_stats.show(truncate=False)

# ========== ANALIZA E: Top instruktori ==========
top_instructors = (df.groupBy("instructor_names")
                     .agg(F.sum("num_subscribers").alias("subs_total"),
                          F.avg("rating").alias("avg_rating"),
                          F.count("*").alias("n_courses"))
                     .orderBy(F.desc("subs_total"))
                     .limit(20))
print("\n[E] Top instruktori po ukupnim polaznicima:")
top_instructors.show(truncate=False)

# (opciono) snimi agregate u CSV/Parquet za grafove u prezentaciji
top_cat.coalesce(1).write.mode("overwrite").option("header", True).csv("spark_demo/output/top_categories_csv")
level_stats.coalesce(1).write.mode("overwrite").option("header", True).csv("spark_demo/output/levels_csv")
paid.coalesce(1).write.mode("overwrite").option("header", True).csv("spark_demo/output/paid_csv")
top_instructors.coalesce(1).write.mode("overwrite").option("header", True).csv("spark_demo/output/instructors_csv")

spark.stop()


