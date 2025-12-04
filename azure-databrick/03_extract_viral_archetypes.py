# Databricks notebook source
# NOTEBOOK 3: Extract Viral Archetypes 
# Purpose: Identify top 40 viral posts from last 24 hours
# Schedule: Daily at 3 AM

import json
from datetime import datetime, timedelta

from pyspark.sql.functions import (
    col, expr, lit, when, coalesce, concat_ws, trim, lower, regexp_replace,
    current_date, current_timestamp, to_timestamp, hour, length,
    count as spark_count,
    sum as spark_sum,
    avg as spark_avg,
    min as spark_min,
    max as spark_max,
    row_number, desc, asc
)
from pyspark.sql.types import *
from pyspark.sql.window import Window

print("✅ Imports loaded")

# COMMAND ----------

# Configuration
SILVER_PATH = "/mnt/silver/reddit_posts"
HOT_POSTS_PATH = "/mnt/bronze/hot_posts"
GOLD_ARCHETYPES_PATH = "/mnt/gold/viral_archetypes"
GOLD_EMBEDDINGS_PATH = "/mnt/gold/viral_embeddings"

# Parameters
TOP_N_ARCHETYPES = 40
LOOKBACK_HOURS = 24
MIN_SCORE = 50  # Relaxed for testing
MIN_COMMENTS = 5

print(f"📂 Silver: {SILVER_PATH}")
print(f"📂 Hot Posts: {HOT_POSTS_PATH}")
print(f"💾 Output Archetypes: {GOLD_ARCHETYPES_PATH}")
print(f"💾 Output Embeddings: {GOLD_EMBEDDINGS_PATH}")
print(f"\n🎯 Target: Top {TOP_N_ARCHETYPES} viral posts")
print(f"⏰ Time window: Last {LOOKBACK_HOURS} hours")
print(f"📊 Min criteria: score >= {MIN_SCORE}, comments >= {MIN_COMMENTS}")

# COMMAND ----------

# Load Silver posts
print("\n🔄 Loading Silver posts...")

try:
    df_silver = spark.read.format("delta").load(SILVER_PATH)
    
    silver_count = df_silver.count()
    print(f"✅ Silver posts: {silver_count:,}")
    
    date_stats = df_silver.agg(
        spark_min("processing_date").alias("min_date"),
        spark_max("processing_date").alias("max_date")
    ).collect()[0]
    
    print(f"   Date range: {date_stats.min_date} to {date_stats.max_date}")
    
except Exception as e:
    print(f"❌ Error loading Silver: {e}")
    dbutils.notebook.exit({"status": "error", "message": str(e)})

# COMMAND ----------

# Load Hot posts - ALL BATCHES
print("\n🔄 Loading Hot posts from all batches...")

try:
    all_items = dbutils.fs.ls(HOT_POSTS_PATH)
    
    timestamp_folders = [
        f.path for f in all_items 
        if f.isDir() and not f.name.startswith('_')
    ]
    
    if not timestamp_folders:
        raise Exception("No hot posts batches found")
    
    print(f"   Found {len(timestamp_folders)} fetch batches")
    
    sorted_folders = sorted(timestamp_folders)
    for folder in sorted_folders:
        batch_name = folder.rstrip('/').split('/')[-1]
        print(f"     - {batch_name}")
    
    # Read all batches
    dfs = []
    for folder in timestamp_folders:
        try:
            df_batch = spark.read.format("parquet").load(folder)
            batch_count = df_batch.count()
            print(f"       ✅ {batch_count} posts")
            dfs.append(df_batch)
        except Exception as e:
            print(f"       ❌ Error: {e}")
            continue
    
    if not dfs:
        raise Exception("Failed to read any hot posts batch")
    
    # Union all
    df_hot_combined = dfs[0]
    for df in dfs[1:]:
        df_hot_combined = df_hot_combined.union(df)
    
    total_before_dedup = df_hot_combined.count()
    print(f"\n   📊 Total (all batches): {total_before_dedup:,}")
    
    # Deduplicate by ID (keep latest)
    window_dedup = Window.partitionBy("id").orderBy(desc("fetch_timestamp"))
    
    df_hot = (df_hot_combined
        .withColumn("_row_num", row_number().over(window_dedup))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )
    
    hot_count = df_hot.count()
    duplicates = total_before_dedup - hot_count
    
    print(f"   ✅ Unique hot posts: {hot_count:,}")
    print(f"   🔄 Duplicates removed: {duplicates:,}")
    
except Exception as e:
    print(f"❌ Error loading Hot posts: {e}")
    import traceback
    traceback.print_exc()
    dbutils.notebook.exit({"status": "error", "message": str(e)})

# COMMAND ----------

# Standardize Hot posts schema
print("\n🔄 Standardizing schemas...")

df_hot_standardized = (df_hot
    .withColumnRenamed("total_awards_received", "num_awards")
    .withColumnRenamed("upvote_ratio", "ratio")
    
    .withColumn("has_media", col("is_video"))
    .withColumn("processing_date", col("fetch_date"))
    .withColumn("processed_ts", col("fetch_ts"))
    .withColumn("eventhub_enqueued_time", lit(None).cast(TimestampType()))
    
    # Text features
    .withColumn("selftext_clean", coalesce(col("selftext"), lit("")))
    .withColumn("text_combined", 
                trim(lower(regexp_replace(
                    concat_ws(" ", col("title"), col("selftext_clean")), 
                    "[^a-zA-Z0-9 ]", " "
                ))))
    .withColumn("text_length", length(col("text_combined")))
    .withColumn("title_length", length(col("title")))
    
    # Engagement
    .withColumn("engagement_score", 
                (col("score") * 0.5) + 
                (col("num_comments") * 0.3) + 
                (col("num_awards") * 0.2))
    .withColumn("comment_to_score_ratio", 
                when(col("score") > 0, col("num_comments") / col("score")).otherwise(0))
    
    # Content type
    .withColumn("content_type",
                when(col("is_video"), "video")
                .when(col("is_self"), "text")
                .otherwise("link"))
    
    # Time features
    .withColumn("hour_of_day", hour(col("created_ts")))
    .withColumn("day_of_week", expr("dayofweek(created_ts)"))
    .withColumn("is_weekend", when(col("day_of_week").isin([1, 7]), 1).otherwise(0))
    
    .withColumn("data_source", lit("hot_api"))
    
    .drop("selftext_clean", "fetch_timestamp", "fetch_source", "fetch_hour", "is_self")
)

df_silver_marked = df_silver.withColumn("data_source", lit("stream"))

print("✅ Schemas standardized")

# COMMAND ----------

# Merge datasets
print("\n🔄 Merging Silver and Hot posts...")

common_columns = [
    "id", "title", "selftext", "subreddit", "author",
    "created_ts", "url", "permalink", 
    "over_18", "spoiler", "link_flair_text",
    "score", "ups", "downs", "ratio",
    "num_comments", "num_awards",
    "is_video", "has_media",
    "text_combined", "text_length", "title_length",
    "engagement_score", "comment_to_score_ratio",
    "content_type", "hour_of_day", "day_of_week", "is_weekend",
    "processing_date", "processed_ts",
    "data_source"
]

df_combined = (
    df_silver_marked.select(common_columns)
    .union(df_hot_standardized.select(common_columns))
)

# Deduplicate
window_dedup = Window.partitionBy("id").orderBy(desc("score"))

df_merged = (df_combined
    .withColumn("row_num", row_number().over(window_dedup))
    .filter(col("row_num") == 1)
    .drop("row_num")
)

merged_count = df_merged.count()
print(f"✅ Merged dataset: {merged_count:,} unique posts")

# COMMAND ----------

# Filter for LAST 24 HOURS only
print(f"\n🔄 Filtering posts from last {LOOKBACK_HOURS} hours...")

cutoff_timestamp = datetime.now() - timedelta(hours=LOOKBACK_HOURS)
print(f"   Cutoff: {cutoff_timestamp.strftime('%Y-%m-%d %H:%M:%S')}")

df_recent = (df_merged
    .filter(col("processed_ts") >= lit(cutoff_timestamp))
)

recent_count = df_recent.count()
print(f"✅ Posts in last {LOOKBACK_HOURS}h: {recent_count:,}")

if recent_count == 0:
    print("⚠️  No posts in 24h, trying 48h...")
    cutoff_timestamp = datetime.now() - timedelta(hours=48)
    df_recent = df_merged.filter(col("processed_ts") >= lit(cutoff_timestamp))
    recent_count = df_recent.count()
    print(f"✅ Posts in last 48h: {recent_count:,}")

# COMMAND ----------

# Apply viral criteria
print(f"\n🔄 Applying viral criteria...")

df_viral = (df_recent
    .filter(col("score") >= MIN_SCORE)
    .filter(col("num_comments") >= MIN_COMMENTS)
    .filter(col("author") != "[deleted]")
    .filter(col("author") != "[removed]")
    .filter(col("text_length") > 10)
)

viral_count = df_viral.count()
print(f"✅ Viral posts: {viral_count:,}")

if viral_count == 0:
    print("⚠️  No viral posts, lowering criteria...")
    df_viral = (df_recent
        .filter(col("score") >= 10)
        .filter(col("num_comments") >= 1)
        .filter(col("author") != "[deleted]")
        .filter(col("author") != "[removed]")
        .filter(col("text_length") > 5)
    )
    viral_count = df_viral.count()
    print(f"✅ Posts with relaxed criteria: {viral_count:,}")

if viral_count == 0:
    dbutils.notebook.exit({
        "status": "error",
        "message": "No posts found",
        "viral_count": 0
    })

# COMMAND ----------

# Calculate viral score (normalized)
print("\n🔄 Calculating viral scores...")

max_values = df_viral.agg(
    spark_max("score").alias("max_score"),
    spark_max("num_comments").alias("max_comments"),
    spark_max("num_awards").alias("max_awards")
).collect()[0]

print(f"   Max score: {max_values.max_score}")
print(f"   Max comments: {max_values.max_comments}")
print(f"   Max awards: {max_values.max_awards}")

df_viral_scored = (df_viral
    .withColumn("score_norm", col("score") / lit(max_values.max_score))
    .withColumn("comments_norm", col("num_comments") / lit(max_values.max_comments))
    .withColumn("awards_norm", 
                when(lit(max_values.max_awards) > 0, 
                     col("num_awards") / lit(max_values.max_awards))
                .otherwise(0))
    
    # Viral score = weighted combination
    .withColumn("viral_score",
                (col("score_norm") * 0.4) +
                (col("comments_norm") * 0.3) +
                (col("awards_norm") * 0.2) +
                (col("ratio") * 0.1))
)

print("✅ Viral scores calculated")

# COMMAND ----------

# Select Top N archetypes by viral score
print(f"\n🎯 Selecting Top {TOP_N_ARCHETYPES} viral archetypes...")

df_archetypes = (df_viral_scored
    .orderBy(desc("viral_score"), desc("engagement_score"), desc("score"))
    .limit(TOP_N_ARCHETYPES)
)

archetype_count = df_archetypes.count()
print(f"✅ Selected {archetype_count} archetypes")

# COMMAND ----------

# Top 10 preview
print("\n🏆 Top 10 Viral Archetypes:")

display(
    df_archetypes
    .select(
        "id",
        "title",
        "subreddit",
        "score",
        "num_comments",
        "viral_score",
        "content_type",
        "data_source"
    )
    .limit(10)
)

# COMMAND ----------

# Statistics
print("\n📊 Archetype Statistics:")

stats_df = df_archetypes.groupBy("content_type").agg(
    spark_count("*").alias("count"),
    spark_avg("score").alias("avg_score"),
    spark_avg("viral_score").alias("avg_viral_score")
).orderBy(desc("count"))

display(stats_df)

# COMMAND ----------

# Subreddit distribution
print("\n📊 Subreddit Distribution:")

subreddit_df = df_archetypes.groupBy("subreddit").agg(
    spark_count("*").alias("count"),
    spark_avg("viral_score").alias("avg_viral_score")
).orderBy(desc("count"))

display(subreddit_df.limit(15))

# COMMAND ----------

# Time distribution
print("\n⏰ Time Distribution:")

time_df = (df_archetypes
    .withColumn("hours_ago", 
        expr("(unix_timestamp(current_timestamp()) - unix_timestamp(processed_ts)) / 3600"))
    .withColumn("time_bucket",
        when(col("hours_ago") < 6, "0-6h")
        .when(col("hours_ago") < 12, "6-12h")
        .when(col("hours_ago") < 24, "12-24h")
        .otherwise(">24h"))
    .groupBy("time_bucket")
    .agg(spark_count("*").alias("count"))
    .orderBy("time_bucket")
)

display(time_df)

# COMMAND ----------

# Save archetypes to Gold
print(f"\n💾 Saving archetypes to: {GOLD_ARCHETYPES_PATH}")

df_archetypes_final = (df_archetypes
    .withColumn("archetype_rank", row_number().over(Window.orderBy(desc("viral_score"))))
    .withColumn("extraction_date", current_date())
    .withColumn("extraction_timestamp", current_timestamp())
)

(df_archetypes_final.write
    .format("delta")
    .mode("overwrite")
    .save(GOLD_ARCHETYPES_PATH)
)

print(f"✅ Saved {archetype_count} archetypes")

# COMMAND ----------

# Prepare for embeddings
print("\n🔄 Preparing for embeddings...")

df_for_embedding = (df_archetypes_final
    .select(
        "id",
        "archetype_rank",
        "title",
        "selftext",
        "text_combined",
        "subreddit",
        "viral_score",
        "content_type"
    )
    .withColumn("embedding_text", 
                concat_ws(" | ", col("title"), col("selftext")))
)

embedding_prep_path = GOLD_EMBEDDINGS_PATH + "_prep"

(df_for_embedding.write
    .format("parquet")
    .mode("overwrite")
    .save(embedding_prep_path)
)

print(f"✅ Embedding prep saved: {embedding_prep_path}")

# COMMAND ----------

# Generate TF-IDF embeddings
print("\n🔄 Generating TF-IDF embeddings...")

from pyspark.ml.feature import HashingTF, IDF, Tokenizer

tokenizer = Tokenizer(inputCol="text_combined", outputCol="words")
df_words = tokenizer.transform(df_archetypes_final)

hashingTF = HashingTF(inputCol="words", outputCol="raw_features", numFeatures=100)
df_tf = hashingTF.transform(df_words)

idf = IDF(inputCol="raw_features", outputCol="tfidf_features")
idf_model = idf.fit(df_tf)
df_embeddings = idf_model.transform(df_tf)

print("✅ TF-IDF embeddings generated")

# COMMAND ----------

# Save embeddings
print(f"\n💾 Saving embeddings to: {GOLD_EMBEDDINGS_PATH}")

df_embeddings_final = (df_embeddings
    .select(
        "id",
        "archetype_rank",
        "title",
        "subreddit",
        "viral_score",
        "tfidf_features",
        "extraction_date"
    )
)

(df_embeddings_final.write
    .format("delta")
    .mode("overwrite")
    .save(GOLD_EMBEDDINGS_PATH)
)

print(f"✅ Embeddings saved")

# COMMAND ----------

# Summary
print("\n" + "=" * 60)
print("📊 ARCHETYPE EXTRACTION SUMMARY")
print("=" * 60)

summary = {
    "extraction_timestamp": datetime.now().isoformat(),
    "lookback_hours": LOOKBACK_HOURS,
    "total_posts_analyzed": merged_count,
    "recent_posts": recent_count,
    "viral_posts_found": viral_count,
    "archetypes_extracted": archetype_count,
    "min_score": MIN_SCORE,
    "min_comments": MIN_COMMENTS,
    "top_subreddits": [row.subreddit for row in subreddit_df.limit(5).collect()],
    "content_types": {row.content_type: int(row["count"]) for row in stats_df.collect()}
}

print(json.dumps(summary, indent=2))

# Save metadata
metadata_path = f"{GOLD_ARCHETYPES_PATH}/_metadata.json"
dbutils.fs.put(metadata_path, json.dumps(summary, indent=2), overwrite=True)
print(f"\n✅ Metadata saved: {metadata_path}")

# COMMAND ----------

# Exit
dbutils.notebook.exit({
    "status": "success",
    "archetypes_extracted": archetype_count,
    "viral_posts_found": viral_count,
    "output_path": GOLD_ARCHETYPES_PATH,
    "extraction_timestamp": datetime.now().isoformat()
})