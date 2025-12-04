# Databricks notebook source
# NOTEBOOK 4: Score Viral Potential 
# Purpose: Compare stream posts vs viral archetypes and predict viral potential
# Schedule: Every 1 hour

import json
from datetime import datetime, timedelta
import numpy as np

from pyspark.sql.functions import (
    col, expr, lit, when, coalesce, concat_ws, trim, lower,
    current_date, current_timestamp, udf, array, struct,
    count as spark_count,
    sum as spark_sum,
    avg as spark_avg,
    min as spark_min,
    max as spark_max,
    row_number, desc, broadcast
)
from pyspark.sql.types import *
from pyspark.sql.window import Window

print("✅ Imports loaded")

# COMMAND ----------

# Configuration
SILVER_PATH = "/mnt/silver/reddit_posts"
GOLD_ARCHETYPES_PATH = "/mnt/gold/viral_archetypes"
GOLD_EMBEDDINGS_PATH = "/mnt/gold/viral_embeddings"
GOLD_PREDICTIONS_PATH = "/mnt/gold/viral_predictions"

# Scoring parameters
LOOKBACK_HOURS = 24  # Score posts from last 24h
VIRAL_THRESHOLD_HIGH = 0.70  # >= 70% similarity → HIGH potential
VIRAL_THRESHOLD_MEDIUM = 0.40  # >= 40% → MEDIUM
VIRAL_SCORE_CUTOFF = 100  # Posts with score >= 100 are already viral

print(f"📂 Silver: {SILVER_PATH}")
print(f"📂 Archetypes: {GOLD_ARCHETYPES_PATH}")
print(f"💾 Output: {GOLD_PREDICTIONS_PATH}")
print(f"\n⏰ Scoring window: Last {LOOKBACK_HOURS} hours")
print(f"🎯 Thresholds: HIGH ≥ {VIRAL_THRESHOLD_HIGH}, MEDIUM ≥ {VIRAL_THRESHOLD_MEDIUM}")

# COMMAND ----------

# Load viral archetypes
print("\n🔄 Loading viral archetypes...")

try:
    df_archetypes = spark.read.format("delta").load(GOLD_ARCHETYPES_PATH)
    
    archetype_count = df_archetypes.count()
    print(f"✅ Loaded {archetype_count} viral archetypes")
    
    # Show top 5
    print("\n📊 Top 5 archetypes:")
    df_archetypes.select(
        "archetype_rank", "title", "subreddit", "viral_score", "content_type"
    ).orderBy("archetype_rank").show(5, truncate=50)
    
    # Get archetype IDs to exclude from scoring
    archetype_ids = [row.id for row in df_archetypes.select("id").collect()]
    print(f"   Archetype IDs to exclude: {len(archetype_ids)}")
    
except Exception as e:
    print(f"❌ Error loading archetypes: {e}")
    dbutils.notebook.exit({"status": "error", "message": str(e)})

# COMMAND ----------

# Load embeddings (TF-IDF features)
print("\n🔄 Loading archetype embeddings...")

try:
    df_embeddings = spark.read.format("delta").load(GOLD_EMBEDDINGS_PATH)
    
    print(f"✅ Loaded embeddings for {df_embeddings.count()} archetypes")
    
except Exception as e:
    print(f"⚠️  Embeddings not found, will use feature-based scoring")
    df_embeddings = None

# COMMAND ----------

# Load Silver posts (posts to score)
print(f"\n🔄 Loading Silver posts (last {LOOKBACK_HOURS}h)...")

cutoff_timestamp = datetime.now() - timedelta(hours=LOOKBACK_HOURS)
print(f"   Cutoff: {cutoff_timestamp.strftime('%Y-%m-%d %H:%M:%S')}")

try:
    df_silver_all = spark.read.format("delta").load(SILVER_PATH)
    
    df_silver = (df_silver_all
        .filter(col("processed_ts") >= lit(cutoff_timestamp))
        .filter(~col("id").isin(archetype_ids))  # Exclude archetypes
    )
    
    silver_count = df_silver.count()
    print(f"✅ Posts to score: {silver_count:,}")
    
    if silver_count == 0:
        print("⚠️  No new posts to score")
        dbutils.notebook.exit({
            "status": "success",
            "message": "No posts to score",
            "scored_count": 0
        })
    
except Exception as e:
    print(f"❌ Error loading Silver: {e}")
    dbutils.notebook.exit({"status": "error", "message": str(e)})

# COMMAND ----------

# Label posts already viral (based on actual score)
print("\n🔄 Labeling already-viral posts...")

df_silver_labeled = (df_silver
    .withColumn("is_already_viral",
        when(col("score") >= VIRAL_SCORE_CUTOFF, True)
        .otherwise(False))
)

already_viral = df_silver_labeled.filter(col("is_already_viral") == True).count()
to_predict = df_silver_labeled.filter(col("is_already_viral") == False).count()

print(f"   ✅ Already viral: {already_viral}")
print(f"   🔮 To predict: {to_predict}")

# COMMAND ----------

# Calculate archetype statistics (for feature-based comparison)
print("\n🔄 Calculating archetype statistics...")

archetype_stats = df_archetypes.agg(
    spark_avg("score").alias("avg_score"),
    spark_avg("num_comments").alias("avg_comments"),
    spark_avg("engagement_score").alias("avg_engagement"),
    spark_avg("text_length").alias("avg_text_length"),
    spark_avg("title_length").alias("avg_title_length"),
    spark_avg("ratio").alias("avg_ratio")
).collect()[0]

print(f"   Avg archetype score: {archetype_stats.avg_score:.0f}")
print(f"   Avg archetype comments: {archetype_stats.avg_comments:.0f}")
print(f"   Avg archetype engagement: {archetype_stats.avg_engagement:.0f}")

# Get subreddit and content_type distributions
archetype_subreddits = {
    row.subreddit: row["count"] 
    for row in df_archetypes.groupBy("subreddit").agg(
        spark_count("*").alias("count")
    ).collect()
}

archetype_content_types = {
    row.content_type: row["count"]
    for row in df_archetypes.groupBy("content_type").agg(
        spark_count("*").alias("count")
    ).collect()
}

print(f"   Top subreddits: {list(archetype_subreddits.keys())[:5]}")
print(f"   Content types: {archetype_content_types}")

# COMMAND ----------

# Feature-based similarity calculation
print("\n🔄 Calculating feature-based similarity scores...")

# Broadcast archetype stats for efficient joins
broadcast_subreddits = spark.sparkContext.broadcast(archetype_subreddits)
broadcast_content_types = spark.sparkContext.broadcast(archetype_content_types)

# UDF for feature similarity
def calculate_feature_similarity(
    subreddit, content_type, score, num_comments, 
    engagement_score, text_length, title_length, ratio
):
    """
    Calculate similarity to viral archetypes based on features
    Returns score 0.0 - 1.0
    """
    similarity_score = 0.0
    
    # Subreddit match (30% weight)
    subreddit_weight = 0.3
    if subreddit in broadcast_subreddits.value:
        subreddit_freq = broadcast_subreddits.value[subreddit] / 40.0
        similarity_score += subreddit_freq * subreddit_weight
    
    # Content type match (20% weight)
    content_weight = 0.2
    if content_type in broadcast_content_types.value:
        content_freq = broadcast_content_types.value[content_type] / 40.0
        similarity_score += content_freq * content_weight
    
    # Engagement similarity (25% weight)
    engagement_weight = 0.25
    if archetype_stats.avg_engagement > 0:
        engagement_sim = min(engagement_score / archetype_stats.avg_engagement, 1.0)
        similarity_score += engagement_sim * engagement_weight
    
    # Text length similarity (15% weight)
    text_weight = 0.15
    if archetype_stats.avg_text_length > 0:
        text_sim = 1.0 - abs(text_length - archetype_stats.avg_text_length) / archetype_stats.avg_text_length
        text_sim = max(0.0, min(1.0, text_sim))
        similarity_score += text_sim * text_weight
    
    # Upvote ratio similarity (10% weight)
    ratio_weight = 0.1
    if archetype_stats.avg_ratio > 0:
        ratio_sim = min(ratio / archetype_stats.avg_ratio, 1.0)
        similarity_score += ratio_sim * ratio_weight
    
    return float(min(1.0, similarity_score))

# Register UDF
similarity_udf = udf(calculate_feature_similarity, DoubleType())

# Apply to Silver posts
df_scored = (df_silver_labeled
    .withColumn("viral_similarity_score",
        similarity_udf(
            col("subreddit"),
            col("content_type"),
            col("score"),
            col("num_comments"),
            col("engagement_score"),
            col("text_length"),
            col("title_length"),
            col("ratio")
        ))
)

print("✅ Feature similarity calculated")

# COMMAND ----------

# Label viral potential
print("\n🔄 Labeling viral potential...")

df_predicted = (df_scored
    .withColumn("viral_potential_label",
        when(col("is_already_viral") == True, "ALREADY_VIRAL")
        .when(col("viral_similarity_score") >= VIRAL_THRESHOLD_HIGH, "HIGH")
        .when(col("viral_similarity_score") >= VIRAL_THRESHOLD_MEDIUM, "MEDIUM")
        .otherwise("LOW"))
    
    .withColumn("is_viral",
        when(col("is_already_viral") == True, True)
        .when(col("viral_similarity_score") >= VIRAL_THRESHOLD_HIGH, True)
        .otherwise(False))
    
    .withColumn("prediction_timestamp", current_timestamp())
    .withColumn("prediction_date", current_date())
)

print("✅ Viral labels assigned")

# COMMAND ----------

# Show distribution
print("\n📊 Viral Potential Distribution:")

distribution = (df_predicted
    .groupBy("viral_potential_label")
    .agg(
        spark_count("*").alias("count"),
        spark_avg("score").alias("avg_score"),
        spark_avg("viral_similarity_score").alias("avg_similarity")
    )
    .orderBy(
        when(col("viral_potential_label") == "ALREADY_VIRAL", 1)
        .when(col("viral_potential_label") == "HIGH", 2)
        .when(col("viral_potential_label") == "MEDIUM", 3)
        .otherwise(4)
    )
)

display(distribution)

# COMMAND ----------

# Show top predictions (HIGH potential)
print("\n🏆 Top 10 HIGH Viral Potential Posts:")

high_potential = (df_predicted
    .filter(col("viral_potential_label") == "HIGH")
    .orderBy(desc("viral_similarity_score"), desc("engagement_score"))
    .select(
        "id",
        "title",
        "subreddit",
        "score",
        "num_comments",
        "viral_similarity_score",
        "content_type",
        "processed_ts"
    )
    .limit(10)
)

display(high_potential)

# COMMAND ----------

# Content type breakdown
print("\n📊 Viral Potential by Content Type:")

content_breakdown = (df_predicted
    .groupBy("content_type", "viral_potential_label")
    .agg(spark_count("*").alias("count"))
    .orderBy("content_type", "viral_potential_label")
)

display(content_breakdown)

# COMMAND ----------

# Subreddit analysis
print("\n📊 Top Subreddits with HIGH Potential:")

subreddit_analysis = (df_predicted
    .filter(col("viral_potential_label") == "HIGH")
    .groupBy("subreddit")
    .agg(
        spark_count("*").alias("high_potential_posts"),
        spark_avg("viral_similarity_score").alias("avg_similarity")
    )
    .orderBy(desc("high_potential_posts"))
    .limit(15)
)

display(subreddit_analysis)

# COMMAND ----------

# Save predictions to Gold
print(f"\n💾 Saving predictions to: {GOLD_PREDICTIONS_PATH}")

df_predictions_final = (df_predicted
    .select(
        "id",
        "title",
        "subreddit",
        "author",
        "score",
        "num_comments",
        "engagement_score",
        "viral_similarity_score",
        "viral_potential_label",
        "is_viral",
        "is_already_viral",
        "content_type",
        "text_length",
        "created_ts",
        "processed_ts",
        "prediction_timestamp",
        "prediction_date"
    )
)

# Write to Delta (partitioned by date and label)
(df_predictions_final.write
    .format("delta")
    .mode("append")  # Append new predictions
    .partitionBy("prediction_date", "viral_potential_label")
    .save(GOLD_PREDICTIONS_PATH)
)

scored_count = df_predictions_final.count()
print(f"✅ Saved {scored_count:,} predictions")

# COMMAND ----------

# Calculate prediction statistics
print("\n📊 Prediction Statistics:")

stats = df_predicted.agg(
    spark_count("*").alias("total_posts"),
    spark_sum(when(col("viral_potential_label") == "ALREADY_VIRAL", 1).otherwise(0)).alias("already_viral"),
    spark_sum(when(col("viral_potential_label") == "HIGH", 1).otherwise(0)).alias("high_potential"),
    spark_sum(when(col("viral_potential_label") == "MEDIUM", 1).otherwise(0)).alias("medium_potential"),
    spark_sum(when(col("viral_potential_label") == "LOW", 1).otherwise(0)).alias("low_potential"),
    spark_avg("viral_similarity_score").alias("avg_similarity"),
    spark_max("viral_similarity_score").alias("max_similarity")
).collect()[0]

print(f"   Total posts scored: {stats.total_posts}")
print(f"   Already viral: {stats.already_viral}")
print(f"   HIGH potential: {stats.high_potential}")
print(f"   MEDIUM potential: {stats.medium_potential}")
print(f"   LOW potential: {stats.low_potential}")
print(f"   Avg similarity: {stats.avg_similarity:.3f}")
print(f"   Max similarity: {stats.max_similarity:.3f}")

# COMMAND ----------

# Summary with recommendations
print("\n" + "=" * 60)
print("📊 VIRAL PREDICTION SUMMARY")
print("=" * 60)

summary = {
    "prediction_timestamp": datetime.now().isoformat(),
    "lookback_hours": LOOKBACK_HOURS,
    "archetypes_used": archetype_count,
    "posts_scored": int(stats.total_posts),
    "predictions": {
        "already_viral": int(stats.already_viral),
        "high_potential": int(stats.high_potential),
        "medium_potential": int(stats.medium_potential),
        "low_potential": int(stats.low_potential)
    },
    "similarity_stats": {
        "average": float(stats.avg_similarity),
        "maximum": float(stats.max_similarity)
    },
    "thresholds": {
        "high": VIRAL_THRESHOLD_HIGH,
        "medium": VIRAL_THRESHOLD_MEDIUM
    },
    "output_path": GOLD_PREDICTIONS_PATH
}

print(json.dumps(summary, indent=2))

# COMMAND ----------

# Save summary metadata
metadata_path = f"{GOLD_PREDICTIONS_PATH}/_metadata_{datetime.now().strftime('%Y%m%d_%H%M')}.json"

dbutils.fs.put(
    metadata_path,
    json.dumps(summary, indent=2),
    overwrite=True
)

print(f"\n✅ Metadata saved: {metadata_path}")

# COMMAND ----------

# Create data access layers - FIXED

print("\n🔄 Creating data access layers...")

try:
    # Read predictions
    df_predictions_view = spark.read.format("delta").load(GOLD_PREDICTIONS_PATH)
    
    # Create temp view
    df_predictions_view.createOrReplaceTempView("viral_predictions")
    view_count = df_predictions_view.count()
    print(f"✅ Temp view: viral_predictions ({view_count:,} rows)")
    
    # Try to create persistent table
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS reddit_analytics")
        
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS reddit_analytics.viral_predictions
            USING DELTA
            LOCATION '{GOLD_PREDICTIONS_PATH}'
        """)
        
        spark.sql("REFRESH TABLE reddit_analytics.viral_predictions")
        print("✅ Delta table: reddit_analytics.viral_predictions")
        
    except Exception as table_error:
        print(f"⚠️  Could not create persistent table: {table_error}")
    
except Exception as e:
    print(f"❌ Error creating views: {e}")

# COMMAND ----------

# Example queries and test

print("\n📝 Example SQL Queries:")
print("=" * 60)

print("\n-- High potential posts:")
print("SELECT * FROM viral_predictions WHERE viral_potential_label = 'HIGH'")

print("\n-- Today's summary:")
print("SELECT viral_potential_label, COUNT(*) FROM viral_predictions")
print("WHERE prediction_date = current_date() GROUP BY viral_potential_label")

print("\n-- Top posts by similarity:")
print("SELECT title, viral_similarity_score, is_viral FROM viral_predictions")
print("ORDER BY viral_similarity_score DESC LIMIT 20")

# Test query
print("\n🧪 Testing query...")

try:
    test_df = spark.sql("""
        SELECT viral_potential_label, COUNT(*) as count
        FROM viral_predictions
        GROUP BY viral_potential_label
    """)
    
    print("✅ Query works!")
    display(test_df)
    
except Exception as e:
    print(f"❌ Query failed: {e}")

# COMMAND ----------

# Recommendations
print("\n💡 RECOMMENDATIONS:")

high_count = int(stats.high_potential)
if high_count > 0:
    print(f"   ✅ {high_count} posts have HIGH viral potential")
    print("      → Consider boosting/promoting these posts")
    print("      → Monitor engagement closely")
else:
    print("   ⚠️  No HIGH potential posts found")
    print("      → Wait for next scoring cycle")

medium_count = int(stats.medium_potential)
if medium_count > 10:
    print(f"\n   📊 {medium_count} posts have MEDIUM potential")
    print("      → Watch for engagement trends")
    print("      → May become viral with right timing")

# COMMAND ----------

# Exit with results
dbutils.notebook.exit({
    "status": "success",
    "posts_scored": int(stats.total_posts),
    "high_potential": int(stats.high_potential),
    "medium_potential": int(stats.medium_potential),
    "low_potential": int(stats.low_potential),
    "already_viral": int(stats.already_viral),
    "output_path": GOLD_PREDICTIONS_PATH,
    "prediction_timestamp": datetime.now().isoformat()
})