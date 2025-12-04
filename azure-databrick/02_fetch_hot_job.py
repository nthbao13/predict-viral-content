# Databricks notebook source
# NOTEBOOK 2: Fetch Hot Posts from Reddit 
# Schedule: Every 6 hours

import praw
import json
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Reddit API Configuration (from .env)
CLIENT_ID = "" # ← Paste Reddit Client ID
CLIENT_SECRET = "" # ← Paste Reddit Client Secret
USER_AGENT = "" # ← Paste Reddit User Agent

# Storage paths
BRONZE_HOT_PATH = "/mnt/bronze/hot_posts"
CHECKPOINT_PATH = "/mnt/bronze/_checkpoints/hot_posts_fetch"

print(f"📡 Reddit API: {CLIENT_ID[:10]}...")
print(f"💾 Output: {BRONZE_HOT_PATH}")

# COMMAND ----------

# Initialize Reddit API
print("🔄 Initializing Reddit API...")

reddit = praw.Reddit(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    user_agent=USER_AGENT
)

# Test connection
try:
    subreddit = reddit.subreddit('python')
    print(f"✅ Connected to Reddit API")
    print(f"   Test subreddit: r/{subreddit.display_name}")
except Exception as e:
    print(f"❌ Reddit API error: {e}")
    dbutils.notebook.exit({"status": "error", "message": str(e)})

# COMMAND ----------

# Configuration: Subreddits to monitor
SUBREDDITS = [
    'all',           # Top posts from all subreddits
    'funny',
    'todayilearned',
    'worldnews',
    'news',
    'science',
    'technology',
    'gaming',
    'movies',
    'music',
    'books',
    'AskReddit',
    'explainlikeimfive',
    'Showerthoughts',
    'dataisbeautiful'
]

POSTS_PER_SUBREDDIT = 50  # Fetch top 50 hot posts from each

print(f"📋 Subreddits to monitor: {len(SUBREDDITS)}")
print(f"📊 Posts per subreddit: {POSTS_PER_SUBREDDIT}")
print(f"🎯 Total target posts: {len(SUBREDDITS) * POSTS_PER_SUBREDDIT}")

# COMMAND ----------

# Fetch hot posts function
def fetch_hot_posts(subreddit_name, limit=50):
    """Fetch hot posts from a subreddit"""
    posts = []
    
    try:
        subreddit = reddit.subreddit(subreddit_name)
        
        for post in subreddit.hot(limit=limit):
            # Skip stickied posts
            if post.stickied:
                continue
            
            posts.append({
                'id': post.id,
                'title': post.title,
                'selftext': post.selftext if hasattr(post, 'selftext') else '',
                'subreddit': str(post.subreddit),
                'author': str(post.author),
                'created_utc': post.created_utc,
                'url': post.url,
                'permalink': post.permalink,
                'over_18': post.over_18,
                'spoiler': post.spoiler,
                'link_flair_text': post.link_flair_text if hasattr(post, 'link_flair_text') else None,
                'score': post.score,
                'ups': post.ups,
                'downs': post.downs,
                'upvote_ratio': post.upvote_ratio,
                'num_comments': post.num_comments,
                'total_awards_received': post.total_awards_received if hasattr(post, 'total_awards_received') else 0,
                'is_video': post.is_video,
                'is_self': post.is_self,
                'is_original_content': post.is_original_content if hasattr(post, 'is_original_content') else False,
                'fetch_timestamp': datetime.utcnow().isoformat(),
                'fetch_source': 'hot'
            })
    
    except Exception as e:
        print(f"  ❌ Error fetching r/{subreddit_name}: {e}")
    
    return posts

# COMMAND ----------

# Fetch from all subreddits
print("\n🚀 Starting fetch process...")
print("=" * 60)

all_posts = []
fetch_stats = {}

for i, subreddit_name in enumerate(SUBREDDITS, 1):
    print(f"\n[{i}/{len(SUBREDDITS)}] 📡 Fetching r/{subreddit_name}...")
    
    posts = fetch_hot_posts(subreddit_name, limit=POSTS_PER_SUBREDDIT)
    
    all_posts.extend(posts)
    fetch_stats[subreddit_name] = len(posts)
    
    print(f"         ✅ Got {len(posts)} posts")

print("\n" + "=" * 60)
print(f"🎉 Fetch complete!")
print(f"📊 Total posts fetched: {len(all_posts)}")

# COMMAND ----------

# Show fetch statistics
print("\n📈 Fetch Statistics:")
print("=" * 60)

from operator import itemgetter

sorted_stats = sorted(fetch_stats.items(), key=itemgetter(1), reverse=True)

for sub, count in sorted_stats:
    bar = '█' * (count // 2)
    print(f"r/{sub:20s} {count:3d} posts {bar}")

# COMMAND ----------

# Check for duplicates
unique_ids = set(post['id'] for post in all_posts)
duplicates = len(all_posts) - len(unique_ids)

print(f"\n🔍 Duplicate check:")
print(f"   Total posts: {len(all_posts)}")
print(f"   Unique posts: {len(unique_ids)}")
print(f"   Duplicates: {duplicates}")

if duplicates > 0:
    print(f"\n⚠️  Removing {duplicates} duplicates...")
    # Deduplicate by ID (keep first occurrence)
    seen = set()
    all_posts = [p for p in all_posts if p['id'] not in seen and not seen.add(p['id'])]
    print(f"   ✅ After dedup: {len(all_posts)} posts")

# COMMAND ----------

# Convert to DataFrame
print("\n🔄 Creating DataFrame...")

schema = StructType([
    StructField("id", StringType()),
    StructField("title", StringType()),
    StructField("selftext", StringType()),
    StructField("subreddit", StringType()),
    StructField("author", StringType()),
    StructField("created_utc", DoubleType()),
    StructField("url", StringType()),
    StructField("permalink", StringType()),
    StructField("over_18", BooleanType()),
    StructField("spoiler", BooleanType()),
    StructField("link_flair_text", StringType()),
    StructField("score", LongType()),
    StructField("ups", LongType()),
    StructField("downs", LongType()),
    StructField("upvote_ratio", DoubleType()),
    StructField("num_comments", LongType()),
    StructField("total_awards_received", LongType()),
    StructField("is_video", BooleanType()),
    StructField("is_self", BooleanType()),
    StructField("is_original_content", BooleanType()),
    StructField("fetch_timestamp", StringType()),
    StructField("fetch_source", StringType())
])

df_hot = spark.createDataFrame(all_posts, schema)

# Add metadata
df_hot = (df_hot
    .withColumn("created_ts", to_timestamp(col("created_utc")))
    .withColumn("fetch_ts", col("fetch_timestamp").cast(TimestampType()))
    .withColumn("fetch_date", current_date())
    .withColumn("fetch_hour", hour(current_timestamp()))
    .withColumn("has_media", when(col("is_video"), True).otherwise(False))
)

print(f"✅ DataFrame created: {df_hot.count()} rows")

# COMMAND ----------

# Show sample
print("\n📋 Sample hot posts:")
display(
    df_hot
    .select("id", "title", "subreddit", "score", "num_comments", "upvote_ratio")
    .orderBy(col("score").desc())
    .limit(10)
)

# COMMAND ----------

# Save to Bronze (partitioned by date)
fetch_datetime = datetime.utcnow()
output_path = f"{BRONZE_HOT_PATH}/{fetch_datetime.strftime('%Y-%m-%d_%H-%M')}"

print(f"\n💾 Saving to: {output_path}")

(df_hot.write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("fetch_date", "subreddit")
    .save(output_path)
)

print(f"✅ Saved {df_hot.count()} posts")

# COMMAND ----------

# Summary statistics
print("\n" + "=" * 60)
print("📊 HOT POSTS SUMMARY")
print("=" * 60)

summary_df = (df_hot.groupBy("subreddit")
    .agg(
        expr("count(*) as posts"),  # ✅ Use SQL expression
        expr("avg(score) as avg_score"),
        expr("max(score) as max_score"),
        expr("avg(num_comments) as avg_comments"),
        expr("avg(upvote_ratio) as avg_ratio")
    )
    .orderBy(col("posts").desc())
)

display(summary_df)

# COMMAND ----------

# Top posts by engagement
print("\n🏆 Top 10 Hot Posts by Score:")

display(
    df_hot
    .select(
        "title",
        "subreddit",
        "score",
        "num_comments",
        "upvote_ratio",
        "created_ts"
    )
    .orderBy(col("score").desc())
    .limit(10)
)

# COMMAND ----------

# Content type breakdown
print("\n📊 Content Type Distribution:")

content_stats = (df_hot
    .withColumn("content_type",
        when(col("is_video"), "video")
        .when(col("is_self"), "text")
        .otherwise("link")
    )
    .groupBy("content_type")
    .agg(
        expr("count(*) as count"),  # ✅ Use SQL expression
        expr("avg(score) as avg_score"),
    )
    .orderBy(col("count").desc())
)

display(content_stats)

# COMMAND ----------

# Create metadata file
metadata = {
    "fetch_datetime": fetch_datetime.isoformat(),
    "total_posts": len(all_posts),
    "unique_posts": len(unique_ids),
    "duplicates_removed": duplicates,
    "subreddits_fetched": len(SUBREDDITS),
    "output_path": output_path,
    "fetch_stats": fetch_stats
}

metadata_path = f"{output_path}/_metadata.json"

# Save metadata
dbutils.fs.put(
    metadata_path,
    json.dumps(metadata, indent=2),
    overwrite=True
)

print(f"\n📝 Metadata saved to: {metadata_path}")

# COMMAND ----------

# Exit with results
print("\n✅ Job completed successfully!")

dbutils.notebook.exit({
    "status": "success",
    "total_posts": len(all_posts),
    "unique_posts": len(unique_ids),
    "output_path": output_path,
    "fetch_datetime": fetch_datetime.isoformat()
})