"""Spark Structured Streaming job: Wikipedia changes relevant to the IMDb dataset.

Reads Wikimedia "recentchange" events from Kafka, keeps edits of English
Wikipedia articles, and joins them with the IMDb watchlist built by
src/streaming/watchlist.py.

    python -m src.streaming.stream_job --topic wiki-changes

Late data: events are timestamped with their Wikipedia event time and a
watermark (default 10 minutes) bounds how late an event may arrive. Duplicate
events and events older than the watermark are discarded.

Outputs (Parquet, under --output, default outputs/streaming):
    matches/             one row per matching Wikipedia edit
    activity_by_window/  edits per IMDb entity per 10-minute event-time window
"""
import argparse
import os

from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.streaming.common import (
    DEFAULT_TOPIC,
    KAFKA_BOOTSTRAP,
    WATCHLIST_PATH,
    get_spark,
    normalise_key,
    qualifier_kind,
)

EVENT_SCHEMA = T.StructType([
    T.StructField("id", T.LongType()),
    T.StructField("type", T.StringType()),
    T.StructField("namespace", T.IntegerType()),
    T.StructField("title", T.StringType()),
    T.StructField("comment", T.StringType()),
    T.StructField("timestamp", T.LongType()),
    T.StructField("user", T.StringType()),
    T.StructField("bot", T.BooleanType()),
    T.StructField("wiki", T.StringType()),
    T.StructField("server_url", T.StringType()),
    T.StructField("meta", T.StructType([
        T.StructField("id", T.StringType()),
        T.StructField("dt", T.StringType()),
        T.StructField("domain", T.StringType()),
    ])),
    T.StructField("length", T.StructType([
        T.StructField("old", T.LongType()),
        T.StructField("new", T.LongType()),
    ])),
])


def read_source(spark, args):
    """Raw JSON strings from Kafka, or from a folder of JSON-lines files (tests)."""
    if args.input_dir:
        return (
            spark.readStream.option("maxFilesPerTrigger", 1)
            .text(args.input_dir)
            .select(F.col("value").alias("json"), F.current_timestamp().alias("received_at"))
        )
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
        .select(F.col("value").cast("string").alias("json"),
                F.col("timestamp").alias("received_at"))
    )


def relevant_edits(raw, watchlist, watermark):
    events = (
        raw.select(F.from_json("json", EVENT_SCHEMA).alias("e"), "received_at")
        .select("e.*", "received_at")
        .where(
            (F.col("wiki") == "enwiki")
            & (F.col("namespace") == 0)  # articles only, no talk/user pages
            & F.col("type").isin("edit", "new")
            & F.col("timestamp").isNotNull()
        )
        .withColumn("event_time", F.timestamp_seconds("timestamp"))
        .withColumn("event_id", F.coalesce(F.col("meta.id"), F.col("id").cast("string")))
    )

    # Event-time watermark: drop duplicates and events later than the threshold.
    deduped = events.withWatermark("event_time", watermark).dropDuplicates(
        ["event_id", "event_time"]
    )

    candidates = deduped.withColumn("key", normalise_key(F.col("title"))).withColumn(
        "qualifier", qualifier_kind(F.col("title"))
    ).where(F.col("qualifier") != "other")

    # Stream-static join with the (small) watchlist.
    matches = candidates.join(F.broadcast(watchlist), "key").where(
        (F.col("qualifier") == "none") | (F.col("qualifier") == F.col("entity_type"))
    )

    return matches.select(
        "event_id",
        "event_time",
        "received_at",
        (F.unix_timestamp("received_at") - F.col("timestamp")).alias("arrival_delay_s"),
        "title",
        F.concat(F.lit("https://en.wikipedia.org/wiki/"),
                 F.regexp_replace("title", " ", "_")).alias("wiki_url"),
        "type",
        "user",
        "bot",
        (F.col("length.new") - F.col("length.old")).alias("size_delta"),
        "comment",
        "entity_type",
        "imdb_ids",
        "names",
        F.when(F.col("qualifier") == "none", "name_only")
        .otherwise("qualified").alias("match_quality"),
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--starting-offsets", default="latest", choices=["latest", "earliest"])
    parser.add_argument("--input-dir", help="read JSON-lines files instead of Kafka (tests)")
    parser.add_argument("--watchlist", default=WATCHLIST_PATH)
    parser.add_argument("--output", default="outputs/streaming")
    parser.add_argument("--watermark", default="10 minutes")
    parser.add_argument("--window", default="10 minutes")
    parser.add_argument("--trigger", default="30 seconds")
    parser.add_argument("--minutes", type=float, default=0,
                        help="stop after this many minutes (0 = run until Ctrl+C)")
    args = parser.parse_args()

    spark = get_spark("imdb-wiki-stream", with_kafka=not args.input_dir)
    watchlist = spark.read.parquet(args.watchlist).cache()
    print(f"[watchlist] {watchlist.count()} keys loaded from {args.watchlist}")

    matches = relevant_edits(read_source(spark, args), watchlist, args.watermark)
    checkpoints = os.path.join(args.output, "_checkpoints")

    def save_matches(batch_df, batch_id):
        batch_df = batch_df.cache()
        n = batch_df.count()
        if n:
            batch_df.write.mode("append").parquet(os.path.join(args.output, "matches"))
            print(f"\n[batch {batch_id}] {n} matching edit(s)")
            batch_df.select("event_time", "arrival_delay_s", "title", "entity_type",
                            "match_quality", "size_delta").show(20, truncate=40)
        else:
            print(f"[batch {batch_id}] no matching edit")
        batch_df.unpersist()

    q_matches = (
        matches.writeStream.foreachBatch(save_matches)
        .option("checkpointLocation", os.path.join(checkpoints, "matches"))
        .trigger(processingTime=args.trigger)
        .queryName("matches")
        .start()
    )

    # Windowed activity per IMDb entity. In append mode a window is written once
    # the watermark has passed its end, so late events can still be counted
    # until then, and events later than the watermark are ignored.
    activity = (
        matches.groupBy(F.window("event_time", args.window), "entity_type", "title")
        .agg(
            F.count("*").alias("edits"),
            F.approx_count_distinct("user").alias("editors"),
            F.sum("size_delta").alias("size_delta"),
            F.first("imdb_ids").alias("imdb_ids"),
            F.max("arrival_delay_s").alias("max_arrival_delay_s"),
        )
        .select(F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                "entity_type", "title", "imdb_ids", "edits", "editors",
                "size_delta", "max_arrival_delay_s")
    )
    q_activity = (
        activity.writeStream.outputMode("append")
        .format("parquet")
        .option("path", os.path.join(args.output, "activity_by_window"))
        .option("checkpointLocation", os.path.join(checkpoints, "activity"))
        .trigger(processingTime=args.trigger)
        .queryName("activity")
        .start()
    )

    try:
        if args.minutes:
            q_matches.awaitTermination(args.minutes * 60)
        else:
            q_matches.awaitTermination()
    except KeyboardInterrupt:
        pass
    finally:
        for q in (q_matches, q_activity):
            if q.isActive:
                q.stop()
        print(f"[done] results in {args.output}/matches and {args.output}/activity_by_window")
        spark.stop()


if __name__ == "__main__":
    main()
