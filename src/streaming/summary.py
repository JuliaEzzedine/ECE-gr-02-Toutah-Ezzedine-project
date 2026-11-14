"""Summarise the results of a streaming run.

    python -m src.streaming.summary --output outputs/streaming

Prints the figures and writes them to <output>/summary.md.
"""
import argparse
import os

from pyspark.sql import functions as F

from src.streaming.common import get_spark


def to_md(df, limit=50):
    return df.limit(limit).toPandas().to_markdown(index=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="outputs/streaming")
    args = parser.parse_args()

    spark = get_spark("imdb-wiki-stream-summary", with_kafka=False)
    matches = spark.read.parquet(os.path.join(args.output, "matches"))

    overview = matches.agg(
        F.count("*").alias("matching_edits"),
        F.countDistinct("title").alias("distinct_pages"),
        F.min("event_time").alias("first_event"),
        F.max("event_time").alias("last_event"),
        F.round(F.avg("arrival_delay_s"), 1).alias("avg_arrival_delay_s"),
        F.max("arrival_delay_s").alias("max_arrival_delay_s"),
    )
    by_type = matches.groupBy("entity_type", "match_quality").count().orderBy("entity_type", "match_quality")
    top_pages = (
        matches.groupBy("title", "entity_type")
        .agg(F.count("*").alias("edits"), F.first("wiki_url").alias("wiki_url"))
        .orderBy(F.desc("edits"), "title")
    )

    sections = [
        ("Overview", to_md(overview)),
        ("Matches by entity type and match quality", to_md(by_type)),
        ("Most edited pages", to_md(top_pages, 20)),
    ]

    activity_path = os.path.join(args.output, "activity_by_window")
    try:
        activity = spark.read.parquet(activity_path)
        per_window = (
            activity.groupBy("window_start", "window_end")
            .agg(F.sum("edits").alias("edits"), F.count("*").alias("entities"))
            .orderBy("window_start")
        )
        sections.append(("Edits per 10-minute event-time window", to_md(per_window)))
    except Exception:  # no window closed yet
        sections.append(("Edits per 10-minute event-time window",
                         "No window finalised yet (the watermark had not passed any window end)."))

    text = "# Streaming run summary\n\n" + "\n\n".join(f"## {t}\n\n{b}" for t, b in sections) + "\n"
    print(text)
    with open(os.path.join(args.output, "summary.md"), "w", encoding="utf-8") as fh:
        fh.write(text)
    print(f"[ok] written to {args.output}/summary.md")
    spark.stop()


if __name__ == "__main__":
    main()
