"""Percentage of null / missing / empty values for every column of every table.

For each column the job counts, in a single pass per table:
  - null_count      : value is NULL in Parquet (field absent in the source row)
  - imdb_null_count : value equals the IMDb marker \\N
  - empty_count     : empty or whitespace-only string
  - invalid_numeric : non-empty value in a numeric column that cannot be cast
pct_missing = (null + \\N + empty) / total rows * 100

Outputs:
  outputs/null_report.csv
  outputs/null_report.md   (full table + top 5 columns, ready for REPORT.md)

Usage: python -m src.prep.null_report
"""
import pandas as pd
from pyspark.sql import functions as F

from src.common.config import (
    IMDB_NULL, NUMERIC_COLUMNS, REPORTS_DIR, TABLES, bronze_path,
)
from src.common.spark import get_spark


def column_stats(spark, table: str) -> list[dict]:
    df = spark.read.parquet(bronze_path(table))
    numeric = set(NUMERIC_COLUMNS.get(table, []))

    aggs = [F.count(F.lit(1)).alias("__total")]
    for c in TABLES[table]:
        col = F.col(c)
        aggs += [
            F.sum(F.when(col.isNull(), 1).otherwise(0)).alias(f"{c}__null"),
            F.sum(F.when(col == IMDB_NULL, 1).otherwise(0)).alias(f"{c}__imdbnull"),
            F.sum(F.when(F.trim(col) == "", 1).otherwise(0)).alias(f"{c}__empty"),
        ]
        if c in numeric:
            bad = (
                col.isNotNull() & (col != IMDB_NULL) & (F.trim(col) != "")
                & col.cast("double").isNull()
            )
            aggs.append(F.sum(F.when(bad, 1).otherwise(0)).alias(f"{c}__invalid"))

    row = df.agg(*aggs).collect()[0].asDict()
    total = row["__total"]

    stats = []
    for c in TABLES[table]:
        null_c = row[f"{c}__null"] or 0
        imdb_c = row[f"{c}__imdbnull"] or 0
        empty_c = row[f"{c}__empty"] or 0
        missing = null_c + imdb_c + empty_c
        stats.append({
            "table": table,
            "column": c,
            "total_rows": total,
            "null_count": null_c,
            "imdb_null_count": imdb_c,
            "empty_count": empty_c,
            "invalid_numeric": row.get(f"{c}__invalid"),
            "missing_total": missing,
            "pct_missing": round(100 * missing / total, 3) if total else None,
        })
    print(f"[ok] {table}: {total:,} rows analysed")
    return stats


def main() -> None:
    spark = get_spark("imdb-null-report")
    rows = []
    for table in TABLES:
        rows += column_stats(spark, table)
    spark.stop()

    report = pd.DataFrame(rows)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report.to_csv(REPORTS_DIR / "null_report.csv", index=False)

    top5 = report.sort_values("pct_missing", ascending=False).head(5)
    cols = ["table", "column", "total_rows", "missing_total", "pct_missing"]
    md = [
        "## Missing values per column",
        "",
        report[cols + ["null_count", "imdb_null_count", "empty_count", "invalid_numeric"]]
        .to_markdown(index=False),
        "",
        "## Top 5 columns by percentage of missing values",
        "",
        top5[cols].to_markdown(index=False),
        "",
    ]
    (REPORTS_DIR / "null_report.md").write_text("\n".join(md), encoding="utf-8")

    print("\nTop 5 columns by % missing:")
    print(top5[cols].to_string(index=False))
    print(f"\nFull report: {REPORTS_DIR / 'null_report.md'}")


if __name__ == "__main__":
    main()
