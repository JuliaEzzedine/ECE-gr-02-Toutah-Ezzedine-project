"""Run the exploration queries.

Usage:
  python -m src.explore.run            # all 8 questions
  python -m src.explore.run --q 2 3    # only questions 2 and 3

Results: outputs/exploration/qN_*.md (+ CSV folders for long results)
"""
import argparse
import time

from src.common.spark import get_spark
from src.explore.queries import ALL


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--q", type=int, nargs="*", default=list(ALL))
    args = parser.parse_args()

    spark = get_spark("imdb-exploration")
    for n in args.q:
        start = time.time()
        print(f"[q{n}] running {ALL[n].__name__} ...")
        ALL[n](spark)
        spark.catalog.clearCache()
        print(f"[q{n}] done in {time.time() - start:.0f}s")
    spark.stop()


if __name__ == "__main__":
    main()
