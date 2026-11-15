"""The 8 exploration questions (Spark batch, on the silver tables).

Each function computes one question, writes outputs/exploration/qN_*.md
(and a CSV with the full result when it is long) and returns nothing.
Thresholds are function parameters so they can be changed from the command
line (see run.py) and are listed at the top of every output file.
"""
from pyspark.sql import Window
from pyspark.sql import functions as F

from src.explore.common import (
    ACTING, MIN_VOTES, YEAR_MAX, YEAR_MIN, feature_movies, rated, save, table,
)

# Main language of a production country, used only when IMDb gives no
# language for the original title (title.akas.language is often empty).
REGION_LANGUAGE = {
    "US": "en", "GB": "en", "CA": "en", "AU": "en", "IE": "en", "NZ": "en",
    "FR": "fr", "BE": "fr", "DE": "de", "AT": "de", "IT": "it", "ES": "es",
    "MX": "es", "AR": "es", "CO": "es", "CL": "es", "PE": "es", "VE": "es",
    "PT": "pt", "BR": "pt", "JP": "ja", "KR": "ko", "CN": "zh", "TW": "zh",
    "HK": "zh", "IN": "hi", "RU": "ru", "SU": "ru", "SE": "sv", "NO": "no",
    "DK": "da", "FI": "fi", "NL": "nl", "PL": "pl", "CZ": "cs", "CSHH": "cs",
    "HU": "hu", "GR": "el", "TR": "tr", "IR": "fa", "IL": "he", "EG": "ar",
    "TH": "th", "ID": "id", "PH": "tl", "RO": "ro", "UA": "uk", "XWG": "de",
    "DDDE": "de", "YUCS": "sr", "RS": "sr", "HR": "hr", "IS": "is",
}


# --------------------------------------------------------------------------- #
def q1_runtime_outliers(spark, z_threshold: float = 3.0):
    """Feature films whose runtime is notably different from their genre average."""
    movies = feature_movies(spark).select(
        "tconst", "primaryTitle", "startYear", "runtimeMinutes", "numVotes",
        F.explode("genres").alias("genre"))
    stats = Window.partitionBy("genre")
    scored = (
        movies
        .withColumn("genre_mean", F.avg("runtimeMinutes").over(stats))
        .withColumn("genre_std", F.stddev_pop("runtimeMinutes").over(stats))
        .withColumn("genre_movies", F.count(F.lit(1)).over(stats))
        .withColumn("z_score", (F.col("runtimeMinutes") - F.col("genre_mean")) / F.col("genre_std"))
    )
    outliers = scored.filter(F.abs("z_score") >= z_threshold).withColumn(
        "direction", F.when(F.col("z_score") > 0, "longer").otherwise("shorter")
    ).cache()

    per_genre = (
        scored.groupBy("genre").agg(
            F.first("genre_movies").alias("movies_in_genre"),
            F.round(F.first("genre_mean"), 1).alias("mean_runtime"),
            F.round(F.first("genre_std"), 1).alias("std_runtime"),
            F.sum((F.col("z_score") >= z_threshold).cast("int")).alias("longer_outliers"),
            F.sum((F.col("z_score") <= -z_threshold).cast("int")).alias("shorter_outliers"),
        )
        .withColumn("pct_outliers", F.round(
            100 * (F.col("longer_outliers") + F.col("shorter_outliers")) / F.col("movies_in_genre"), 2))
        .orderBy("genre").toPandas()
    )
    extreme = (
        outliers.orderBy(F.desc(F.abs("z_score")))
        .select("genre", "tconst", "primaryTitle", "startYear", "runtimeMinutes",
                F.round("genre_mean", 1).alias("genre_mean"), F.round("z_score", 2).alias("z_score"))
        .limit(25).toPandas()
    )
    full = outliers.select("genre", "tconst", "primaryTitle", "startYear", "runtimeMinutes",
                           F.round("genre_mean", 1).alias("genre_mean"),
                           F.round("z_score", 2).alias("z_score"), "direction") \
        .orderBy("genre", F.desc(F.abs("z_score")))
    save("q1_runtime_outliers", [
        ("Outliers per genre", per_genre),
        ("25 most extreme outliers", extreme),
    ], full, intro=f"Feature film = movie, runtime >= 40 min. Outlier = |z-score| >= {z_threshold} "
                   "within the genre (z = (runtime - genre mean) / genre standard deviation).")


# --------------------------------------------------------------------------- #
def _genre_year(spark):
    """Popularity of each (year, genre): total votes of its feature films."""
    movies = feature_movies(spark).filter(
        F.col("startYear").between(YEAR_MIN, YEAR_MAX) & F.col("numVotes").isNotNull())
    return (
        movies.select("tconst", "startYear", "averageRating", "numVotes",
                      F.explode("genres").alias("genre"))
        .groupBy("startYear", "genre").agg(
            F.sum("numVotes").alias("total_votes"),
            F.count(F.lit(1)).alias("movies"),
            (F.sum(F.col("averageRating") * F.col("numVotes")) / F.sum("numVotes"))
            .alias("weighted_rating"),
        )
        .withColumn("rank", F.row_number().over(
            Window.partitionBy("startYear").orderBy(F.desc("total_votes"), "genre")))
    )


def q2_top_genres(spark):
    """Top 3 most popular movie genres per year (1980-2025)."""
    top3 = (
        _genre_year(spark).filter("rank <= 3")
        .select(F.col("startYear").alias("year"), "rank", "genre", "total_votes", "movies",
                F.round("weighted_rating", 2).alias("weighted_rating"))
        .orderBy("year", "rank")
    ).cache()
    wide = (
        top3.groupBy("year").pivot("rank", [1, 2, 3]).agg(F.first("genre"))
        .withColumnRenamed("1", "1st").withColumnRenamed("2", "2nd").withColumnRenamed("3", "3rd")
        .orderBy("year").toPandas()
    )
    frequency = (
        top3.groupBy("genre").agg(
            F.count(F.lit(1)).alias("years_in_top3"),
            F.sum((F.col("rank") == 1).cast("int")).alias("years_ranked_1st"))
        .orderBy(F.desc("years_in_top3")).toPandas()
    )
    save("q2_top_genres", [
        ("Top 3 genres per year", wide),
        ("How often each genre is in the top 3", frequency),
    ], top3, intro="Popularity of a genre in a year = total number of IMDb votes of the "
                   "feature films of that genre released that year (a film counts for each "
                   "of its genres).")


def q3_top3_vs_rest(spark):
    """How the top 3 genres compare with the other genres each year."""
    gy = _genre_year(spark).withColumn("group", F.when(F.col("rank") <= 3, "top3").otherwise("rest"))
    per_year = (
        gy.groupBy("startYear").agg(
            F.sum("total_votes").alias("all_votes"),
            F.sum(F.when(F.col("group") == "top3", F.col("total_votes"))).alias("top3_votes"),
            F.countDistinct(F.when(F.col("group") == "rest", F.col("genre"))).alias("other_genres"),
            F.sum(F.when(F.col("group") == "top3", F.col("movies"))).alias("top3_movies"),
            F.sum(F.when(F.col("group") == "rest", F.col("movies"))).alias("rest_movies"),
            (F.sum(F.when(F.col("group") == "top3", F.col("weighted_rating") * F.col("total_votes")))
             / F.sum(F.when(F.col("group") == "top3", F.col("total_votes")))).alias("top3_rating"),
            (F.sum(F.when(F.col("group") == "rest", F.col("weighted_rating") * F.col("total_votes")))
             / F.sum(F.when(F.col("group") == "rest", F.col("total_votes")))).alias("rest_rating"),
            F.avg(F.when(F.col("group") == "rest", F.col("total_votes"))).alias("avg_votes_other_genre"),
            F.avg(F.when(F.col("group") == "top3", F.col("total_votes"))).alias("avg_votes_top3_genre"),
        )
        .withColumn("top3_vote_share_pct", F.round(100 * F.col("top3_votes") / F.col("all_votes"), 1))
        .withColumn("popularity_ratio", F.round(
            F.col("avg_votes_top3_genre") / F.col("avg_votes_other_genre"), 1))
        .select(F.col("startYear").alias("year"), "top3_vote_share_pct", "popularity_ratio",
                "top3_movies", "rest_movies", "other_genres",
                F.round("top3_rating", 2).alias("top3_rating"),
                F.round("rest_rating", 2).alias("rest_rating"))
        .orderBy("year")
    ).cache()
    by_decade = (
        per_year.withColumn("decade", (F.floor(F.col("year") / 10) * 10).cast("int"))
        .groupBy("decade").agg(
            F.round(F.avg("top3_vote_share_pct"), 1).alias("avg_top3_vote_share_pct"),
            F.round(F.avg("popularity_ratio"), 1).alias("avg_popularity_ratio"),
            F.round(F.avg("top3_rating"), 2).alias("avg_top3_rating"),
            F.round(F.avg("rest_rating"), 2).alias("avg_rest_rating"))
        .orderBy("decade").toPandas()
    )
    save("q3_top3_vs_rest", [
        ("Summary by decade", by_decade),
        ("Per year", per_year.toPandas()),
    ], intro="top3_vote_share_pct = share of all votes going to the top 3 genres; "
             "popularity_ratio = average votes of a top-3 genre / average votes of another "
             "genre; ratings are vote-weighted averages.")


# --------------------------------------------------------------------------- #
def _director_movies(spark, min_votes):
    crew = table(spark, "title.crew").select("tconst", F.explode("directors").alias("nconst"))
    movies = rated(feature_movies(spark), min_votes)
    return crew.join(movies.select("tconst", "primaryTitle", "startYear", "averageRating",
                                   "numVotes"), "tconst")


def q4_one_hit_directors(spark, hit: float = 8.0, low: float = 6.0, min_votes: int = MIN_VOTES):
    """Directors with exactly one movie rated >= 8.0 and >= 2 other movies averaging <= 6.0."""
    dm = _director_movies(spark, min_votes).withColumn("is_hit", F.col("averageRating") >= hit)
    per_director = dm.groupBy("nconst").agg(
        F.sum(F.col("is_hit").cast("int")).alias("hits"),
        F.sum((~F.col("is_hit")).cast("int")).alias("other_movies"),
        F.avg(F.when(~F.col("is_hit"), F.col("averageRating"))).alias("other_avg_rating"),
        F.max(F.when(F.col("is_hit"), F.struct("averageRating", "primaryTitle", "startYear")))
        .alias("hit"),
    )
    result = (
        per_director.filter((F.col("hits") == 1) & (F.col("other_movies") >= 2)
                            & (F.col("other_avg_rating") <= low))
        .join(table(spark, "name.basics").select("nconst", "primaryName"), "nconst", "left")
        .select("nconst", "primaryName", F.col("hit.primaryTitle").alias("hit_movie"),
                F.col("hit.startYear").alias("hit_year"),
                F.col("hit.averageRating").alias("hit_rating"), "other_movies",
                F.round("other_avg_rating", 2).alias("other_avg_rating"))
        .orderBy(F.desc("hit_rating"), "other_avg_rating")
    ).cache()
    n = result.count()
    save("q4_one_hit_directors", [
        ("Number of directors", f"{n:,} directors match."),
        ("Directors (first 50)", result.limit(50).toPandas()),
    ], result, intro=f"Feature films with >= {min_votes} votes. Exactly one film rated >= {hit}, "
                     f"and at least 2 other films whose average rating is <= {low}.")


# --------------------------------------------------------------------------- #
def q5_declining_series(spark, min_seasons: int = 3, min_episodes: int = 10,
                        min_episode_votes: int = 10):
    """Series whose worst episode is in the last season and best one in seasons 1-2."""
    series = table(spark, "title.basics").filter(
        F.col("titleType").isin("tvSeries", "tvMiniSeries")
    ).select(F.col("tconst").alias("parentTconst"), F.col("primaryTitle").alias("series"),
             F.col("startYear").alias("series_start"))
    episodes = (
        table(spark, "title.episode")
        .filter(F.col("seasonNumber") >= 1)
        .join(series, "parentTconst")
        .join(table(spark, "title.ratings"), "tconst", "left")
    )
    seasons = episodes.groupBy("parentTconst", "seasonNumber").agg(
        F.count(F.lit(1)).alias("episodes"))
    eligible = (
        seasons.groupBy("parentTconst").agg(
            F.sum((F.col("episodes") >= min_episodes).cast("int")).alias("big_seasons"),
            F.max("seasonNumber").alias("last_season"))
        .filter(F.col("big_seasons") >= min_seasons)
    )
    rated_eps = (
        episodes.join(eligible, "parentTconst")
        .filter(F.col("averageRating").isNotNull() & (F.col("numVotes") >= min_episode_votes))
    )
    by_series = Window.partitionBy("parentTconst")
    ranked = (
        rated_eps
        .withColumn("worst_rank", F.row_number().over(
            by_series.orderBy("averageRating", F.desc("numVotes"), "tconst")))
        .withColumn("best_rank", F.row_number().over(
            by_series.orderBy(F.desc("averageRating"), F.desc("numVotes"), "tconst")))
    )
    worst = ranked.filter("worst_rank = 1").select(
        "parentTconst", F.col("seasonNumber").alias("worst_season"),
        F.col("episodeNumber").alias("worst_episode"), F.col("averageRating").alias("worst_rating"))
    best = ranked.filter("best_rank = 1").select(
        "parentTconst", F.col("seasonNumber").alias("best_season"),
        F.col("episodeNumber").alias("best_episode"), F.col("averageRating").alias("best_rating"))
    result = (
        eligible.join(worst, "parentTconst").join(best, "parentTconst")
        .join(series, "parentTconst")
        .filter((F.col("worst_season") == F.col("last_season")) & (F.col("best_season") <= 2))
        .select("parentTconst", "series", "series_start", "last_season", "big_seasons",
                "best_season", "best_episode", "best_rating",
                "worst_season", "worst_episode", "worst_rating")
        .orderBy(F.desc(F.col("best_rating") - F.col("worst_rating")))
    ).cache()
    n_eligible = eligible.count()
    n = result.count()
    save("q5_declining_series", [
        ("Counts", f"{n_eligible:,} series have >= {min_seasons} seasons of >= {min_episodes} "
                   f"episodes; {n:,} of them match."),
        ("Matching series (largest best-worst gap first, first 50)", result.limit(50).toPandas()),
    ], result, intro=f"Only episodes with a season number >= 1 and >= {min_episode_votes} votes "
                     "are ranked. Last season = highest season number of the series.")


# --------------------------------------------------------------------------- #
def _original_language(spark):
    """Original language of each title from title.akas.

    1. language of an aka whose title equals the original title
    2. otherwise the main language of the region of such an aka
    """
    basics = table(spark, "title.basics").select("tconst", "originalTitle")
    akas = table(spark, "title.akas").select(
        F.col("titleId").alias("tconst"), "title", "region", "language")
    region_map = F.create_map(*[F.lit(x) for kv in REGION_LANGUAGE.items() for x in kv])
    same = (
        akas.join(basics, "tconst")
        .filter(F.lower(F.col("title")) == F.lower(F.col("originalTitle")))
        .withColumn("lang", F.coalesce(F.col("language"), region_map[F.col("region")]))
        .filter(F.col("lang").isNotNull())
    )
    counts = same.groupBy("tconst", "lang").agg(
        F.count(F.lit(1)).alias("n"),
        F.max(F.col("language").isNotNull().cast("int")).alias("explicit"))
    pick = Window.partitionBy("tconst").orderBy(F.desc("explicit"), F.desc("n"), "lang")
    return counts.withColumn("r", F.row_number().over(pick)).filter("r = 1") \
        .select("tconst", F.col("lang").alias("language"))


def q6_multilingual_actors(spark, min_rating: float = 7.0, success_votes: int = 10000):
    """Actors/actresses with successful films in several original languages."""
    hits = (
        feature_movies(spark)
        .filter((F.col("averageRating") >= min_rating) & (F.col("numVotes") >= success_votes))
        .join(_original_language(spark), "tconst")
        .select("tconst", "primaryTitle", "language")
    )
    cast = table(spark, "title.principals").filter(F.col("category").isin(*ACTING)) \
        .select("tconst", "nconst").distinct()
    per_actor = (
        cast.join(hits, "tconst")
        .groupBy("nconst").agg(
            F.countDistinct("language").alias("languages"),
            F.sort_array(F.collect_set("language")).alias("language_list"),
            F.countDistinct("tconst").alias("successful_films"))
        .filter("languages >= 2")
        .join(table(spark, "name.basics").select("nconst", "primaryName"), "nconst", "left")
        .select("nconst", "primaryName", "languages",
                F.concat_ws(", ", "language_list").alias("languages_list"), "successful_films")
        .orderBy(F.desc("languages"), F.desc("successful_films"))
    ).cache()
    distribution = per_actor.groupBy("languages").count().orderBy("languages").toPandas()
    three_plus = per_actor.filter("languages >= 3")
    coverage = hits.groupBy("language").count().orderBy(F.desc("count")).limit(15).toPandas()
    save("q6_multilingual_actors", [
        ("Actors per number of languages", distribution),
        ("Actors with successful films in >= 3 languages", three_plus.toPandas()),
        ("Actors with successful films in 2 languages (first 50)",
         per_actor.filter("languages = 2").limit(50).toPandas()),
        ("Successful films per original language (top 15)", coverage),
    ], per_actor, intro=f"Successful film = feature film rated >= {min_rating} with >= "
                        f"{success_votes} votes. Original language from title.akas (see REPORT.md).")


# --------------------------------------------------------------------------- #
def q7_early_peak_actors(spark, min_years: int = 20, min_movies: int = 15,
                         min_votes: int = MIN_VOTES, early_share: float = 1 / 3):
    """Long careers (20+ years, 15+ films) whose best-rated films came early."""
    cast = table(spark, "title.principals").filter(F.col("category").isin(*ACTING)) \
        .select("tconst", "nconst").distinct()
    films = cast.join(rated(feature_movies(spark), min_votes)
                      .select("tconst", "primaryTitle", "startYear", "averageRating", "numVotes"),
                      "tconst").filter(F.col("startYear").isNotNull())
    by_actor = Window.partitionBy("nconst")
    films = (
        films.withColumn("first_year", F.min("startYear").over(by_actor))
        .withColumn("last_year", F.max("startYear").over(by_actor))
        .withColumn("n_movies", F.count(F.lit(1)).over(by_actor))
        .filter(((F.col("last_year") - F.col("first_year")) >= min_years)
                & (F.col("n_movies") >= min_movies))
        .withColumn("career_pos", (F.col("startYear") - F.col("first_year"))
                    / (F.col("last_year") - F.col("first_year")))
        .withColumn("best", F.row_number().over(
            by_actor.orderBy(F.desc("averageRating"), F.desc("numVotes"))))
    )
    summary = films.groupBy("nconst").agg(
        F.first("first_year").alias("first_year"), F.first("last_year").alias("last_year"),
        F.first("n_movies").alias("movies"),
        F.max(F.when(F.col("best") == 1, F.struct("averageRating", "primaryTitle", "startYear",
                                                  "career_pos"))).alias("peak"),
        F.avg(F.when(F.col("career_pos") <= 0.5, F.col("averageRating"))).alias("first_half_avg"),
        F.avg(F.when(F.col("career_pos") > 0.5, F.col("averageRating"))).alias("second_half_avg"),
    )
    eligible = summary.count()
    result = (
        summary.filter((F.col("peak.career_pos") <= early_share)
                       & (F.col("first_half_avg") > F.col("second_half_avg")))
        .join(table(spark, "name.basics").select("nconst", "primaryName"), "nconst", "left")
        .select("nconst", "primaryName", "first_year", "last_year", "movies",
                F.col("peak.primaryTitle").alias("best_film"),
                F.col("peak.startYear").alias("best_year"),
                F.col("peak.averageRating").alias("best_rating"),
                F.round("first_half_avg", 2).alias("first_half_avg"),
                F.round("second_half_avg", 2).alias("second_half_avg"),
                F.round(F.col("first_half_avg") - F.col("second_half_avg"), 2).alias("decline"))
        .orderBy(F.desc("decline"))
    ).cache()
    n = result.count()
    save("q7_early_peak_actors", [
        ("Counts", f"{eligible:,} actors/actresses have >= {min_years} years and >= {min_movies} "
                   f"rated feature films; {n:,} peaked early."),
        ("Early-peak actors (largest decline first, first 50)", result.limit(50).toPandas()),
    ], result, intro=f"Feature films with >= {min_votes} votes. Peaked early = best-rated film "
                     f"in the first {early_share:.0%} of the career AND average rating of the "
                     "first half of the career higher than the second half.")


# --------------------------------------------------------------------------- #
def q8_multi_talented(spark, min_votes: int = MIN_VOTES):
    """People who direct, write and/or act, and how they do when combining roles."""
    crew = table(spark, "title.crew")
    roles = (
        crew.select("tconst", F.explode("directors").alias("nconst"), F.lit("director").alias("role"))
        .unionByName(crew.select("tconst", F.explode("writers").alias("nconst"),
                                 F.lit("writer").alias("role")))
        .unionByName(table(spark, "title.principals").filter(F.col("category").isin(*ACTING))
                     .select("tconst", "nconst", F.lit("actor").alias("role")))
        .distinct()
    )
    films = rated(feature_movies(spark), min_votes).select("tconst", "averageRating", "numVotes")
    person_film = (
        roles.join(films, "tconst")
        .groupBy("nconst", "tconst", "averageRating", "numVotes")
        .agg(F.sort_array(F.collect_set("role")).alias("roles"))
        .withColumn("n_roles", F.size("roles"))
    ).cache()
    people = (
        person_film.groupBy("nconst").agg(
            F.sort_array(F.array_distinct(F.flatten(F.collect_list("roles")))).alias("all_roles"),
            F.sum((F.col("n_roles") >= 2).cast("int")).alias("multi_role_films"),
            F.sum((F.col("n_roles") == 1).cast("int")).alias("single_role_films"),
            F.avg(F.when(F.col("n_roles") >= 2, F.col("averageRating"))).alias("multi_avg"),
            F.avg(F.when(F.col("n_roles") == 1, F.col("averageRating"))).alias("single_avg"))
        .filter(F.size("all_roles") >= 2)
        .withColumn("profile", F.concat_ws(" + ", "all_roles"))
    ).cache()

    profiles = people.groupBy("profile").agg(
        F.count(F.lit(1)).alias("people"),
        F.sum((F.col("multi_role_films") > 0).cast("int")).alias("combined_roles_on_same_film"),
    ).orderBy(F.desc("people")).toPandas()

    paired = people.filter((F.col("multi_role_films") > 0) & (F.col("single_role_films") > 0)) \
        .withColumn("diff", F.col("multi_avg") - F.col("single_avg"))
    comparison = paired.agg(
        F.count(F.lit(1)).alias("people_with_both"),
        F.round(F.avg("single_avg"), 3).alias("mean_rating_single_role"),
        F.round(F.avg("multi_avg"), 3).alias("mean_rating_multiple_roles"),
        F.round(F.avg("diff"), 3).alias("mean_difference"),
        F.round(100 * F.avg((F.col("diff") > 0).cast("int")), 1).alias("pct_better_when_combining"),
    ).toPandas()
    combos = (
        person_film.filter("n_roles >= 2").withColumn("combo", F.concat_ws(" + ", "roles"))
        .groupBy("combo").agg(F.count(F.lit(1)).alias("person_films"),
                              F.round(F.avg("averageRating"), 2).alias("avg_rating"),
                              F.round(F.avg("numVotes"), 0).alias("avg_votes"))
        .orderBy(F.desc("person_films")).toPandas()
    )
    single = person_film.filter("n_roles = 1").agg(
        F.lit("single role").alias("combo"), F.count(F.lit(1)).alias("person_films"),
        F.round(F.avg("averageRating"), 2).alias("avg_rating"),
        F.round(F.avg("numVotes"), 0).alias("avg_votes")).toPandas()
    top = (
        paired.filter("multi_role_films >= 3 AND single_role_films >= 3")
        .join(table(spark, "name.basics").select("nconst", "primaryName"), "nconst", "left")
        .select("nconst", "primaryName", "profile", "multi_role_films", "single_role_films",
                F.round("multi_avg", 2).alias("multi_avg"),
                F.round("single_avg", 2).alias("single_avg"), F.round("diff", 2).alias("diff"))
        .orderBy(F.desc("diff")).limit(25).toPandas()
    )
    save("q8_multi_talented", [
        ("Multi-talented people by profile", profiles),
        ("Same person: films with several roles vs films with one role", comparison),
        ("Average rating per role combination on the same film", combos),
        ("Baseline: person-film pairs with a single role", single),
        ("Largest gains when combining roles (>= 3 films of each kind)", top),
    ], intro=f"Roles: director / writer (title.crew), actor (title.principals actor/actress). "
             f"Feature films with >= {min_votes} votes. Multi-talented = at least 2 different "
             "roles over the career. The comparison is paired: each person is compared with "
             "themselves.")


ALL = {
    1: q1_runtime_outliers, 2: q2_top_genres, 3: q3_top3_vs_rest, 4: q4_one_hit_directors,
    5: q5_declining_series, 6: q6_multilingual_actors, 7: q7_early_peak_actors,
    8: q8_multi_talented,
}
