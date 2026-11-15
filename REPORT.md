# IMDb Big Data Processing - Report

## 1. Data preparation

### 1.1 Missing values per column

Source: `outputs/null_report.md`. Missing = IMDb null marker `\N` + empty string + absent field (row too short). `Invalid numeric` counts non-numeric text in numeric columns (— for text columns).

| Table | Column | Total rows | Missing | % missing | `\N` | Empty | Absent | Invalid numeric |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| name.basics | nconst | 15,816,005 | 0 | 0.000 | 0 | 0 | 0 | — |
| name.basics | primaryName | 15,816,005 | 293 | 0.002 | 293 | 0 | 0 | — |
| name.basics | birthYear | 15,816,005 | 15,130,128 | 95.663 | 15,130,128 | 0 | 0 | 0 |
| name.basics | deathYear | 15,816,005 | 15,550,475 | 98.321 | 15,550,475 | 0 | 0 | 0 |
| name.basics | primaryProfession | 15,816,005 | 3,220,639 | 20.363 | 3,220,639 | 0 | 0 | — |
| name.basics | knownForTitles | 15,816,005 | 1,917,578 | 12.124 | 1,917,578 | 0 | 0 | — |
| title.akas | titleId | 60,424,702 | 0 | 0.000 | 0 | 0 | 0 | — |
| title.akas | ordering | 60,424,702 | 0 | 0.000 | 0 | 0 | 0 | 0 |
| title.akas | title | 60,424,702 | 0 | 0.000 | 0 | 0 | 0 | — |
| title.akas | region | 60,424,702 | 13,002,884 | 21.519 | 13,002,884 | 0 | 0 | — |
| title.akas | language | 60,424,702 | 19,665,115 | 32.545 | 19,665,115 | 0 | 0 | — |
| title.akas | types | 60,424,702 | 40,393,110 | 66.849 | 40,393,110 | 0 | 0 | — |
| title.akas | attributes | 60,424,702 | 60,107,437 | 99.475 | 60,107,437 | 0 | 0 | — |
| title.akas | isOriginalTitle | 60,424,702 | 0 | 0.000 | 0 | 0 | 0 | 0 |
| title.basics | tconst | 12,963,111 | 0 | 0.000 | 0 | 0 | 0 | — |
| title.basics | titleType | 12,963,111 | 0 | 0.000 | 0 | 0 | 0 | — |
| title.basics | primaryTitle | 12,963,111 | 0 | 0.000 | 0 | 0 | 0 | — |
| title.basics | originalTitle | 12,963,111 | 0 | 0.000 | 0 | 0 | 0 | — |
| title.basics | isAdult | 12,963,111 | 0 | 0.000 | 0 | 0 | 0 | 0 |
| title.basics | startYear | 12,963,111 | 1,487,320 | 11.473 | 1,487,320 | 0 | 0 | 0 |
| title.basics | endYear | 12,963,111 | 12,798,642 | 98.731 | 12,798,642 | 0 | 0 | 0 |
| title.basics | runtimeMinutes | 12,963,111 | 8,050,574 | 62.104 | 8,050,574 | 0 | 0 | 0 |
| title.basics | genres | 12,963,111 | 548,144 | 4.228 | 548,144 | 0 | 0 | — |
| title.crew | tconst | 12,960,686 | 0 | 0.000 | 0 | 0 | 0 | — |
| title.crew | directors | 12,960,686 | 5,791,405 | 44.684 | 5,791,405 | 0 | 0 | — |
| title.crew | writers | 12,960,686 | 6,432,432 | 49.630 | 6,432,432 | 0 | 0 | — |
| title.episode | tconst | 10,034,862 | 0 | 0.000 | 0 | 0 | 0 | — |
| title.episode | parentTconst | 10,034,862 | 0 | 0.000 | 0 | 0 | 0 | — |
| title.episode | seasonNumber | 10,034,862 | 2,126,237 | 21.189 | 2,126,237 | 0 | 0 | 0 |
| title.episode | episodeNumber | 10,034,862 | 2,126,237 | 21.189 | 2,126,237 | 0 | 0 | 0 |
| title.principals | tconst | 102,756,358 | 0 | 0.000 | 0 | 0 | 0 | — |
| title.principals | ordering | 102,756,358 | 0 | 0.000 | 0 | 0 | 0 | 0 |
| title.principals | nconst | 102,756,358 | 0 | 0.000 | 0 | 0 | 0 | — |
| title.principals | category | 102,756,358 | 0 | 0.000 | 0 | 0 | 0 | — |
| title.principals | job | 102,756,358 | 82,882,023 | 80.659 | 82,882,023 | 0 | 0 | — |
| title.principals | characters | 102,756,358 | 52,691,057 | 51.278 | 52,691,057 | 0 | 0 | — |
| title.ratings | tconst | 1,732,169 | 0 | 0.000 | 0 | 0 | 0 | — |
| title.ratings | averageRating | 1,732,169 | 0 | 0.000 | 0 | 0 | 0 | 0 |
| title.ratings | numVotes | 1,732,169 | 0 | 0.000 | 0 | 0 | 0 | 0 |

All missing values use the IMDb marker `\N`: no empty string, no truncated row and no invalid number was found. Identifier columns are always filled; the most incomplete columns are optional attributes (`attributes`, `types`, `job`, `characters`) and dates that only apply to some records (`endYear` for series, `deathYear` for people who died).

### 1.2 Top 5 columns with the most missing values

Source: `outputs/null_report.md` (missing = `\N`, empty string or absent field).

| Table | Column | Total rows | Missing values | % missing |
| --- | --- | ---: | ---: | ---: |
| title.akas | attributes | 60,424,702 | 60,107,437 | 99.48 |
| title.basics | endYear | 12,963,111 | 12,798,642 | 98.73 |
| name.basics | deathYear | 15,816,005 | 15,550,475 | 98.32 |
| name.basics | birthYear | 15,816,005 | 15,130,128 | 95.66 |
| title.principals | job | 102,756,358 | 82,882,023 | 80.66 |

### 1.3 Other data quality issues

Source: `outputs/quality_report.md`. Handling: **quarantine** = row moved to `data/quarantine/`, **nullify** = faulty value set to NULL, **flag** = row kept with a `dq_*` column.

| Table | Rule | Description | Rows affected | % rows | Handling |
| --- | --- | --- | ---: | ---: | --- |
| title.basics | `dq_duplicate_key` | Duplicate primary key (tconst) | 0 | 0.0000 | quarantine |
| title.basics | `dq_missing_title` | No primary title | 0 | 0.0000 | flag |
| title.basics | `dq_invalid_is_adult` | isAdult is not 0 or 1 | 0 | 0.0000 | nullify isAdult |
| title.basics | `dq_end_before_start` | endYear earlier than startYear | 0 | 0.0000 | nullify endYear |
| title.basics | `dq_end_year_not_series` | endYear set on a title that is not a series | 0 | 0.0000 | nullify endYear |
| title.basics | `dq_start_in_future` | startYear in the future (announced / in production) | 6,238 | 0.0481 | flag |
| title.basics | `dq_start_too_old` | startYear before the first motion pictures (1870) | 0 | 0.0000 | flag |
| title.basics | `dq_runtime_not_positive` | runtimeMinutes <= 0 | 4 | 0.0000 | nullify runtimeMinutes |
| title.basics | `dq_runtime_extreme` | runtimeMinutes above 1440 (24 h) | 210 | 0.0016 | flag |
| title.basics | `dq_missing_genres` | No genre | 548,144 | 4.2285 | flag |
| name.basics | `dq_duplicate_key` | Duplicate primary key (nconst) | 0 | 0.0000 | quarantine |
| name.basics | `dq_missing_name` | No name | 293 | 0.0019 | flag |
| name.basics | `dq_death_before_birth` | deathYear earlier than birthYear | 28 | 0.0002 | nullify birthYear, deathYear |
| name.basics | `dq_birth_in_future` | birthYear in the future | 0 | 0.0000 | nullify birthYear |
| name.basics | `dq_death_in_future` | deathYear in the future | 0 | 0.0000 | nullify deathYear |
| name.basics | `dq_lifespan_too_long` | Lifespan above 120 years | 2 | 0.0000 | flag |
| name.basics | `dq_presumed_dead_no_death_year` | No deathYear but born more than 120 years ago | 2,794 | 0.0177 | flag |
| name.basics | `dq_known_for_orphans` | knownForTitles referenced titles missing from title.basics (ids removed) | 67 | 0.0004 | flag |
| title.ratings | `dq_duplicate_key` | Duplicate primary key (tconst) | 0 | 0.0000 | quarantine |
| title.ratings | `dq_orphan_title` | tconst missing from title.basics | 0 | 0.0000 | quarantine |
| title.ratings | `dq_rating_out_of_range` | averageRating outside [1, 10] | 0 | 0.0000 | quarantine |
| title.ratings | `dq_votes_not_positive` | numVotes <= 0 | 0 | 0.0000 | quarantine |
| title.episode | `dq_duplicate_key` | Duplicate primary key (tconst) | 0 | 0.0000 | quarantine |
| title.episode | `dq_orphan_episode` | Episode tconst missing from title.basics | 0 | 0.0000 | quarantine |
| title.episode | `dq_orphan_parent` | parentTconst missing from title.basics | 5 | 0.0000 | quarantine |
| title.episode | `dq_self_parent` | Episode is its own parent | 0 | 0.0000 | quarantine |
| title.episode | `dq_negative_numbers` | Negative season or episode number | 0 | 0.0000 | nullify seasonNumber, episodeNumber |
| title.episode | `dq_missing_season` | No seasonNumber | 2,126,237 | 21.1885 | flag |
| title.crew | `dq_duplicate_key` | Duplicate primary key (tconst) | 0 | 0.0000 | quarantine |
| title.crew | `dq_orphan_title` | tconst missing from title.basics | 10 | 0.0001 | quarantine |
| title.crew | `dq_director_orphans` | directors referenced people missing from name.basics (ids removed) | 405 | 0.0031 | flag |
| title.crew | `dq_writer_orphans` | writers referenced people missing from name.basics (ids removed) | 785 | 0.0061 | flag |
| title.principals | `dq_duplicate_key` | Duplicate primary key (tconst, ordering) | 0 | 0.0000 | quarantine |
| title.principals | `dq_orphan_title` | tconst missing from title.basics | 0 | 0.0000 | quarantine |
| title.principals | `dq_orphan_person` | nconst missing from name.basics | 7,262 | 0.0071 | quarantine |
| title.principals | `dq_born_after_release` | Principal born after the title was released | 3,109 | 0.0030 | flag |
| title.principals | `dq_died_before_release` | Principal died before the release (not archive footage) | 287,065 | 0.2794 | flag |
| title.akas | `dq_duplicate_key` | Duplicate primary key (titleId, ordering) | 0 | 0.0000 | quarantine |
| title.akas | `dq_orphan_title` | titleId missing from title.basics | 8 | 0.0000 | quarantine |
| title.akas | `dq_invalid_is_original` | isOriginalTitle is not 0 or 1 | 0 | 0.0000 | nullify isOriginalTitle |

Main findings: no duplicate keys and no rating outside [1, 10]; 7,262 credits in `title.principals` point to people missing from `name.basics` (quarantined); 28 people died before they were born (both years nullified); 287,065 credits of people who died before the release (outside archive footage) are flagged, most of them being posthumous releases; 548,144 titles have no genre.

### 1.4 Methodological explanation

#### Methodology and why it was chosen

We first converted the seven IMDb files into Parquet format, keeping all columns as text in the bronze layer. We chose this approach to avoid losing information during conversion. For example, if `startYear` contains an invalid value like `abc`, converting it directly into a number would turn it into NULL, making it harder to identify and count the original error.

To measure missing values, we checked each column for three cases: `\N` (IMDb's way of indicating unknown values), empty strings, and missing fields caused by incomplete rows. We also counted invalid text in numeric columns separately. These checks were performed in a single Spark pass per table. The percentage of missing values was calculated by dividing the total number of unknown, empty, and absent values by the number of rows. We kept these categories separate because they can have different causes.

We also looked for other data quality issues, such as people who died before they were born, people born after a movie's release, missing foreign key references, duplicate records, invalid ratings outside the 1–10 range, unrealistic movie durations, inconsistent start and end years, and episodes referencing themselves as their parent series.

We handled these issues in three different ways depending on their severity. First, unusable records, such as orphan records, duplicates, or impossible ratings, were moved to a separate quarantine folder instead of being permanently deleted. Malformed rows with missing fields were also excluded from the silver tables. However, for columns containing lists of identifiers, such as `knownForTitles`, `directors`, and `writers`, we only removed the invalid references from the lists instead of quarantining the entire record.

Second, when only a specific value was incorrect, we replaced it with NULL while keeping the rest of the record. For example, if a person's death year was earlier than their birth year, we set both dates to NULL because we could not know which one was incorrect.

Finally, we added `dq_*` flags for unusual values that might still be valid, such as future movie releases, very long experimental films, or posthumous releases. This allows us to keep the data while letting later analyses decide whether to include these records.

The final cleaned datasets form the silver layer, which is used for the following analyses.

#### Alternatives considered and trade-offs

We considered removing every record containing a data quality issue, but this would result in losing too much useful information. For example, an incorrect birth or death year should not cause us to lose a person's entire filmography.

Another option was replacing missing values with averages or estimated values. However, we decided not to do this because it would introduce information that was not present in the original IMDb dataset.

We also considered letting Spark automatically convert data types when reading the files. Although this would be simpler, invalid values could become NULL without being clearly identified, making data quality problems harder to detect.

The main disadvantage of our approach is that it requires more storage and processing. We need additional columns for the `dq_*` flags and separate tables for quarantined records. Also, checking foreign key relationships requires joins between large datasets, which can increase processing time. Finally, later analyses need to take the quality flags into account to avoid including potentially problematic values.

## 2. Data exploration

All the queries are in `src/explore/queries.py` and run on the silver tables (`python -m src.explore.run`). Results are in `outputs/exploration/` (one `.md` file per question, plus the full result as CSV).

For all questions, a "feature film" is a movie of at least 40 minutes. When we look at success or careers, we only keep films with at least 1,000 votes, because a rating given by 10 people does not mean much.

### 2.1 Runtime outliers per genre (q1)

We computed the mean and standard deviation of the runtime for each genre, and we called a film an outlier when it is more than 3 standard deviations away from its genre mean.

| Genre | Films | Mean (min) | Std (min) | Outliers | % |
| --- | ---: | ---: | ---: | ---: | ---: |
| Drama | 196,243 | 96.0 | 27.4 | 1,388 | 0.71 |
| Documentary | 116,275 | 76.5 | 30.2 | 1,079 | 0.93 |
| Comedy | 95,804 | 93.2 | 22.4 | 802 | 0.84 |
| Action | 40,412 | 101.2 | 31.3 | 325 | 0.80 |
| Western | 6,354 | 76.5 | 28.7 | 101 | 1.59 |

Around 1 % of each genre are outliers, and they are all longer than normal. There are no "too short" outliers because we already removed films under 40 minutes. The extreme ones are 24-hour films like *The Clock* (2010).

### 2.2 Top 3 genres per year (q2)

We measured the popularity of a genre in a year with the total number of votes of its films.

| Genre | Years in top 3 (1980-2025) | Years 1st |
| --- | ---: | ---: |
| Drama | 46 | 42 |
| Comedy | 34 | 2 |
| Action | 29 | 1 |
| Adventure | 16 | 1 |
| Crime | 9 | 0 |

Drama is almost always first. Comedy was usually second until the 2000s, and then Action took its place from 2008 (probably because of the big franchise and superhero movies). Crime was in the top 3 mostly in the 90s.

### 2.3 Top 3 genres vs the other genres (q3)

| Decade | % of votes for the top 3 | Votes top-3 genre / other genre | Rating top 3 | Rating others |
| --- | ---: | ---: | ---: | ---: |
| 1980s | 46.0 | 5.2 | 7.33 | 7.08 |
| 1990s | 47.2 | 5.5 | 7.37 | 7.16 |
| 2000s | 42.0 | 4.7 | 7.05 | 7.02 |
| 2010s | 44.1 | 5.4 | 6.94 | 6.87 |
| 2020s | 44.1 | 5.6 | 6.62 | 6.52 |

The top 3 genres get almost half of all the votes every year, and a top-3 genre gets about 5 times more votes than another genre. But their ratings are only a bit higher (+0.1 to +0.25). So they are much more watched, but not really better rated.

### 2.4 Directors with only one hit (q4)

We looked for directors with exactly one film rated 8.0 or more, and at least 2 other films with an average of 6.0 or less. We found **47 directors**, for example Irvin Kershner (*The Empire Strikes Back*, 8.7, his other films average 5.94), M. Night Shyamalan (*The Sixth Sense*) or Richard Kelly (*Donnie Darko*). Some films appear twice because they have two directors (*Sin City*), and one director appears twice because IMDb has two records for him.

### 2.5 Series that got worse (q5)

We kept series with at least 3 seasons of 10 episodes or more (13,625 series) and only episodes with at least 10 votes. **475 series** have their best episode in an earlier season and their worst episode in the last season. The biggest drops are *One Punch Man* (9.5 → 1.6), *House of Cards* (9.5 → 2.5), *The Flash* (9.6 → 3.8) and *Two and a Half Men* (8.7 → 3.6). These shows are known for a bad last season, so the result makes sense.

### 2.6 Actors successful in several languages (q6)

A successful film here = rated 7.0 or more with at least 10,000 votes. We used `title.akas` to find the original language of each film.

| Languages | Actors |
| ---: | ---: |
| 2 | 1,099 |
| 3 | 180 |
| 4 | 16 |
| 5 | 5 |
| 6 | 2 |

**203 actors** have successful films in 3 languages or more, like Song Kang-ho and Toshirō Mifune (6), Max von Sydow (4) or Robert De Niro and Penélope Cruz (3). This result is not perfect: IMDb has no real "original language" column and the language is missing in a third of `title.akas`, so some languages are wrong (for example Brad Pitt appears with Romanian films).

### 2.7 Actors who peaked early (q7)

We kept actors with at least 20 years of career and 15 films. An actor "peaked early" if their best film is in the first third of their career and their first half is better rated than their second half. **1,557 out of 3,528** actors (44 %) are in this case, for example Wendell Corey (*Rear Window*), Corey Feldman (*Stand by Me*), Edward Furlong (*Terminator 2*) or Shelley Duvall (*The Shining*).

### 2.8 Multi-talented people (q8)

We took directors and writers from `title.crew` and actors from `title.principals`.

| Profile | People | With roles combined on the same film |
| --- | ---: | ---: |
| director + writer | 13,113 | 12,713 |
| actor + writer | 3,452 | 2,068 |
| actor + director + writer | 2,938 | 2,903 |
| actor + director | 754 | 305 |

To see if doing several jobs on the same film helps, we compared each person with themselves (8,698 people). Their films with several roles have an average rating of **6.091**, and their films with one role **6.092**, so basically **no difference**. Some people do much better when they write and direct, like Frank Darabont (8.33 vs 6.36) or Sergio Leone (7.86 vs 6.18), but on average it does not change anything.

### 2.9 Methodological explanation

#### Methodology and why it was chosen

We used the silver tables so that the cleaning from part 1 applies to every question. Everything is written with the PySpark DataFrame API, so the same code works on the full dataset.

We mostly used:
- window functions, to rank genres per year, compute the z-scores per genre and sort the films of an actor's career;
- joins between the IMDb tables;
- `explode` for the columns that contain lists (genres, directors, writers), so a film counts once for each genre or person.

We added vote thresholds (1,000 or 10,000 votes) to avoid films rated by very few people. All the thresholds are written at the top of each result file.

For q8 we compared each person with themselves instead of comparing all the multi-role films with all the other films, because otherwise we would mostly compare different people with different talent.

#### Alternatives considered and trade-offs

- We measured popularity with the number of votes and not with the number of films or the rating. The number of films favours genres with lots of small productions (like documentaries), and the rating favours niche genres.
- For runtimes we used a z-score instead of a fixed limit like 180 minutes, because each genre has a different normal length. The downside is that runtimes are not symmetric, so it is not perfect.
- The vote thresholds make the results more reliable but remove recent or less known films, and the exact values are our choice.
- For q6 we had to approximate the original language with `title.akas`, because IMDb does not give it directly. Using another source like Wikidata would be more accurate.
- The heavy queries (q6, q7, q8) join `title.principals` (102 million rows) with other big tables. On our 8 GB laptop they took 20 to 45 minutes each with 2 Spark cores.

## 3. Data enrichment

### 3.1 Results

Source: `outputs/enrichment_summary.md`. Selection: 7,186 films (top 10 per genre and year, 1980-2025) and 10,934 principals (first 3 per film). No malformed raw line was dropped.

**Films (synopsis)**

| Status | Count | % |
| --- | ---: | ---: |
| ok | 5,981 | 83.23 |
| no_wikidata_item | 491 | 6.83 |
| no_plot_section | 385 | 5.36 |
| no_english_wikipedia_article | 328 | 4.56 |
| too_short | 1 | 0.01 |

**People (biography)**

| Status | Count | % |
| --- | ---: | ---: |
| ok | 7,813 | 71.46 |
| no_wikidata_item | 1,990 | 18.20 |
| no_english_wikipedia_article | 1,123 | 10.27 |
| too_short | 7 | 0.06 |
| empty_introduction | 1 | 0.01 |

**Number of principals with a biography per film**

| Biographies | Films |
| ---: | ---: |
| 0 | 470 |
| 1 | 531 |
| 2 | 878 |
| 3 | 5,300 |

**Sample**

| tconst | Title | Year | Synopsis (start) | Source |
| --- | --- | ---: | --- | --- |
| tt0111161 | The Shawshank Redemption | 1994 | In 1947, Portland, Maine, banker Andy Dufresne arrives at Shawshank State Prison... | https://en.wikipedia.org/wiki/The_Shawshank_Redemption |
| tt1375666 | Inception | 2010 | Dom Cobb and Arthur are professional thieves who perform corporate espionage... | https://en.wikipedia.org/wiki/Inception |
| tt0816692 | Interstellar | 2014 | In the near future, humanity faces extinction due to dust storms... | https://en.wikipedia.org/wiki/Interstellar_(film) |

A synopsis was found for 83 % of the films and a biography for 71 % of the people; 74 % of the films have a biography for all three principals. Missing information is not an error of the pipeline: every record without content keeps a `status` and a `note` explaining why (no Wikidata item, no English article, no plot section).

### 3.2 Methodological explanation

#### Methodology and why it was chosen

We selected the top 10 feature-length movies (at least 40 minutes) for each genre and year between 1980 and 2025, ranked by the number of IMDb votes. For each movie, we selected the first three principals based on IMDb's `ordering` field.

We used Wikidata to find the correct Wikipedia pages through the IMDb identifier (property P345). This method was chosen because it is more reliable than searching by movie title or person name.

For each movie, we extracted the synopsis from the "Plot" section of its Wikipedia article. For each principal, we collected biographical information from the introduction of their Wikipedia page.

To respect Wikipedia's policies, we sent requests one at a time, waited at least 0.5 seconds between requests, and paused when the server requested a delay.

For traceability, each record includes the source URL, Wikidata identifier, Wikipedia revision ID, and retrieval date. When information was missing, we added a note explaining the reason, such as a missing Wikipedia page or Plot section.

Python was used to collect the information, while PySpark handled data cleaning and validation. Spark applied a predefined schema, rejected invalid JSON records, and kept the most recent version of each record.

Our approach can scale to the full IMDb dataset because the selection criteria are configurable parameters, and the scraper can resume from where it stopped without restarting the entire process.

#### Alternatives considered and trade-offs

We considered searching Wikipedia directly by movie title or person name. However, this could produce incorrect results when different movies or people share similar names. Using IMDb identifiers through Wikidata provides more accurate matching.

We also considered other sources, such as TMDB, but it requires an API key and has its own usage conditions. Wikipedia was preferred because it provides accessible, human-written information.

Another option was downloading the entire Wikipedia dataset. However, this would require significantly more storage and processing resources, while our approach only retrieves the information needed.

The main limitation is that some IMDb movies or people may not have a corresponding Wikidata entry or Wikipedia page. In particular, if a movie has no Wikidata entry, our method cannot find its Wikipedia article, even if one exists. API requests can also make the enrichment process slower for large datasets.

## 4. Streaming

### 4.1 How it works

We listen to the live Wikipedia changes (the Wikimedia `recentchange` stream). A small Python script (`src/streaming/producer.py`) sends every event to Kafka, in a topic called `wiki-changes`. Then our Spark Structured Streaming job (`src/streaming/stream_job.py`) reads Kafka and checks if the changed page is about something from IMDb.

### 4.2 What we keep

First we built a list of IMDb names with Spark (`src/streaming/watchlist.py`): the movies and series with at least 5,000 votes (22,538 titles), and the actors, directors, writers and producers known for these titles (862,271 people). We put all names in lower case and removed things like "(2010 film)" so they can be compared with Wikipedia page titles. We skipped names shorter than 4 letters because they matched too many random pages.

On the Wikipedia side we only keep edits of English articles (no talk pages, no other languages). If the page title has something in brackets we use it: "(film)" can only match a movie, "(actor)" can only match a person, and something like "(album)" is ignored. We mark a match as `qualified` when it had brackets and `name_only` when it didn't.

### 4.3 Late data

We use the time when the edit happened on Wikipedia, not the time it arrived in Kafka. We put a **watermark of 10 minutes**: if an event arrives less than 10 minutes late it is still counted, if it is later it is dropped. It also means Spark doesn't have to keep old data in memory forever.

The watermark is used to remove duplicate events (they can happen when the connection restarts) and for the 10-minute windows where we count edits per title or person. A window is only written once the watermark has passed it, so late events can still be added to it.

We picked 10 minutes because in our test the events arrived after 4 seconds on average (19 seconds max), so 10 minutes is a big margin without keeping too much in memory.

To check it, we sent fake events from a file (`src/streaming/sample_events.jsonl`). The duplicate was counted once, "Barbie (film)" (3 minutes late) was kept and "Titanic (1997 film)" (25 minutes late) was dropped. The first time, Titanic was not dropped: Spark only updates the watermark at the end of a batch, and our first batch took 26 seconds, so Titanic arrived too early. With a longer pause it worked.

### 4.4 Results (live test, about 25 minutes)

| | Value |
| --- | --- |
| Matching edits | 82 |
| Different pages | 65 |
| Average / max delay | 4.1 s / 19 s |
| People (qualified / name only) | 2 / 50 |
| Titles (qualified / name only) | 11 / 19 |

So we got about 3 matching edits per minute, which is enough for a 15-30 minute test. More details in `outputs/streaming/summary.md`.

### 4.5 Limits

Some `name_only` matches are just the same name: "Hamlet" matched a film and a person, and "The Prodigy" is also a band. The `qualified` ones are reliable. Also, our laptop only has 8 GB of RAM, so batches took 30 to 50 seconds instead of 30, and the last windows were not written because we stopped the job before the watermark passed them.

## 5. Open question

### 5.1 Question

People often say "old movies were better". On IMDb, old films often look better rated, but is it because they are really better, or because only the good old films are still watched today (survivorship bias)? Recent films are rated by everybody, good or bad, while a bad film from 1950 is probably forgotten.

If it is survivorship bias, we should see that:
- the average film of each decade is rated about the same;
- the old films with many votes are much better rated than the others;
- when we compare films with the same visibility, the gap between old and recent films gets much smaller.

The code is in `src/open_question/run.py` (`python -m src.open_question.run`) and the results are in `outputs/open_question.md`. We used 314,490 rated feature films (movies of 40 minutes to 24 hours) released between 1920 and 2025.

### 5.2 Results

**All rated films by decade**

| Decade | Films | Mean rating | Vote-weighted rating | Median votes | Correlation log(votes) / rating |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1920s | 3,471 | 6.05 | 7.53 | 35 | 0.332 |
| 1950s | 12,505 | 6.13 | 7.50 | 68 | 0.251 |
| 1970s | 21,960 | 5.89 | 7.49 | 65 | 0.174 |
| 1990s | 24,091 | 6.03 | 7.31 | 98 | 0.043 |
| 2000s | 44,161 | 6.20 | 7.01 | 106 | -0.125 |
| 2010s | 89,366 | 6.20 | 6.89 | 81 | -0.184 |
| 2020s | 60,412 | 6.20 | 6.57 | 78 | -0.191 |

The normal average is basically the same in every decade (around 6.0-6.2). Old films only look better when we weight by votes (7.53 in the 1920s vs 6.57 in the 2020s), which means the old films that people vote for the most are the good ones. The correlation shows it clearly: for old films, more votes = better rating (+0.33), but for recent films it is the opposite (-0.19), because big popular movies get a lot of votes even when they are average.

**Equal visibility: the 100 most-voted films of each decade**

| Decade | Mean rating (all) | Mean rating (top 100) |
| --- | ---: | ---: |
| 1920s | 6.05 | 7.36 |
| 1940s | 6.16 | 7.70 |
| 1960s | 6.02 | 7.63 |
| 1980s | 5.97 | 7.60 |
| 2000s | 6.20 | 7.80 |
| 2010s | 6.20 | 7.72 |
| 2020s | 6.20 | 7.09 |

When we compare the most watched films of each decade, they are all rated about the same (7.4 to 7.8). So the classics from the past are not better than the most watched films of today. Only the 2020s are lower (7.09), probably because these films are too recent and still don't have enough votes to be "selected".

**Same number of votes for every decade**

| Decade | ≥ 1,000 votes | ≥ 10,000 votes | ≥ 100,000 votes |
| --- | ---: | ---: | ---: |
| 1920s | 7.02 | 7.82 | 8.08 |
| 1950s | 6.65 | 7.42 | 8.01 |
| 1980s | 6.27 | 6.72 | 7.40 |
| 2000s | 6.20 | 6.49 | 6.89 |
| 2020s | 6.07 | 6.33 | 6.79 |

With a fixed number of votes, old films look better again. But this comparison is not fair: an old film with 100,000 votes is a famous classic (there are very few of them), while lots of normal recent films reach 100,000 votes easily. So this table actually shows the selection effect instead of a real quality difference.

### 5.3 Answer

Old films are not better rated in general: the average film of every decade gets about 6.0-6.2. They only look better because the old films that are still watched today are mostly the good ones, and when we compare the most watched films of each decade, they all get about the same rating. So the "old films were better" idea on IMDb is mostly **survivorship bias**.

### 5.4 Methodological explanation

#### Methodology and why it was chosen

We used the silver tables `title.basics` and `title.ratings` and kept feature films with a rating. We grouped them by decade and compared three things: the normal mean, the vote-weighted mean, and the correlation between the number of votes (in log, votes go from a few votes to about 3 million) and the rating. Then we compared films with the same visibility in two ways: the 100 most-voted films of each decade (with a window function `row_number` per decade), and fixed vote thresholds. Everything is done with Spark.

#### Alternatives considered and trade-offs

- We could have only compared the mean rating per decade, but this would not tell us why old films sometimes look better, so we added the visibility comparisons.
- We used the top 100 per decade and not a top percentage, because the number of films per decade is very different (3,471 in the 1920s vs 89,366 in the 2010s).
- The fixed vote thresholds are easy to understand, but they are not fair between decades, as explained above.
- IMDb ratings come from today's users, so we cannot know how films were rated when they came out. A source with ratings from the release time would be needed to go further.