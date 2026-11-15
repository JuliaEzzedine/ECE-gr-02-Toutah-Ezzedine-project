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

<!-- Talia -->

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

<!-- Talia -->

## 5. Open question

<!-- ensemble -->