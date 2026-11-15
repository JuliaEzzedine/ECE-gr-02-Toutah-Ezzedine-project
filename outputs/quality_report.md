## Data quality rules

| table            | rule                           | description                                                              |   rows_affected |   pct_rows | handling                            |
|:-----------------|:-------------------------------|:-------------------------------------------------------------------------|----------------:|-----------:|:------------------------------------|
| title.basics     | dq_duplicate_key               | Duplicate primary key (tconst)                                           |               0 |     0      | quarantine                          |
| title.basics     | dq_missing_title               | No primary title                                                         |               0 |     0      | flag                                |
| title.basics     | dq_invalid_is_adult            | isAdult is not 0 or 1                                                    |               0 |     0      | nullify isAdult                     |
| title.basics     | dq_end_before_start            | endYear earlier than startYear                                           |               0 |     0      | nullify endYear                     |
| title.basics     | dq_end_year_not_series         | endYear set on a title that is not a series                              |               0 |     0      | nullify endYear                     |
| title.basics     | dq_start_in_future             | startYear in the future (announced / in production)                      |            6238 |     0.0481 | flag                                |
| title.basics     | dq_start_too_old               | startYear before the first motion pictures (1870)                        |               0 |     0      | flag                                |
| title.basics     | dq_runtime_not_positive        | runtimeMinutes <= 0                                                      |               4 |     0      | nullify runtimeMinutes              |
| title.basics     | dq_runtime_extreme             | runtimeMinutes above 1440 (24 h)                                         |             210 |     0.0016 | flag                                |
| title.basics     | dq_missing_genres              | No genre                                                                 |          548144 |     4.2285 | flag                                |
| name.basics      | dq_duplicate_key               | Duplicate primary key (nconst)                                           |               0 |     0      | quarantine                          |
| name.basics      | dq_missing_name                | No name                                                                  |             293 |     0.0019 | flag                                |
| name.basics      | dq_death_before_birth          | deathYear earlier than birthYear (impossible to know which one is wrong) |              28 |     0.0002 | nullify birthYear, deathYear        |
| name.basics      | dq_birth_in_future             | birthYear in the future                                                  |               0 |     0      | nullify birthYear                   |
| name.basics      | dq_death_in_future             | deathYear in the future                                                  |               0 |     0      | nullify deathYear                   |
| name.basics      | dq_lifespan_too_long           | Lifespan above 120 years                                                 |               2 |     0      | flag                                |
| name.basics      | dq_presumed_dead_no_death_year | No deathYear but born more than 120 years ago                            |            2794 |     0.0177 | flag                                |
| name.basics      | dq_known_for_orphans           | knownForTitles referenced titles missing from title.basics (ids removed) |              67 |     0.0004 | flag                                |
| title.ratings    | dq_duplicate_key               | Duplicate primary key (tconst)                                           |               0 |     0      | quarantine                          |
| title.ratings    | dq_orphan_title                | tconst missing from title.basics                                         |               0 |     0      | quarantine                          |
| title.ratings    | dq_rating_out_of_range         | averageRating outside [1, 10]                                            |               0 |     0      | quarantine                          |
| title.ratings    | dq_votes_not_positive          | numVotes <= 0                                                            |               0 |     0      | quarantine                          |
| title.episode    | dq_duplicate_key               | Duplicate primary key (tconst)                                           |               0 |     0      | quarantine                          |
| title.episode    | dq_orphan_episode              | Episode tconst missing from title.basics                                 |               0 |     0      | quarantine                          |
| title.episode    | dq_orphan_parent               | parentTconst missing from title.basics                                   |               5 |     0      | quarantine                          |
| title.episode    | dq_self_parent                 | Episode is its own parent                                                |               0 |     0      | quarantine                          |
| title.episode    | dq_negative_numbers            | Negative season or episode number                                        |               0 |     0      | nullify seasonNumber, episodeNumber |
| title.episode    | dq_missing_season              | No seasonNumber                                                          |         2126237 |    21.1885 | flag                                |
| title.crew       | dq_duplicate_key               | Duplicate primary key (tconst)                                           |               0 |     0      | quarantine                          |
| title.crew       | dq_orphan_title                | tconst missing from title.basics                                         |              10 |     0.0001 | quarantine                          |
| title.crew       | dq_director_orphans            | directors referenced people missing from name.basics (ids removed)       |             405 |     0.0031 | flag                                |
| title.crew       | dq_writer_orphans              | writers referenced people missing from name.basics (ids removed)         |             785 |     0.0061 | flag                                |
| title.principals | dq_duplicate_key               | Duplicate primary key (tconst, ordering)                                 |               0 |     0      | quarantine                          |
| title.principals | dq_orphan_title                | tconst missing from title.basics                                         |               0 |     0      | quarantine                          |
| title.principals | dq_orphan_person               | nconst missing from name.basics                                          |            7262 |     0.0071 | quarantine                          |
| title.principals | dq_born_after_release          | Principal born after the title was released                              |            3109 |     0.003  | flag                                |
| title.principals | dq_died_before_release         | Principal died before the release (not archive footage)                  |          287065 |     0.2794 | flag                                |
| title.akas       | dq_duplicate_key               | Duplicate primary key (titleId, ordering)                                |               0 |     0      | quarantine                          |
| title.akas       | dq_orphan_title                | titleId missing from title.basics                                        |               8 |     0      | quarantine                          |
| title.akas       | dq_invalid_is_original         | isOriginalTitle is not 0 or 1                                            |               0 |     0      | nullify isOriginalTitle             |
