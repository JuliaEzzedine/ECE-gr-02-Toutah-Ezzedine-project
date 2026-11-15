## Missing values per column

| table            | column            |   total_rows |   missing_total |   pct_missing |   null_count |   imdb_null_count |   empty_count |   invalid_numeric |
|:-----------------|:------------------|-------------:|----------------:|--------------:|-------------:|------------------:|--------------:|------------------:|
| name.basics      | nconst            |     15816005 |               0 |         0     |            0 |                 0 |             0 |               nan |
| name.basics      | primaryName       |     15816005 |             293 |         0.002 |            0 |               293 |             0 |               nan |
| name.basics      | birthYear         |     15816005 |        15130128 |        95.663 |            0 |          15130128 |             0 |                 0 |
| name.basics      | deathYear         |     15816005 |        15550475 |        98.321 |            0 |          15550475 |             0 |                 0 |
| name.basics      | primaryProfession |     15816005 |         3220639 |        20.363 |            0 |           3220639 |             0 |               nan |
| name.basics      | knownForTitles    |     15816005 |         1917578 |        12.124 |            0 |           1917578 |             0 |               nan |
| title.akas       | titleId           |     60424702 |               0 |         0     |            0 |                 0 |             0 |               nan |
| title.akas       | ordering          |     60424702 |               0 |         0     |            0 |                 0 |             0 |                 0 |
| title.akas       | title             |     60424702 |               0 |         0     |            0 |                 0 |             0 |               nan |
| title.akas       | region            |     60424702 |        13002884 |        21.519 |            0 |          13002884 |             0 |               nan |
| title.akas       | language          |     60424702 |        19665115 |        32.545 |            0 |          19665115 |             0 |               nan |
| title.akas       | types             |     60424702 |        40393110 |        66.849 |            0 |          40393110 |             0 |               nan |
| title.akas       | attributes        |     60424702 |        60107437 |        99.475 |            0 |          60107437 |             0 |               nan |
| title.akas       | isOriginalTitle   |     60424702 |               0 |         0     |            0 |                 0 |             0 |                 0 |
| title.basics     | tconst            |     12963111 |               0 |         0     |            0 |                 0 |             0 |               nan |
| title.basics     | titleType         |     12963111 |               0 |         0     |            0 |                 0 |             0 |               nan |
| title.basics     | primaryTitle      |     12963111 |               0 |         0     |            0 |                 0 |             0 |               nan |
| title.basics     | originalTitle     |     12963111 |               0 |         0     |            0 |                 0 |             0 |               nan |
| title.basics     | isAdult           |     12963111 |               0 |         0     |            0 |                 0 |             0 |                 0 |
| title.basics     | startYear         |     12963111 |         1487320 |        11.473 |            0 |           1487320 |             0 |                 0 |
| title.basics     | endYear           |     12963111 |        12798642 |        98.731 |            0 |          12798642 |             0 |                 0 |
| title.basics     | runtimeMinutes    |     12963111 |         8050574 |        62.104 |            0 |           8050574 |             0 |                 0 |
| title.basics     | genres            |     12963111 |          548144 |         4.228 |            0 |            548144 |             0 |               nan |
| title.crew       | tconst            |     12960686 |               0 |         0     |            0 |                 0 |             0 |               nan |
| title.crew       | directors         |     12960686 |         5791405 |        44.684 |            0 |           5791405 |             0 |               nan |
| title.crew       | writers           |     12960686 |         6432432 |        49.63  |            0 |           6432432 |             0 |               nan |
| title.episode    | tconst            |     10034862 |               0 |         0     |            0 |                 0 |             0 |               nan |
| title.episode    | parentTconst      |     10034862 |               0 |         0     |            0 |                 0 |             0 |               nan |
| title.episode    | seasonNumber      |     10034862 |         2126237 |        21.189 |            0 |           2126237 |             0 |                 0 |
| title.episode    | episodeNumber     |     10034862 |         2126237 |        21.189 |            0 |           2126237 |             0 |                 0 |
| title.principals | tconst            |    102756358 |               0 |         0     |            0 |                 0 |             0 |               nan |
| title.principals | ordering          |    102756358 |               0 |         0     |            0 |                 0 |             0 |                 0 |
| title.principals | nconst            |    102756358 |               0 |         0     |            0 |                 0 |             0 |               nan |
| title.principals | category          |    102756358 |               0 |         0     |            0 |                 0 |             0 |               nan |
| title.principals | job               |    102756358 |        82882023 |        80.659 |            0 |          82882023 |             0 |               nan |
| title.principals | characters        |    102756358 |        52691057 |        51.278 |            0 |          52691057 |             0 |               nan |
| title.ratings    | tconst            |      1732169 |               0 |         0     |            0 |                 0 |             0 |               nan |
| title.ratings    | averageRating     |      1732169 |               0 |         0     |            0 |                 0 |             0 |                 0 |
| title.ratings    | numVotes          |      1732169 |               0 |         0     |            0 |                 0 |             0 |                 0 |

## Top 5 columns by percentage of missing values

| table            | column     |   total_rows |   missing_total |   pct_missing |
|:-----------------|:-----------|-------------:|----------------:|--------------:|
| title.akas       | attributes |     60424702 |        60107437 |        99.475 |
| title.basics     | endYear    |     12963111 |        12798642 |        98.731 |
| name.basics      | deathYear  |     15816005 |        15550475 |        98.321 |
| name.basics      | birthYear  |     15816005 |        15130128 |        95.663 |
| title.principals | job        |    102756358 |        82882023 |        80.659 |
