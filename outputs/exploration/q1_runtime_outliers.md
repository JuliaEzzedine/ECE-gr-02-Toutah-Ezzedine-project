## q1_runtime_outliers

Feature film = movie, runtime >= 40 min. Outlier = |z-score| >= 3.0 within the genre (z = (runtime - genre mean) / genre standard deviation).

### Outliers per genre

| genre       |   movies_in_genre |   mean_runtime |   std_runtime |   longer_outliers |   shorter_outliers |   pct_outliers |
|:------------|------------------:|---------------:|--------------:|------------------:|-------------------:|---------------:|
| Action      |             40412 |          101.2 |          31.3 |               325 |                  0 |           0.8  |
| Adventure   |             24030 |           93   |          30.6 |               302 |                  0 |           1.26 |
| Animation   |              8338 |           82.7 |          26.8 |                89 |                  0 |           1.07 |
| Biography   |             16112 |           89.2 |          29.4 |               164 |                  0 |           1.02 |
| Comedy      |             95804 |           93.2 |          22.4 |               802 |                  0 |           0.84 |
| Crime       |             34432 |           95.7 |          25.6 |               274 |                  0 |           0.8  |
| Documentary |            116275 |           76.5 |          30.2 |              1079 |                  0 |           0.93 |
| Drama       |            196243 |           96   |          27.4 |              1388 |                  0 |           0.71 |
| Family      |             15972 |           92.1 |          25.6 |               125 |                  0 |           0.78 |
| Fantasy     |             12601 |           93.9 |          28.9 |                80 |                  0 |           0.63 |
| Film-Noir   |               878 |           81.9 |          13.8 |                 3 |                  0 |           0.34 |
| Game-Show   |                 7 |           79.6 |          32.7 |                 0 |                  0 |           0    |
| History     |             13637 |           93.8 |          37.8 |               122 |                  0 |           0.89 |
| Horror      |             30579 |           88.7 |          22.3 |               217 |                  0 |           0.71 |
| Music       |             13981 |           87.8 |          30.2 |               170 |                  0 |           1.22 |
| Musical     |              8503 |          100.1 |          33.3 |                38 |                  0 |           0.45 |
| Mystery     |             16434 |           94   |          24.8 |               116 |                  0 |           0.71 |
| News        |              1409 |           77.6 |          32.2 |                19 |                  0 |           1.35 |
| Reality-TV  |               297 |           92.1 |          85.1 |                 1 |                  0 |           0.34 |
| Romance     |             42229 |           99.2 |          25.1 |               290 |                  0 |           0.69 |
| Sci-Fi      |              9782 |           91.2 |          30.5 |                61 |                  0 |           0.62 |
| Sport       |              6820 |           88.7 |          32.1 |                47 |                  0 |           0.69 |
| Talk-Show   |               102 |           88.1 |          33.5 |                 1 |                  0 |           0.98 |
| Thriller    |             34079 |           96.1 |          21.8 |               285 |                  0 |           0.84 |
| War         |              8049 |           94.8 |          31.2 |                83 |                  0 |           1.03 |
| Western     |              6354 |           76.5 |          28.7 |               101 |                  0 |           1.59 |

### 25 most extreme outliers

| genre       | tconst     | primaryTitle                               |   startYear |   runtimeMinutes |   genre_mean |   z_score |
|:------------|:-----------|:-------------------------------------------|------------:|-----------------:|-------------:|----------:|
| Horror      | tt7521772  | 24 Hour Psycho                             |        1993 |             1440 |         88.7 |     60.67 |
| Comedy      | tt45643419 | Ooio Aifujairea: Lifedesign                |        2025 |             1440 |         93.2 |     60.25 |
| Drama       | tt2008009  | The Clock                                  |        2010 |             1440 |         96   |     49.11 |
| Fantasy     | tt6127010  | Funusion                                   |        2016 |             1440 |         93.9 |     46.65 |
| Documentary | tt16385264 | Gondwana                                   |        2022 |             1440 |         76.5 |     45.18 |
| Documentary | tt38146238 | Sky TV                                     |        1966 |             1440 |         76.5 |     45.18 |
| Comedy      | tt8677246  | Bullfighting Memories                      |        2017 |             1100 |         93.2 |     45.04 |
| Drama       | tt35509411 | Our First Day                              |        2025 |             1311 |         96   |     44.39 |
| Sci-Fi      | tt6127010  | Funusion                                   |        2016 |             1440 |         91.2 |     44.27 |
| Adventure   | tt45643419 | Ooio Aifujairea: Lifedesign                |        2025 |             1440 |         93   |     44.04 |
| Documentary | tt32701008 | L'Afrique aux Portes de Paris              |        2024 |             1400 |         76.5 |     43.85 |
| Action      | tt45643419 | Ooio Aifujairea: Lifedesign                |        2025 |             1440 |        101.2 |     42.82 |
| Drama       | tt7156814  | The Innocence                              |        2019 |             1260 |         96   |     42.53 |
| Action      | tt0004052  | The Hazards of Helen                       |        1914 |             1428 |        101.2 |     42.43 |
| Documentary | tt1277455  | A Time to Stir                             |        2018 |             1320 |         76.5 |     41.2  |
| Musical     | tt22445582 | Sigur rós: Route One                       |        2016 |             1440 |        100.1 |     40.21 |
| Drama       | tt7549254  | Sakhi                                      |         nan |             1179 |         96   |     39.57 |
| Drama       | tt4567170  | Breaking Yop                               |        2015 |             1151 |         96   |     38.55 |
| Documentary | tt7529390  | Pakistan: Education and Women              |        2016 |             1184 |         76.5 |     36.7  |
| Comedy      | tt19711676 | Kahrlton Heights North                     |        2022 |              900 |         93.2 |     36.09 |
| Crime       | tt9900018  | His Last Stand                             |         nan |              990 |         95.7 |     34.93 |
| Sport       | tt3963450  | NHL: Washington Capitals 10 Greatest Games |        2010 |             1200 |         88.7 |     34.58 |
| Drama       | tt0821950  | A Mosca cieca                              |        1921 |             1035 |         96   |     34.31 |
| Documentary | tt42230083 | Behind the Royal Curtain:The Retreat Palm  |        2026 |             1100 |         76.5 |     33.91 |
| Documentary | tt39163316 | The Story of Documentary Film              |        2026 |              960 |         76.5 |     29.27 |

Full result: `outputs/exploration/q1_runtime_outliers_full/` (CSV)
