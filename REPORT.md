# IMDb Big Data Processing - Report

## 1. Data preparation

### 1.1 Missing values per column

<!-- coller le tableau de outputs/null_report.md -->

### 1.2 Top 5 columns with the most missing values

<!-- coller le top 5 de outputs/null_report.md -->

### 1.3 Other data quality issues

<!-- coller le tableau de outputs/quality_report.md -->

### 1.4 Methodological explanation

#### Methodology and why it was chosen

#### Alternatives considered and trade-offs

## 2. Data exploration

<!-- Talia -->

## 3. Data enrichment

### 3.1 Results

<!-- coller outputs/enrichment_summary.md (couverture, statuts, exemple) -->

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