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