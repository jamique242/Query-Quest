<h1>📍Project Purpose </h1>

This project simulates the ingestion and taming of messy, event-style transportation data using SQL.

The objective at this stage is to:
* Perform exploratory SQL analysis on event-based trip data
* Identify, document, and correct data quality issues
* Transform raw trips into a clean, analytics-ready dataset
* Derive daily and aggregate metrics to support analytical insights
* Apply rolling/window calculations to uncover patterns in usage and demand
* Clearly document assumptions, decisions, and findings

<h1>Dataset Context</h1>

<b>Dataset:</b> NYC Taxi Trip Records (sample month)

<b>Source:</b> NYC TLC public dataset

<b>Business domain:</b> Transportation / Mobility analytics

<b>Expected grain:</b> Individual taxi trip (trip_id or row-level unique identifier)

<h1>🗺️ Folder Structure</h1>

* data/      → Raw source data <i>      ** csv due to size of parquet</i>
* sql/       → Cleaning, feature engineering, and analytics logic
* outputs/   → Results and aggregated data artifacts

<h1>📌 Assumptions</h1>
During the wrangling phase of Adventure.02, the following assumptions were made:

* The raw trip dataset is a representative subset of taxi activity for the selected period.
* Each trip record (trip_id, pickup_datetime, dropoff_datetime) represents a single event.
* Location coordinates may contain errors or missing values; assumptions about validity are made based on reasonable geographic boundaries.
* Duration and fare fields may contain negative or zero values; these are corrected or removed for downstream analytics.
* Derived metrics (daily counts, averages, peak hours) are calculated after scrubbing to ensure reliability.

<h1>🤼 Wrangling Decisions</h1>

<h3>🧼 Data Scrubbing</h3>

* Negative trip durations and impossible coordinates were detected and corrected.
* Missing values for key fields (pickup/dropoff times, coordinates, fare) were either imputed or removed.
* Duplicate records were identified and eliminated.
* Grain enforcement: Each trip is treated as a unique row-level event; surrogate <b>trip_id</b> was added when natural identifiers were inconsistent.
* Inconsistencies handled: Geographic coordinates outside NYC bounds, negative fares, and dropoff times preceding pickup times were flagged and corrected or removed.
* All cleaning logic resides in the <b>sql/01_data_scrubbing/</b> folder to ensure traceability from raw data to cleaned output.

<h3>⚙️ Feature Engineering & Aggregation</h3>

* Derived metrics include trip duration, daily totals, hourly utilization, and revenue aggregates.
* Window functions were applied to calculate rolling averages, peak demand hours, and utilization rates.
* Aggregations are intentionally separated from raw data to maintain traceability.
* Feature engineering logic lives in the <b>sql/02_feature_engineering/</b> folder for clarity and reusability.

<h3>📊 Analytics Patterns</h3>

* Analytical queries uncover trends, peak usage periods, and operational insights.
* Rolling averages, peak hour analysis, and revenue/utilization metrics were calculated on cleaned and feature-engineered data.
* Analytics logic is contained in the <b>sql/03_analytics_patterns/</b> folder, ensuring reproducibility and separation from cleaning and feature engineering steps.

<h1>📝 Observations</h1>

* Event-based datasets require more <b>data validation upfront</b> than clean transactional datasets.
* Transformations and derived metrics should always preserve <b>traceability to the original events</b>.
* Applying analytics (rolling averages, peak hour detection) is best done <b>after cleaning</b>, to avoid propagating errors.
* Over-engineering transformations early can obscure insights; a balance between <b>cleaning, usability, and transparency</b> is essential.
