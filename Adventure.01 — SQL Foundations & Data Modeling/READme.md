<h1>📍Project Purpose </h1>

This project simulates the ingestion and modeling of a retail transactional dataset using SQL and a lakehouse-style workflow (MotherDuck + Parquet).

The objective at this stage is to:
* Exploratory SQL analysis
* Analyze, Define and Document grain definition
* Build a clean staging model
* Create a structured analytical layer
* Define a business case and satisfy all questions using the model created
* Clearly document findings and process
  
[Test(Adventure.01 — SQL Foundations & Data Modeling/sql/analytic queries/Readme.md)]
<h1>Dataset Context</h1>

<b>Dataset:</b> Sample Superstore

<b>Source:</b> Tableau public dataset

<b>Business domain:</b> Retail transactions

<b>Expected grain:</b> Order Line (Order ID + Product)

<h1>🗺️ Folder Structure</h1>

* data/      → Raw source data (immutable)
* sql/       → Transformation logic
* outputs/   → Modeled data artifacts

<h1>📌 Assumptions</h1>
During the modeling phase of Adventure 01, the following assumptions were made:

* The source transactional data represents complete order activity.
* Each customer_id, product_id, and city_code maps consistently to a single business entity.
* Profit and discount values are recorded at the transaction line level and do not require recalculation.
* Grain-level data could be reliably derived for dimension tables without requiring deep normalization.

<h1>🏗 Modeling Decisions</h1>
<h3>1️⃣ Star Schema Orientation</h3>

The model follows a basic star schema pattern:

* fact_orders as central transactional fact table
* dim_customer
* dim_product
* dim_location

There were tradeoffs in this modeling approach, such as limited normalization and reduced OLAP optimization.

<h3>2️⃣ Minimal Transformation </h3>

Transformations were intentionally limited to:
* Standardizing date fields
* Deriving year/month attributes
* Creating surrogate keys where needed
* Resolving minor inconsistencies (e.g., city/zipcode mapping)

This was done to ensure transparency of source data, traceability from fact to staging and to avoid premature optimization. As a tradeoff some data quality issues remained visible. This model prioritizes clarity over aggressive cleansing.

<h3>3️⃣ fact_orders Table Grain Correction</h3>

Initial assumption was that the natural grain of the dataset would be order_id + product_id.
Upon inspection, this assumption proved incorrect. Multiple records existed where this combination did not uniquely identify a transaction row.

<b>Descision</b> <br>
A surrogate row_id was kept to enforce a true transactional grain. The fact table was modeled at row-level transaction grain (row_id), rather than assuming uniqueness at the order_id + product_id level.


<h3>4️⃣ dim_location Inconsistencies</h3>

During modeling, a zipcode was found to map to multiple cities (e.g., 92024 listed as both Encinitas and San Diego). Research confirmed a single correct mapping that 92024, mapped to Encinitas.

There was 2 feasible options to correct this issue:
* Standardized the addresses by introducing another table with correct locations and join this to regions.
* Create a new PK referencing 2 most granular levels of detail: city and zipcode.

<b>Descision</b> <br>
We decided to use create a new pk based on the information we get from the existing table. This descision supports our initiative for transparency and reducing transformations in our modeling layer. 

To future proof this and create a more scalable product there will be a need for a future reference/validation table.

<h3>5️⃣ dim_product Inconsistencies</h3>

During modeling multiple instances were found where a single product_id had differing product names. Though this was similar to what we encountered with our dim_location table this was a bit different as there was no way to be 100% certain which name should persist. These inconsistencies were seen at varrying time periods and sporadically. The only clear indicator was the individual price of an item (calculated). 

<b>Decision</b> <br>
We decided to keep model at product_id level, to ensure less transformations for our fact_orders table. We retained a consistent naming approach based on dominant/most representative value. Simplicity was prioritized over temporal dimensional accuracy.

To enhance the accuracy and integrity of our dim_product and subsequent product naming, there would need to be more data governance controls enforced in our CMS where we introduce products upstream. However in the interim a product_sku could be created by using individual product_id and price.

<h1>📝 Observations</h1>

* Logical grain assumptions must be validated with actual row uniqueness checks.
* Upstream data governance can be even more important than those enforced at the engineering level - especially when transparency is a deliverable.
* Resist the urge to over engineer especially when it contridicts your objectives.


