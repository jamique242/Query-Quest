<h1>📍Project Purpose </h1>

This project simulates the ingestion and modeling of a retail transactional dataset using SQL and a lakehouse-style workflow (MotherDuck + Parquet).

The objective of Week 1 is to:
* Upload Raw data as Parquet (unmodified)
* Exploratory SQL analysis
* Analyze, Define and Document grain definition
* Build a clean staging model

<h1>Dataset Context</h1>

<b>Dataset:</b> Sample Superstore

<b>Source:</b> Tableau public dataset

<b>Business domain:</b> Retail transactions

<b>Expected grain:</b> Order Line (Order ID + Product)

<h1>🗺️ Folder Structure</h1>

* data/      → Raw source data (immutable)
* sql/       → Transformation logic
* outputs/   → Modeled data artifacts

<h1>Assumptions</h1>
We assumed grain level data would be easily obtained for all various dim tables. In this modeling layer transformations were kept to a need only basis. No filtering was done and no in depth normalization. 

<h1>📝 Observations</h1>
During this project a few things were observed:

* Though logically my grain should have been order_id + product_id it wasn't and that was why row_id was created for logging transaction grain data. - My initial theory was wrong.
* 
* 
