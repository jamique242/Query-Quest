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
