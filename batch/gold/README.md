# 🪙 Gold Layer — Snowflake + dbt Core Configuration

Welcome to the core data warehousing and business intelligence modeling layer of the FootballFlow platform. This directory contains the complete dbt (Data Build Tool) project used to transform raw, semi-structured ingestion schemas into a highly optimized enterprise Galaxy Schema (Fact Constellation) inside Snowflake.  
📂 Gold Layer Directory Structure

As shown in our repository architecture, this folder contains the standard structure of a professional dbt Core engineering environment:  
Plaintext
```text
batch/gold/
├── 📂 analyses/          # Ad-hoc analytical SQL queries
├── 📂 macros/            # Reusable SQL functions (generic testing, date utilities)
├── 📂 models/            # Core transformations (The Medallion Pipeline)
│   ├── 📂 staging/       # Light cleaning, casting, and renaming
│   ├── 📂 dimensions/    # Conformed dimensions (dim_player, dim_club, etc.)
│   ├── 📂 facts/         # Multi-grain fact tables
│   └── 📂 marts/         # BI-ready business aggregations
├── 📂 seeds/             # Static CSV reference mappings (e.g., country codes)
├── 📂 snapshots/         # Slowing Changing Dimensions (SCD Type 2) scripts
├── 📂 tests/             # Custom singular data quality tests
├── .gitignore
├── dbt_project.yml       # Main dbt project configuration file
├── packages.yml          # External dbt package dependencies (dbt_utils)
└── requirements.txt      # Python dependencies for the gold environment
```
# ❄️ Snowflake Data Warehouse Architecture

Data moves through four distinct, isolated schemas inside the FOOTBALL_DE Snowflake database to maintain strict governance and separation of concerns:  
```text
Snowflake (FOOTBALL_DE Database)
├── 📂 RAW Schema         ◄── COPY INTO files from S3 Silver Parquet paths
├── 📂 STAGING Schema     ◄── dbt stg_* models (views for light casting & renaming)
├── 📂 ANALYTICS Schema   ◄── dbt dim_ / fact_ / mart_* models (The Galaxy Schema)
└── 📂 SNAPSHOTS Schema   ◄── dbt snapshots (Historical SCD Type 2 tracking)
```
# 🏗️ Data Modeling Strategy: The Galaxy Schema

Instead of a traditional single-fact Star Schema, this project implements a Galaxy Schema (Fact Constellation). Because football analytics demands slicing metrics across completely different grains (e.g., per-match statistics, historical club transfers, or real-time live events), we utilize five distinct fact tables sharing a core set of conformed dimensions.  
1. Conformed Dimensions (📂 models/dimensions)

These dimensions provide a single source of truth across all analytical boundaries:  

    dim_player: Comprehensive player profiles, historical details, and convenience columns for current valuations.  

    dim_club: Detailed club metadata and leagues tracker.  

    dim_competition: Official league, cup, and tournament structural metadata.  

    dim_date: A conformed calendar bone-structure (Date Spine) to support continuous time-series joins.  

2. Fact Tables (📂 models/facts)

    fact_appearances: Grain: One row per player per match (tracks minutes, goals, assists).  

    fact_matches: Grain: One row per played match (scores, hosting metadata, endpoints).  

    fact_transfers: Grain: One row per recorded player transfer (market values, fees, destination clubs).  

    fact_player_valuations: Grain: One snapshot record per valuation adjustment.  

    fact_game_events: Grain: One row per discrete in-match action (goals, cards, substitutions). This table is built to accept hybrid loads (combining regular historical batch data with real-time stream syncs).  

3. Business Marts (📂 models/marts)

Marts are wide, heavily indexed, pre-aggregated tables exposed directly to downstream consumption tools like Power BI:  

    mart_player_performance: Season-over-season aggregated metrics for advanced scout-reporting.  

    mart_club_match_stats: Dynamically unpivoted match metrics highlighting home vs. away win configurations.  

    mart_club_transfer_spending: Financial balance ledger maps calculating net transfer profits and losses.  

    mart_match_events_live: High-frequency metrics displaying in-game event flows.  

🕰️ Historical Auditing (SCD Type 2 Snapshots)

To prevent data loss from changing source profiles, we configure dbt snapshots to capture historical progression via Slowly Changing Dimensions (SCD Type 2).  

    snap_dim_player: Tracks structural player profile edits.

    snap_player_valuations: Audits chronological player market value adjustments over time.  

🛠️ Validation & Data Quality Guardrails

To ensure production stability, every single model in this repository is strictly bound to data quality constraints enforced inside schema.yml configurations.  

We execute robust testing combinations:  

    Uniqueness & Nullability: Asserted on primary and surrogate keys (unique, not_null).  

    Referential Integrity: Relationship checks ensuring foreign keys map flawlessly to corresponding dimensions (e.g., validating that every sk_player inside fact_appearances resolves cleanly to a valid record inside dim_player).  

# 🚀 How to Run the Gold Layer Locally
Prerequisites

Ensure your local ~/.dbt/profiles.yml is populated with correct Snowflake warehouse credentials, mapping out schemas for RAW, STAGING, and ANALYTICS.  
Step-by-Step Execution Sequence

Navigate to this folder and execute the development chain:  
Bash

# 1. Install required upstream packages (e.g., dbt_utils)
dbt deps

# 2. Build the historical audit tables (SCD Type 2)
dbt snapshot

# 3. Compile and build the Staging view layer
dbt run --select staging

# 4. Process and build the shared Dimension tables
dbt run --select dimensions

# 5. Materialize the multi-grain Fact tables
dbt run --select facts

# 6. Build the final wide Analytical Marts for Power BI
dbt run --select marts

# 7. Run comprehensive data quality and referential testing suites
dbt test
