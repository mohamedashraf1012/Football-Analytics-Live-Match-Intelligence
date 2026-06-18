# ⚡ FootballFlow — Streaming Pipeline

> **Real-Time Match Intelligence** · Speed Layer of the FootballFlow Lambda Architecture

<br/>

## 🔭 Overview

This repository contains the **Streaming Pipeline** component of the FootballFlow data engineering platform — a production-grade speed layer that simulates live football match broadcasts and processes events in real time.

While the batch pipeline handles historical data at scale, this pipeline answers a different question: **what is happening right now, in this match, at this minute?**

A Python simulator replays real match events from the Transfermarkt dataset through Apache Kafka. A Spark Structured Streaming consumer processes and enriches those events, stores them in InfluxDB for time-series analysis, and surfaces them on live Grafana dashboards — all in real time.

---

## 🏗️ Architecture

```
football_events_enriched2.csv
           │
           ▼
  ┌─────────────────────┐
  │  Match Simulator    │  ← GUI app (Tkinter) — replays events minute by minute
  │  match_simulator.py │    with configurable speed (x5 / x10 / x18 / x30)
  └────────┬────────────┘
           │  JSON messages → topic: real_match_events
           ▼
  ┌─────────────────────┐
  │   Apache Kafka      │  ← KRaft mode (no Zookeeper), Confluent 7.6.1
  │   (Docker)          │    3 partitions · 24h retention
  └────────┬────────────┘
           │
           ▼
  ┌─────────────────────┐
  │  Spark Structured   │  ← Windowed aggregations · enrichment · validation
  │  Streaming          │    Jupyter all-spark-notebook (Spark 3.5.3)
  └────────┬────────────┘
           │
           ▼
  ┌─────────────────────┐
  │     InfluxDB        │  ← Time-series storage · match metrics per minute
  └────────┬────────────┘
           │
    ┌──────┴───────┐
    ▼              ▼
 Grafana      Snowflake
(live dash)  (sync job → fact_game_events)
```

---

## 📁 Repository Structure

```
streaming/
│
├── 📄 docker-compose.yml          ← Spins up Kafka + Kafka UI + Spark + InfluxDB + Grafana
├── 📄 config.py.example           ← Template for credentials (copy → config.py, never commit)
│
├── 📂 datasets/
│   └── raw_data/
│       ├── football_events_enriched2.csv   ← Enriched dataset used by the simulator
│       └── (other source CSVs)
│
├── 📂 pipeline/
│   ├── build_enriched_dataset.py   ← Merges 7 CSVs → produces football_events_enriched2.csv
│   └── verify_kafka.py             ← Health check: confirms Kafka broker is reachable
│
├── 📂 simulator/
│   └── match_simulator.py          ← Main GUI simulator (Tkinter) — select match, set speed, stream
│
├── 📂 notebooks/
│   ├── 01_stream_ingest.ipynb      ← Kafka consumer: read raw events from topic
│   ├── 02_stream_process.ipynb     ← Spark Structured Streaming: process + enrich events
│   └── 03_stream_sink_influxdb.ipynb ← Write processed events to InfluxDB
│
├── 📂 scripts/exploration/
│   ├── check_csv.py                ← Inspect source CSV schemas and row counts
│   ├── clean_nulls_colums.py       ← Identify and handle null columns
│   ├── dataQ.py                    ← Data quality checks on raw events
│   ├── download_csv_files.py       ← Download source CSVs from Kaggle
│   ├── merge_all_csv.py            ← Helper for CSV joining experiments
│   └── test_bronze.py              ← Validate bronze-layer source files
│
└── 📂 docs/
    ├── footballflow_dashboard3.json ← Grafana dashboard export (import-ready)
    ├── full stream.png              ← End-to-end pipeline diagram
    ├── stream in blocks.png         ← Component block diagram
    ├── stream pipeline.png          ← Detailed pipeline flow
    └── grafana dashboard.png        ← Grafana dashboard screenshot
```

---

## 🛠️ Tech Stack

| Component | Tool | Version |
|---|---|---|
| **Message Broker** | Apache Kafka (KRaft) | Confluent 7.6.1 |
| **Kafka UI** | Provectus Kafka UI | latest |
| **Stream Processing** | Apache Spark Structured Streaming | 3.5.3 |
| **Simulator UI** | Python + Tkinter | 3.10 / 3.11 |
| **Time-Series DB** | InfluxDB | (Docker) |
| **Live Dashboards** | Grafana | (Docker) |
| **Containerisation** | Docker + Docker Compose | latest |
| **Dataset** | Transfermarkt via Kaggle | 12 CSV tables |

---

## ⚙️ Services (docker-compose.yml)

| Service | Port | Description |
|---|---|---|
| `footballflow-kafka` | `9092` (host) · `29092` (internal) · `9094` (Spark) | Kafka broker — KRaft mode, single node |
| `footballflow-kafka-ui` | `8081` | Provectus Kafka UI — topic browser, consumer groups |
| `spark` | `8888` (Jupyter) · `4040` (Spark UI) | all-spark-notebook with PySpark 3.5.3 |
| `influxdb` | `8086` | Time-series storage for match metrics |
| `grafana` | `3000` | Live match event dashboards |

> Kafka runs in **KRaft mode** (no Zookeeper). The cluster ID is pre-configured — no manual init required.

---

## 🚀 Getting Started

### Prerequisites

| Tool | Version |
|---|---|
| Docker Desktop | Latest |
| Python | 3.10 or 3.11 |
| Java JDK | 11 (required by Spark locally) |

### 1. Clone the repository

```bash
git clone https://github.com/AbdallahAhmed7/Football-Analytics-Live-Match-Intelligence.git
cd Football-Analytics-Live-Match-Intelligence
```

### 2. Configure credentials

```bash
cp config.py.example config.py
# Fill in your InfluxDB token and Snowflake credentials
# config.py is in .gitignore — never commit it
```

### 3. Install Python dependencies

```bash
python -m venv venv
venv\Scripts\activate        # Windows
source venv/bin/activate     # Mac/Linux

pip install -r requirements.txt
```

### 4. Build the enriched dataset

Run this once to merge the 7 source CSVs into the simulator's input file:

```bash
python pipeline/build_enriched_dataset.py
```

This reads from `datasets/raw_data/` and produces `football_events_enriched2.csv` — a flat, enriched table with 35+ columns including player info, club stats, competition metadata, and lineup details.

### 5. Start all Docker services

```bash
docker-compose up -d
```

Verify everything is running:

```bash
docker ps
# Kafka UI → http://localhost:8081
# Spark (Jupyter) → http://localhost:8888
# Grafana → http://localhost:3000
# InfluxDB → http://localhost:8086
```

Confirm Kafka is reachable:

```bash
python pipeline/verify_kafka.py
```

### 6. Import the Grafana dashboard

1. Open Grafana at `http://localhost:3000`
2. Go to **Dashboards → Import**
3. Upload `docs/footballflow_dashboard3.json`
4. Connect to your InfluxDB data source

### 7. Run the Match Simulator

```bash
python simulator/match_simulator.py
```

The GUI will launch:

1. Enter a **Game ID** from the enriched dataset (first 10 available IDs are shown)
2. Choose a **simulation speed** — x5 (~18 min) · x10 (~9 min) · x18 (~5 min) · x30 (~3 min)
3. Click **▶ START SIMULATION**

The simulator replays match events minute by minute, streams each event as a JSON message to the `real_match_events` Kafka topic, and displays a live scoreboard with dual team feeds.

### 8. Process events with Spark

Open Jupyter at `http://localhost:8888` and run the notebooks in order:

```
01_stream_ingest.ipynb      → connect to Kafka, read raw events
02_stream_process.ipynb     → windowed aggregations, enrichment, validation
03_stream_sink_influxdb.ipynb → write metrics to InfluxDB
```

---

## 🎮 Match Simulator — How It Works

The simulator (`match_simulator.py`) is a full GUI application built with Tkinter.

**Input:** `football_events_enriched2.csv` — produced by `build_enriched_dataset.py` by joining 7 Transfermarkt CSV tables (events, games, players, clubs, competitions, lineups, club_games) into a flat enriched format with 35+ columns per event.

**Simulation flow:**

1. User selects a Game ID — the app previews the match (teams, score, competition, stadium, event count)
2. Events are replayed in chronological order, with real timing gaps between minutes scaled by the chosen speed factor
3. Each event is classified as `goals`, `cards`, or `substitutions` by parsing the `description` field
4. Each event is sent as a JSON message to Kafka topic `real_match_events` with full enriched context + metadata fields (`_ingested_at`, `_source`, `_pipeline`)
5. The live scoreboard updates in real time — goals increment the score, cards/subs update the stats counters, and each team's event feed scrolls live

**Kafka message schema (35+ fields):**

```json
{
  "game_event_id": "...",
  "game_id": 12345,
  "minute": 67,
  "event_type": "goals",
  "description": "Header from close range",
  "player_name": "...",
  "club_id": 123,
  "club_name": "...",
  "home_club_id": 123,
  "away_club_id": 456,
  "home_club_goals": 2,
  "away_club_goals": 1,
  "competition_name": "...",
  "stadium": "...",
  "season": "2023",
  "_ingested_at": "2026-06-19T10:00:00",
  "_source": "match_simulator_v2",
  "_pipeline": "footballflow_streaming"
}
```

---

## 📊 Grafana Dashboard

The live Grafana dashboard (importable from `docs/footballflow_dashboard3.json`) displays:

- **Goal timeline** — goals per team plotted over match minutes
- **Card counts** — yellow and red cards per team
- **Substitution tracker** — subs per team over time
- **Event rate** — total events per minute (match intensity indicator)

> 📸 See `docs/grafana dashboard.png` for a screenshot.

---

## 🔄 Pipeline Dataset — build_enriched_dataset.py

Before running the simulator, you need to build the enriched dataset. This script:

1. **Samples 20 matches** randomly from `game_events.csv` (configurable via `SAMPLE_SIZE`)
2. **Joins 7 tables:** events + games + players + clubs + competitions + lineups + club_games
3. **Resolves all column conflicts** before merging (renames `type`, `date`, `name`, `position` etc. to unambiguous names)
4. **Outputs a flat 35-column CSV** ready for the simulator

```bash
python pipeline/build_enriched_dataset.py
# ✅ Done!
# Rows   : ~2,000
# Columns: 35
# Saved  : datasets/raw_data/football_events_enriched2.csv
```

To use a different sample or different random seed, edit `SAMPLE_SIZE` and `RANDOM_STATE` at the top of the script.

---

## 🔗 Part of FootballFlow

This streaming pipeline is the **speed layer** of the larger FootballFlow platform, which implements a full Lambda Architecture:

| Layer | Repo / Component |
|---|---|
| **Batch** | Bronze (S3) → Silver (Spark) → Gold (Snowflake + dbt) |
| **Speed** | **This repo** — Kafka → Spark Streaming → InfluxDB → Grafana |
| **Serving** | Power BI (batch) + Grafana (real-time) |

Streamed events are also synced from InfluxDB into Snowflake's `RAW.STREAMING` schema, where `fact_game_events` unions them with batch-loaded events (`source_type = 'stream'`), so Power BI reflects both historical and live match data.

---

## ⚠️ Important Notes

- **`config.py` is gitignored** — never commit credentials. Use `config.py.example` as the template.
- **Raw CSV files are gitignored** — download from [Kaggle: davidcariboo/player-scores](https://www.kaggle.com/datasets/davidcariboo/player-scores) and place in `datasets/raw_data/`.
- **Kafka KRaft** requires Docker Desktop. The `CLUSTER_ID` is pre-set — no manual `kafka-storage format` step needed.
- **Spark container** runs as root (`user: root`) with sudo enabled for notebook package installs.

---

<div align="center">

**FootballFlow Streaming** — *The speed layer of the beautiful game.*

⚡ Built with Python · Kafka · Spark · InfluxDB · Grafana · Docker

</div>
