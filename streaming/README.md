<img width="2151" height="731" alt="logo_png" src="https://github.com/user-attachments/assets/56da4330-ef36-4ca7-9b69-96ab685235e9" />

<div align="center">

**Real-Time Football Analytics & Live Match Intelligence**

*Speed Layer · Lambda Architecture · Kafka × Spark × InfluxDB × Grafana*

<br/>

<table>
  <tr>
    <td align="center">
      <img width="60" height="60" alt="kafka" src="https://github.com/user-attachments/assets/0c9f9b8f-077f-4b2d-82e9-a38f6cb3ca3c" /><br/>
      <sub><b>Kafka</b></sub>
    </td>
    <td align="center">
      <img width="80" height="60" alt="spark" src="https://github.com/user-attachments/assets/e9d3ad99-76db-4845-bdc6-1bc32ed8bdf5" /><br/>
      <sub><b>Spark</b></sub>
    </td>
    <td align="center">
      <img width="65" height="63" alt="influxdb" src="https://github.com/user-attachments/assets/e8efcf74-d493-4e9f-ac1e-ef5bf8c07663" /><br/>
      <sub><b>InfluxDB</b></sub>
    </td>
    <td align="center">
      <img width="60" height="60" alt="grafana" src="https://github.com/user-attachments/assets/51943e84-3778-4de3-bbc8-ed7d557d1083" /><br/>
      <sub><b>Grafana</b></sub>
    </td>
    <td align="center">
      <img width="65" height="63" alt="docker" src="https://github.com/user-attachments/assets/825a0362-e9c7-44dd-ab36-d544dbda321a" /><br/>
      <sub><b>Docker</b></sub>
    </td>
  </tr>
</table>

</div>

---

## 🔭 Overview

This repository contains the **Streaming Pipeline** of the **FootballFlow** data engineering platform — the speed layer of a Lambda Architecture that brings football matches to life in real time.

A Python GUI simulator replays real match events from the Transfermarkt dataset through **Apache Kafka**. **Spark Structured Streaming** consumes, validates, and enriches those events across three internal topics. Processed metrics land in **InfluxDB** and surface instantly on live **Grafana** dashboards — all while the match is still being simulated.

> Built as part of the **Data Engineering Track graduation project at ITI (Information Technology Institute)**.

### ✨ Key Highlights

| Feature | Description |
|---|---|
| **Live Match Simulator** | Python + Tkinter GUI replays real game events through Kafka in real time |
| **4 Simulation Speeds** | x5 · x10 · x18 · x30 — from 18 minutes down to 3 minutes per match |
| **3-Stage Stream Processing** | Raw Events → Validated Events → Analytics Preparation |
| **35+ Column Enriched Events** | Player info, club stats, competition metadata, lineup details per event |
| **Time-Series Storage** | InfluxDB 2.7 stores match metrics by minute |
| **Live Grafana Dashboard** | Goals · Cards · Substitutions · Live Score — all updating in real time |
| **Full Docker Setup** | One `docker-compose up -d` spins up the entire infrastructure |

---

## 🏗️ Architecture

![Stream Pipeline](docs/stream_pipeline.png)

```
football_events_enriched2.csv
           │
           ▼
  ┌─────────────────────┐
  │   Match Simulator   │  ← Tkinter GUI · speeds: x5 / x10 / x18 / x30
  │  match_simulator.py │    Goals · Cards · Substitutions · Shootout
  └────────┬────────────┘
           │  JSON → topic: real_match_events
           ▼
  ┌─────────────────────┐
  │   Apache Kafka      │  ← KRaft mode (no Zookeeper) · 3 partitions
  │   (KRaft 7.6.1)     │    Kafka UI at :8081
  └────────┬────────────┘
           │
           ▼
  ┌──────────────────────────────────────────────┐
  │         Spark Structured Streaming           │
  │                                              │
  │  raw_events → validated_events →             │
  │               analytics_preparation          │
  └────────┬─────────────────────────────────────┘
           │
           ▼
  ┌─────────────────────┐
  │     InfluxDB 2.7    │  ← Bucket: match_events · 7d retention
  └────────┬────────────┘
           │
    ┌──────┴──────────┐
    ▼                 ▼
 Grafana          Snowflake
(:3000)          (optional sync)
```

---

## 🛠️ Tech Stack

![Tech Stack & Infrastructure](docs/stream_in_blocks.png)

| Layer | Tool | Detail |
|---|---|---|
| **Simulator** | Python 3.11 + Tkinter | GUI app — replays match events minute by minute |
| **Message Broker** | Apache Kafka (KRaft) | Confluent 7.6.1 · topic: `real_match_events` · 3 partitions |
| **Kafka UI** | Provectus Kafka UI | Topic browser & consumer groups at `:8081` |
| **Stream Processing** | Apache Spark 3.5.3 | Structured Streaming — 3-stage internal pipeline |
| **Time-Series DB** | InfluxDB 2.7 | Bucket: `match_events` · real-time match metrics |
| **Live Dashboards** | Grafana | Live score · goals · cards · subs at `:3000` |
| **Containerisation** | Docker + Docker Compose | Full stack with one command |
| **Dataset** | Transfermarkt via Kaggle | 7 CSV tables joined into 35-column enriched dataset |

---

## ⚙️ Docker Services

| Container | Image | Ports | Purpose |
|---|---|---|---|
| `footballflow-kafka` | `confluentinc/cp-kafka:7.6.1` | 9092 · 9093 · 9094 | Kafka broker — KRaft mode |
| `footballflow-kafka-ui` | `provectuslabs/kafka-ui:latest` | 8081 | Topic browser |
| `footballflow-spark` | `quay.io/jupyter/all-spark-notebook:spark-3.5.3` | 8888 · 4040 | Spark + Jupyter |
| `footballflow-influxdb` | `influxdb:2.7` | 8086 | Time-series storage |
| `footballflow-grafana` | `grafana/grafana:latest` | 3000 | Live dashboards |

---

## 📁 Repository Structure

```
Football-Analytics-Live-Match-Intelligence/
│
├── 📄 docker-compose.yml              ← Full infrastructure: Kafka + Spark + InfluxDB + Grafana
├── 📄 config.py.example               ← Credentials template (copy → config.py, never commit)
│
├── 📂 datasets/
│   └── raw_data/
│       └── football_events_enriched2.csv   ← Enriched input for the simulator (35+ columns)
│
├── 📂 pipeline/
│   ├── build_enriched_dataset.py      ← Joins 7 CSVs → produces football_events_enriched2.csv
│   └── verify_kafka.py                ← Health check: confirms Kafka broker is reachable
│
├── 📂 simulator/
│   └── match_simulator.py             ← Main GUI app — select match, set speed, stream live
│
├── 📂 notebooks/
│   ├── 01_stream_ingest.ipynb         ← Read raw events from Kafka topic
│   ├── 02_stream_process.ipynb        ← Spark Structured Streaming: validate + enrich
│   └── 03_stream_sink_influxdb.ipynb  ← Write processed events to InfluxDB
│
├── 📂 scripts/exploration/
│   ├── check_csv.py                   ← Inspect CSV schemas and row counts
│   ├── clean_nulls_colums.py          ← Identify and handle null columns
│   ├── dataQ.py                       ← Data quality checks on raw events
│   ├── download_csv_files.py          ← Download source CSVs from Kaggle
│   ├── merge_all_csv.py               ← CSV joining experiments
│   └── test_bronze.py                 ← Validate source files
│
└── 📂 docs/
    ├── footballflow_dashboard3.json   ← Grafana dashboard export (import-ready)
    ├── stream_pipeline.png            ← End-to-end pipeline diagram
    ├── stream_in_blocks.png           ← Tech stack & infrastructure overview
    └── grafana_dashboard.png          ← Live dashboard screenshot
```

---

## 🚀 Getting Started

### Prerequisites

| Tool | Version |
|---|---|
| Docker Desktop | Latest |
| Python | 3.10 or 3.11 |
| Java JDK | 11 (required by Spark) |

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

Run once to produce the simulator's input file:

```bash
python pipeline/build_enriched_dataset.py
```

This joins 7 Transfermarkt CSV tables (events · games · players · clubs · competitions · lineups · club_games) into a flat 35-column enriched CSV. Output: `datasets/raw_data/football_events_enriched2.csv`

```
✅ Done!
Rows   : ~2,000
Columns: 35
```

### 5. Start all services

```bash
docker-compose up -d
```

| URL | Service |
|---|---|
| http://localhost:8081 | Kafka UI |
| http://localhost:8888 | Jupyter (Spark) |
| http://localhost:4040 | Spark UI |
| http://localhost:8086 | InfluxDB |
| http://localhost:3000 | Grafana |

Confirm Kafka is ready:

```bash
python pipeline/verify_kafka.py
```

### 6. Import the Grafana dashboard

1. Open Grafana at `http://localhost:3000` (admin / footballflow123)
2. Go to **Dashboards → Import**
3. Upload `docs/footballflow_dashboard3.json`
4. Set InfluxDB as the data source (token: `footballflow-token`)

### 7. Run the Spark notebooks

Open Jupyter at `http://localhost:8888` and run in order:

```
01_stream_ingest.ipynb         → connect to Kafka, read raw events
02_stream_process.ipynb        → validate, enrich, windowed aggregations
03_stream_sink_influxdb.ipynb  → write metrics to InfluxDB
```

### 8. Launch the Match Simulator

```bash
python simulator/match_simulator.py
```

1. Enter a **Game ID** from the enriched dataset (first 10 shown automatically)
2. Choose a **simulation speed** — x5 / x10 / x18 / x30
3. Click **▶ START SIMULATION**

The simulator streams each match event as a JSON message to Kafka in real time, while displaying a live scoreboard with dual team feeds.

---

## 🎮 Match Simulator — Deep Dive

The simulator is a full Tkinter GUI with two screens:

**Screen 1 — Match Selection**
- Enter any Game ID → app previews teams, final score, competition, stadium, event count
- Choose simulation speed
- Available Game IDs listed automatically from the dataset

**Screen 2 — Live Simulation**
- Real-time scoreboard with home/away scores
- Live match clock with progress bar (supports Extra Time up to 120')
- Per-team event feeds scrolling in real time
- Stats counters: goals · cards · substitutions
- Kafka connection status indicator

**Event classification** — inferred from the `description` field:

| Type | Keywords detected |
|---|---|
| `goals` | goal · scored · shot · header · penalty · free kick · tap-in · own-goal |
| `cards` | yellow card · second yellow · red card |
| `substitutions` | everything else |

**Kafka message** — each event is sent as JSON with 35+ enriched fields plus metadata:

```json
{
  "game_id": 12345,
  "minute": 67,
  "event_type": "goals",
  "player_name": "Georges Mikautadze",
  "club_name": "Football Club de Metz",
  "home_club_goals": 2,
  "away_club_goals": 1,
  "competition_name": "Ligue 1",
  "stadium": "Stade Saint-Symphorien",
  "_ingested_at": "2026-06-19T10:00:00",
  "_source": "match_simulator_v2",
  "_pipeline": "footballflow_streaming"
}
```

---

## 📊 Grafana — Live Dashboard

![Grafana Live Dashboard](docs/grafana_dashboard.png)

The live dashboard updates in real time as the simulator streams events:

| Panel | Description |
|---|---|
| **Live Score** | Current score per team — large display |
| **Goals Feed** | Goal minute + scorer name per team |
| **Cards Feed** | Card type · minute · player per team |
| **Substitutions Feed** | Minute · player out per team |

InfluxDB credentials (defaults):
- **URL:** `http://localhost:8086`
- **Org:** `footballflow`
- **Bucket:** `match_events`
- **Token:** `footballflow-token`

---

## 🔄 Stream Processing — 3 Stages

Spark Structured Streaming processes events through three internal Kafka topics:

```
real_match_events (raw)
        │
        ▼
   raw_events
   Ingest raw JSON from Kafka · parse schema · add ingestion timestamp
        │
        ▼
   validated_events
   Validate fields · clean nulls · cast types · filter invalid minutes
        │
        ▼
   analytics_preparation
   Windowed aggregations · running totals per match · analytics-ready output
        │
        ▼
     InfluxDB
```

---

## ⚠️ Important Notes

- **`config.py` is gitignored** — never commit credentials. Use `config.py.example` as the template.
- **CSV files are gitignored** — download from [Kaggle: davidcariboo/player-scores](https://www.kaggle.com/datasets/davidcariboo/player-scores) and place in `datasets/raw_data/`.
- **Kafka runs in KRaft mode** — no Zookeeper, no manual cluster init. `CLUSTER_ID` is pre-configured in `docker-compose.yml`.
- **Snowflake sync is optional** — streamed events can be copied from InfluxDB into Snowflake's `RAW.STREAMING` schema for Gold layer unioning with batch data.

---

## 🔗 Part of FootballFlow

This is the **speed layer** of the larger FootballFlow platform:

| Layer | Pipeline |
|---|---|
| **Batch** | Bronze (S3) → Silver (Spark) → Gold (Snowflake + dbt) → Power BI |
| **Speed** | **This repo** — Kafka → Spark Streaming → InfluxDB → Grafana |

---

<div align="center">

**FootballFlow Streaming** — *From Live Events to Real-Time Insights.*

⚡ Built with Python · Kafka · Spark · InfluxDB · Grafana · Docker

</div>
