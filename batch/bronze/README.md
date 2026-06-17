# 🥉 Bronze Layer — Raw Ingestion to AWS S3

The Bronze layer is the **first stop** in the Football Analytics batch pipeline.
Raw CSV files from Kaggle / Transfermarkt are uploaded **as-is** to S3 — no cleaning, no transformation.

```
S3 Bucket : football-de-2026
Prefix    : bronze/
```

---

## 📁 Folder Structure

```
bronze/
├── check_csv_datasets.py   # Inspect raw CSVs locally before upload
├── upload_to_s3.py         # Upload all 12 raw CSVs to S3 bronze/
├── setup_iam.py            # Create IAM group, users, policies & roles
├── verify_bronze.py        # Confirm all files landed correctly in S3
└── README.md
```

---

## ⚙️ Prerequisites

| Tool | Purpose |
|------|---------|
| AWS Account | Cloud provider access |
| AWS CLI | Configure credentials & interact with AWS from terminal |
| `boto3` | Python SDK to upload files to S3 |
| `pandas` | Quick inspection of CSVs before upload |

```bash
pip install boto3 pandas
```

---

## 🚀 Run Order

### 1 — Inspect Local CSVs

```bash
python check_csv_datasets.py
```

Prints shape, dtypes, null counts, and a 3-row sample for each of the 12 files.

---

### 2 — Configure AWS CLI

```bash
aws configure
```

Enter your **Access Key ID**, **Secret Access Key**, region (e.g. `eu-west-1`), and output format (`json`).

Verify:

```bash
aws configure list
```

---

### 3 — Upload to S3 Bronze

```bash
python upload_to_s3.py
```

Uploads all 12 CSV files to `s3://football-de-2026/bronze/`.
Files are stored raw — no changes made.

---

### 4 — Set up IAM *(run once)*

```bash
python setup_iam.py
```

Creates:

| Resource | Count | Details |
|----------|-------|---------|
| Group | 1 | `football-de-group` |
| Users | 5 | `football-de-abd`, `football-de-ah`, `football-de-fm`, `football-de-ma`, `football-de-mos` |
| Policies | 4 | `FootballDE-S3-Access`, `Football-S3-Pipeline-Policy`, `snowflake_football_int`, `AWSGlueServiceRole-football-EZCRC-s3Policy` |
| Roles | 3 | `AWSGlueServiceRole-football` (Glue), `Football-Pipeline-Execution-Role` (EC2), `Snowflake-football-Access-Role` (Snowflake) |

---

### 5 — Verify Bronze Layer

```bash
python verify_bronze.py
```

Lists all files in `s3://football-de-2026/bronze/` with sizes and upload timestamps.
Reports any missing or unexpected files.

---

## ☁️ AWS Services Used

| Service | Role |
|---------|------|
| **S3** | Cloud storage — bucket `football-de-2026` with `bronze/` and `silver/` prefixes |
| **IAM** | Identity & Access Management — users, groups, policies, roles for team access |
| **AWS CLI** | Unified command-line tool to manage and automate AWS services |