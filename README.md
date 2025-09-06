# What is this Repo?

This repository serves as a simple template for ingesting and exploring data from the NYC Socrata API.

The contents of this repo can be easily copied and sent to an LLM with https://github.com/ChristianCasazza/codeprinter
 
# Project Setup Guide

This project assumes you are using a code IDE, such as [VSCode](https://code.visualstudio.com/docs/setup/setup-overview). Use [this guide](https://github.com/theopendatastackofficial/get-started-coding-locally) to you get setup your computer with VSCode(free code interface), WSL(for windows users), uv(for python) bun(for js/ts) and git/Github(for saving and sharing code).

## 1. Clone the Repository

In VSCode, at the top of the screen, you will see an option that says **Terminal**. Then, you should click the button, and then select **New Terminal**. Then, copy and paste the following commands to git clone the repo.

To clone the repository, run the following command:

```bash
git clone https://github.com/theopendatastackofficial/nyc-mta-data 
```

You can also make the repo have a custom name by adding it at the end:

```bash
git clone https://github.com/ChristianCasazza/mtadata custom_name
```

In VSCode, on the left side of the screen, there will be a vertical bar, and the icon with two pieces of paper will be called **Explorer**. By clicking on this, you can open click **open folder** and then it will be called mtadata or your custom name. 

## 2. Setup the Project

You will need a Socrata API key. Please use your own key if possible. You can obtain one in two minutes by signing up [here](https://evergreen.data.socrata.com/signup) and following [these instructions](https://support.socrata.com/hc/en-us/articles/210138558-Generating-App-Tokens-and-API-Keys). You want to obtain your Socrata App token, at the bottom. Keep this handy to copy and paste in the rest of this step.

This repository includes two setup scripts:
- **`setup.sh`**: For Linux/macOS
- **`setup.bat`**: For Windows

These scripts automate the following tasks:
1. Create and activate a virtual environment using `uv`.
2. Install project dependencies.
3. Ask for your Socrata App Token (`SOCRATA_API_TOKEN`). If no key is provided, the script will use the community key: `uHoP8dT0q1BTcacXLCcxrDp8z`.
   - **Important**: The community key is shared and rate-limited, please get your own token.
4. Creates a `.env` and appends `SOCRATA_API_TOKEN`, `WAREHOUSE_PATH`(to tell DBT where your DuckDB file is) and `DAGSTER_HOME`(for your logs) to the .env file.
- **Note**: Your personal .env file won't appear in Github because of its presence in the .gitignore file
5. Start the Dagster development server.

In your VSCode window, click **Terminal** at the top, and then **New Terminal**. Then, copy and paste the following command: 

#### On Linux/macOS:
```bash
./setup.sh
```

If you encounter a `Permission denied` error, ensure the script is executable by running:
```bash
chmod +x setup.sh
```

#### On Windows:
```cmd
setup.bat
```

If PowerShell does not recognize the script, ensure you're in the correct directory and use `.\` before the script name:
```powershell
.\setup.bat
```

The script will guide you through the setup interactively. Once complete, your `.env` file will be configured, and the Dagster server will be running.

## 3. Access Dagster

After the setup script finishes, you can access the Dagster web UI. The script will display a URL in the terminal. Click on the URL or paste it into your browser to access the Dagster interface.

## 4. Materialize Assets

1. In the Dagster web UI, click on the **Assets** tab in the top-left corner.
2. Then, in the top-right corner, click on **View Global Asset Lineage**.
3. In the top-right corner, click **Materialize All** to start downloading and processing all of the data.

This will besgin the ingestion pipeline for all of the sample assets.

# Using `superscrape.py` to scrape NYC Socrata Data Dictionary Information


### Overview
`superscrape.py` connects to Socrata-powered open data portals. Given either a **CSV file of datasets** or a **single dataset name and URL**, it generates:

- JSON metadata (`scraped_outputs/json/`)
- Python stub assets (`scraped_outputs/py/`) you can plug into your Dagster pipeline.
- By leveraging uv, we don't need to worry about venvs

### Usage Modes

#### 1. CSV Mode (default)
Provide a CSV with headers `asset_name,url`.

```bash
# Input CSV → default output under ./scraped_outputs
uv run superscrape.py assets.csv

# Input CSV → custom output dir
uv run superscrape.py assets.csv out_dir
```

#### 2. Single Asset Mode
Provide an asset name and Socrata dataset URL directly:

```bash
# Output defaults to ./scraped_outputs
uv run superscrape.py mta_daily_ridership https://data.ny.gov/Transportation/MTA-Daily-Ridership-Data-2020-2025/vxuj-8kew
```

This will create:
- `scraped_outputs/json/mta_daily_ridership.json`
- `scraped_outputs/py/mta_daily_ridership.py`

### Output Layout
```
scraped_outputs/
├── json/
│   └── mta_daily_ridership.json
└── py/
    └── mta_daily_ridership.py

Simply drop the generated .py file into pipeline/defs/assets to add the new dataset to your pipeline.

# Querying the data for Ad-hoc analysis

## Querying the data with the Harlequin SQL Editor

### Step 1: Activating Harlequin

Harlequin is a terminal based local SQL editor.

To start it, open a new terminal, then, run the following command to use the Harlequin SQL editor with uv:


```bash
uvx harlequin
```
Then use it to connect to the duckdb file we created during our pipeline.

```bash
harlequin app/sources/app/data.duckdb
```

### Step 2: Query the Data

The duckdb file will already have the views to the tables to query. it can be queried like

```sql
SELECT 
    COUNT(*) AS total_rows,
    MIN(transit_timestamp) AS min_transit_timestamp,
    MAX(transit_timestamp) AS max_transit_timestamp
FROM mta_hourly_subway_ridership
```

This query will return the total number of rows, the earliest timestamp, and the latest timestamp in the dataset.

## Working in with mydatabrowser.com

One great option for visualizing and querying datasets of less than a few million rows is going to mydatabrowser.com. Simply drag and drop or upload the files to view them and query with SQL.


---

## 🗄️ `w.py`: Build DuckDB Warehouse

### Overview
`w.py` reads a `datasets.yaml` file at your repo root that defines:

- Paths to your `clean/`, `raw/`, and `analytics/` lakes
- A list of dataset names (matching folder names in those lakes)

It then **auto-detects partitioning** (flat vs `year=` vs `year=/month=`) and registers every dataset as a DuckDB `VIEW` (or `TABLE`). So as you add new datasets to your pipeline, simply add the name to the datasets.yaml.

### Installation
Install dependencies:

```bash
uv add duckdb pyyaml
```

### Example `datasets.yaml`
```yaml
paths:
  clean:      data/opendata/clean
  raw:        data/opendata/raw
  analytics:  data/opendata/analytics
  warehouse:  data/duckdb/data.duckdb

search_order: [analytics, clean, raw]   # optional

assets:
  - mta_daily_ridership
  - mta_operations_statement
  - mta_subway_hourly_ridership
  - nyc_dob_safety_violations
  - nyc_nypd_arrests
  - nyc_nypd_arrests_historical
  - nyc_nypd_arrests_ytd
```

### Usage

```bash
# Default: build views, wipe old warehouse
uv run python w.py -v

# Dry run: show what would be registered
uv run python w.py --list

# Keep existing warehouse file, just update views
uv run python w.py --no-wipe -v

# Create TABLEs instead of VIEWs
uv run python w.py --as-tables -v

# Restrict to specific assets
uv run python w.py --only mta_subway_hourly_ridership -v

# Exclude assets by substring
uv run python w.py --exclude arrests -v
```

### Output
By default, the warehouse is written to `data/duckdb/data.duckdb` (from `datasets.yaml`). It contains:

- One view per dataset (`CREATE OR REPLACE VIEW asset_name AS …`)
- A `registry__assets` table with asset name, glob pattern, and detected kind (flat, hive_yearly, hive_monthly, etc.)


