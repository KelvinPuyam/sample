"""
BMW Global Sales — ETL Pipeline
================================
Reads a CSV, cleans it, and upserts into PostgreSQL.

Re-runnable: drop a newer CSV and run again — new rows are inserted,
existing (year, month, region, model) rows are updated in place.

Usage:
    python etl.py --csv path/to/file.csv

    # Custom connection:
    python etl.py --csv data/bmw_global_sales_2018_2025.csv \
                  --db-url postgresql://user:pass@localhost:5432/bmw_sales

    # Or set env var:
    DATABASE_URL=postgresql://user:pass@localhost:5432/bmw_sales
"""

import argparse
import logging
import os
import sys
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema DDL  (1 table)
# ---------------------------------------------------------------------------

DDL = """
CREATE TABLE IF NOT EXISTS sales_fact (
    id                SERIAL        PRIMARY KEY,
    year              SMALLINT      NOT NULL,
    month             SMALLINT      NOT NULL,
    region            VARCHAR(50)   NOT NULL,
    model             VARCHAR(50)   NOT NULL,
    units_sold        INTEGER       NOT NULL,
    avg_price_eur     NUMERIC(12,2) NOT NULL,
    revenue_eur       BIGINT        NOT NULL,
    bev_share         NUMERIC(6,4)  NOT NULL,
    premium_share     NUMERIC(6,4)  NOT NULL,
    gdp_growth        NUMERIC(6,4)  NOT NULL,
    fuel_price_index  NUMERIC(6,4)  NOT NULL,
    UNIQUE (year, month, region, model)
);

CREATE INDEX IF NOT EXISTS idx_sf_year        ON sales_fact (year);
CREATE INDEX IF NOT EXISTS idx_sf_region      ON sales_fact (region);
CREATE INDEX IF NOT EXISTS idx_sf_model       ON sales_fact (model);
CREATE INDEX IF NOT EXISTS idx_sf_year_region ON sales_fact (year, region);
CREATE INDEX IF NOT EXISTS idx_sf_year_model  ON sales_fact (year, model);
"""

# ---------------------------------------------------------------------------
# Clean
# ---------------------------------------------------------------------------

def clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]

    required = {
        "Year", "Month", "Region", "Model", "Units_Sold",
        "Avg_Price_EUR", "Revenue_EUR", "BEV_Share",
        "Premium_Share", "GDP_Growth", "Fuel_Price_Index",
    }
    missing = required - set(df.columns)
    if missing:
        log.error("CSV missing columns: %s", missing)
        sys.exit(1)

    df["Year"]             = df["Year"].astype(int)
    df["Month"]            = df["Month"].astype(int)
    df["Units_Sold"]       = df["Units_Sold"].astype(int)
    df["Avg_Price_EUR"]    = df["Avg_Price_EUR"].astype(float).round(2)
    df["Revenue_EUR"]      = df["Revenue_EUR"].astype(float).round(0).astype(int)
    df["BEV_Share"]        = df["BEV_Share"].astype(float).clip(0, 1).round(4)
    df["Premium_Share"]    = df["Premium_Share"].astype(float).clip(0, 1).round(4)
    df["GDP_Growth"]       = df["GDP_Growth"].astype(float).round(4)
    df["Fuel_Price_Index"] = df["Fuel_Price_Index"].astype(float).round(4)
    df["Region"]           = df["Region"].str.strip()
    df["Model"]            = df["Model"].str.strip()

    before = len(df)
    df = df.drop_duplicates(subset=["Year", "Month", "Region", "Model"])
    dropped = before - len(df)
    if dropped:
        log.warning("Dropped %d duplicate rows from CSV.", dropped)

    bad_months = df[~df["Month"].between(1, 12)]
    if not bad_months.empty:
        log.error("Invalid month values:\n%s", bad_months)
        sys.exit(1)

    log.info("Clean shape: %s", df.shape)
    return df

# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------

def load(df: pd.DataFrame, conn) -> None:
    cur = conn.cursor()

    rows = [
        (
            int(r.Year), int(r.Month), r.Region, r.Model,
            int(r.Units_Sold), float(r.Avg_Price_EUR), int(r.Revenue_EUR),
            float(r.BEV_Share), float(r.Premium_Share),
            float(r.GDP_Growth), float(r.Fuel_Price_Index),
        )
        for r in df.itertuples(index=False)
    ]

    execute_values(
        cur,
        """
        INSERT INTO sales_fact
            (year, month, region, model, units_sold, avg_price_eur,
             revenue_eur, bev_share, premium_share, gdp_growth, fuel_price_index)
        VALUES %s
        ON CONFLICT (year, month, region, model) DO UPDATE SET
            units_sold        = EXCLUDED.units_sold,
            avg_price_eur     = EXCLUDED.avg_price_eur,
            revenue_eur       = EXCLUDED.revenue_eur,
            bev_share         = EXCLUDED.bev_share,
            premium_share     = EXCLUDED.premium_share,
            gdp_growth        = EXCLUDED.gdp_growth,
            fuel_price_index  = EXCLUDED.fuel_price_index
        """,
        rows,
        page_size=500,
    )

    conn.commit()
    cur.close()
    log.info("sales_fact: upserted %d rows.", len(rows))

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run(csv_path: Path, db_url: str) -> None:
    log.info("=== BMW Sales ETL ===")
    log.info("Source : %s", csv_path)
    log.info("Target : %s", db_url.split("@")[-1])  # hide credentials in log

    df = clean(pd.read_csv(csv_path))

    conn = psycopg2.connect(db_url)
    try:
        cur = conn.cursor()
        cur.execute(DDL)
        conn.commit()
        cur.close()
        log.info("Schema ready.")

        load(df, conn)
    finally:
        conn.close()

    log.info("=== ETL complete ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BMW Sales ETL → PostgreSQL")
    parser.add_argument(
        "--csv",
        default="data/bmw_global_sales_2018_2025.csv",
        help="Path to source CSV",
    )
    parser.add_argument(
        "--db-url",
        default=os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/bmw_sales"),
        help="PostgreSQL connection URL (or set DATABASE_URL env var)",
    )
    args = parser.parse_args()
    run(Path(args.csv), args.db_url)

# python data_processing/etl.py --csv data_processing/data/bmw_global_sales_2018_2025.csv --db-url postgresql://postgres:Root%40123@localhost:5432/bmw_sales