# SQL Server ETL Mini Data Warehouse (WideWorldImporters → DW)
A single-file T-SQL ETL project demonstrating **incremental loads**, **SCD2-style dimensions**, and **ETL observability (tracking + run logs)**.

> Companion “why this matters” project (Power BI + analysis):
https://github.com/DavidFarm/temporal-credit-analysis

---

## Why this repo exists
This repo is the **implementation** behind a classic analytics workflow:
1) ingest/source views  
2) build a DW core (dims + facts)  
3) publish BI-friendly mart views  
4) run repeatedly with logging + watermarks

It’s written as a structured, staged script (≈1800 lines) intended to be easy to review.

---

## What this demonstrates (portfolio highlights)
- **Incremental loads** using a watermark pattern (`etl_load_tracker`)
- **SCD2-style dimensions** with `active_from`, `active_to`, `is_current`
- **Run logging + error handling** (`etl_run_log`, TRY/CATCH, transactions)
- **Star schema**: DimCustomer / DimProduct / DimSalesPerson / DimDate → FactSales
- **Validation & sanity checks** after execution

---

## Architecture
![Architecture](docs/images/arch.png)

---

## Proof it runs (ETL tracking + run logs)
![Load tracker + run logs](docs/images/Runlogs.png)

---

## Code structure (stages / how to navigate)
The script is organized into numbered stages.

**Start here (5-minute tour):**
1. **Stage 3** – incremental ETL stored procedures (`etl_load_dim*`, `etl_load_factsales`)
2. **Stage 1.4 / 1.9** – ETL metadata: `etl_load_tracker` + `etl_run_log`
3. **Stage 5 / Stage X** – validations + sanity checks

### Stages 1–3
![Stages 1–3](docs/images/toc_part1.png)

### Stages 3–X
![Stages 3–X](docs/images/toc_part2.png)

---

## How to run (10 minutes)
### Prerequisites
- SQL Server
- WideWorldImporters sample database installed on the same instance

### Run
1. Open and execute:
   - `code/SQL_1_Assignment_2_David_Färm.sql`
2. Re-run the ETL procedures to validate incremental behavior.
3. Inspect logs:
   - `davidf_int.etl_run_log`
   - `davidf_int.etl_load_tracker`

---

## Key components (quick links)
- **SQL script:** [`code/SQL_1_Assignment_2_David_Färm.sql`](code/SQL_1_Assignment_2_David_Färm.sql)
- **ETL procedures:** `etl_load_dimcustomer`, `etl_load_dimsalesperson`, `etl_load_dimproduct`, `etl_load_factsales`
- **Metadata tables:** `davidf_int.etl_load_tracker`, `davidf_int.etl_run_log`
- **Schemas:** `davidf_staging`, `davidf_int`, `davidf_mart`
