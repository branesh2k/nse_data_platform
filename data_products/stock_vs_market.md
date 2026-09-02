# stock_vs_market (gold)

Daily relative return of every NSE stock vs the Nifty 50 index.  
One row per (symbol, trade_date).

- Owner: branesh
- Domain: NSE Equity
- Layer: gold

## Sources

- silver.equity - same trade_date as run_date
- silver.index - filtered to Nifty 50, current and prior trade_date

Full lineage per run: `metadata/lineage/lineage_events`,  
filtered to `job_name = stock_vs_market`

## Freshness

Runs daily via Airflow DAG `stock_index_relative_dag`.  
Per-run status and row count: `metadata/operational/job_runs`, filtered to `table_name = stock_vs_market`.  
No row for a date means the run didn't happen.  
Table is stale for that date, not zero-filled.

## Guarantees

- Unique on (symbol, trade_date)
- relative_return is all-null (no prior index data) or none-null (normal run), never partial

Enforced in code: `check_unique_grain`, `check_relative_return_nullness` in `quality/checks.py`.  
Contract file `contracts/gold_stock_vs_market.yaml` documents this, doesn't enforce it.

## Schema

| column                         | type   | notes                                               |
| ------------------------------ | ------ | --------------------------------------------------- |
| symbol                         | string | NSE trading symbol                                  |
| series                         | string | e.g. EQ, BE                                         |
| trade_date                     | date   | join key, not null                                  |
| total_traded_qty               | long   |                                                     |
| total_traded_value             | double |                                                     |
| total_trades                   | long   |                                                     |
| isin                           | string |                                                     |
| stock_open/high/low/close/last | double | from equity silver                                  |
| stock_prev_close               | double | from equity silver                                  |
| stock_return                   | double | (stock_close - stock_prev_close) / stock_prev_close |
| index_name                     | string | always "Nifty 50"                                   |
| volume                         | long   | from index silver                                   |
| turnover_in_rs_cr              | double |                                                     |
| pe / pb / div_yield            | double |                                                     |
| index_open/high/low/close      | double | from index silver                                   |
| index_prev_close               | double | null if no prior date exists                        |
| index_return                   | double | null on first trading day in history                |
| relative_return                | double | core metric: stock_return - index_return            |

## How it's built

Equity silver left-joined to Nifty-50-filtered index silver on trade_date. Every stock on a date shares the same index row by design.  
index_return uses the prior available index date via `find_prev_silver_date` (handles weekends/holidays).

Job: `src/nse_platform/jobs/gold/stock_vs_market_gold_job.py`
