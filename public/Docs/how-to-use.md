# How to Use

## Start / Stop / Restart
```bash
cd /home/che/LocalAppStore/Apps/recon-matching
./start.sh    # Starts on port 4017
./stop.sh     # Stops
./restart.sh  # Stop + start
```

## Access
- **Dashboard**: http://10.219.31.248:4017

## Running a Reconciliation

### Via UI
1. Open the dashboard
2. On the **Run** tab, fill in Workspace (`asset`) and Ruleset (`account`)
3. Dataset path auto-fills, or enter a custom absolute path
4. Click **Execute Full Run**
5. Wait ~6 seconds — results appear with summary stats and waterfall breakdown

### Via API
```bash
curl -X POST http://10.219.31.248:4017/api/recon-match/execute \
  -H 'Content-Type: application/json' \
  -d '{
    "workspace": "asset",
    "ruleset": "account",
    "dataset_absolute_path": "/path/to/dataset.csv",
    "run_id": "my_run_001"
  }'
```

## Viewing Results

### Results Tab
1. Click **Results** tab
2. Click **Load Runs** — shows all runs grouped by date
3. Click a run to see its output files
4. Click any file to open in the viewer:
   - **CSV files** (matches.csv, breaks.csv, exclusions.csv) → rendered as paginated tables
   - **JSON files** (reasoning, manifest, profile) → syntax highlighted
   - **SQL file** (matching_queries.sql) → keyword highlighted

### Agent Logs Tab
1. Click **Agent Logs** tab
2. Click **Load Runs** → select a run
3. See the full interaction timeline:
   - **Orchestrator** (cyan): decisions, dispatch, stop conditions
   - **Executor** (purple): requests, pool state, initialization
   - **DuckDB** (yellow): SQL queries, operator results
   - **Boot** (green): pipeline initialization
   - **Finalize** (gray): CSV export, manifest seal
4. Use filter buttons to show only specific actors or levels
5. Click any detail block to expand/collapse

### Knowledge Base Tab
1. Click **Knowledge Base** tab
2. Enter Workspace + Ruleset → click **Load**
3. Click any file to read:
   - JSON files: schema_structure.json, core_matching_rules.json, features.json
   - Markdown files: identity.md, soul.md, how_to_work.md

## Understanding the Output

### Summary Stats
- **Excluded**: records removed before matching (noise, collateral, derivatives)
- **Matched**: records paired or grouped by matching operators
- **Breaks**: unmatched records remaining after all waterfall steps
- **Total**: excluded + matched + breaks = input records (always balanced)

### Waterfall Steps
Each step shows rule_id, operator type, records removed, and a progress bar. Steps execute in order: all exclusions first, then matching operators.

### Run Manifest
`run_manifest.json` is the canonical audit record. Contains: input row counts, KB hashes, waterfall step details (timing, pool before/after), and final summary.

## Running Tests
```bash
cd /home/che/LocalAppStore/Apps/recon-matching/versions/v1/backend
/home/che/anaconda3/bin/python -m pytest tests/ -v
# 101 tests
```

## API Reference

| Method | Path | Purpose |
|--------|------|---------|
| GET | /api/health | Health check |
| POST | /api/recon-match | Create run (scaffold only) |
| POST | /api/recon-match/execute | Full run (boot+waterfall+finalize) |
| GET | /api/runs | List all runs |
| GET | /api/runs/{run_id} | Get run status |
| GET | /api/lro/{op_id} | Get LRO status |
| GET | /api/kb/{ws}/{rs} | KB validation status |
| GET | /api/kb/{ws}/{rs}/files | List KB files |
| GET | /api/kb/{ws}/{rs}/file/{name} | Read KB file |
| GET | /api/outputs | List output dates |
| GET | /api/outputs/{date}/{ws}/{rs} | List runs for date |
| GET | /api/outputs/{date}/{ws}/{rs}/{rid}/files | List run files |
| GET | /api/outputs/{date}/{ws}/{rs}/{rid}/file/{name} | Read run file (CSV: ?offset=&limit=) |
