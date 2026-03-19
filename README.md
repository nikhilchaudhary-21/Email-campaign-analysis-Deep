# Email Engagement Analysis — v6

Fetches email engagement data from and optionally joins it with Salesforce High Intent leads. Exports a full Excel report with 15 sheets covering campaigns, domains, prospects, subject lines, and SF lead analysis.

---

## Requirements

- Python 3.8+
- Smartlead API key
- Salesforce credentials (optional)

### Install dependencies

```bash
pip install requests pandas openpyxl simple-salesforce
```

---

## Configuration

Edit these values at the top of the script:

### Smartlead Settings
```python
API_KEY          = "your_smartlead_api_key"
SEED_CAMPAIGN_ID = 2830812   # Script analyzes this campaign + all newer ones
THREADS          = 3         # Parallel threads for API calls
```

### Salesforce Settings (optional)
```python
SF_USERNAME = "your@email.com"
SF_PASSWORD = "yourpassword"
SF_TOKEN    = "your_security_token"   # From SF Settings → Security Token
SF_DOMAIN   = "login"                 # Use "test" for sandbox
```
> Leave SF credentials empty (`""`) to skip Salesforce and run Smartlead-only analysis.

### Date Ranges
```python
DATE_RANGES = [
    ("Jan 1-7",  "2026-01-01", "2026-01-07"),
    ...
]
```
Edit these weekly ranges to match your reporting period.

---

## How to Run

```bash
python3 smartlead_analysis.py
```

---

## How It Works

1. **(Optional)** Connects to Salesforce and fetches all High Intent leads created via Email channel
2. Fetches all Smartlead campaigns created on or after the seed campaign date
3. For each campaign, pulls analytics by date range and full lead statistics
4. Aggregates data across all campaigns per date range
5. Joins Salesforce leads with Smartlead activity (if SF configured)
6. Exports everything to a timestamped Excel file

---

## Output File

Excel file named: `Smartlead_FullAnalysis_YYYYMMDD_HHMMSS.xlsx`

### Smartlead Sheets

| Sheet | Description |
|---|---|
| **1. Summary** | Key metric totals per date range |
| **2. Per Campaign** | Sent, opens, brands, HE prospects per campaign per date range |
| **3. Brands Engaged** | All unique domains reached, prospects count per date range |
| **4. Unique Brand Open** | Domains where at least one prospect opened |
| **5. Prospects Engaged** | All lead emails sent to, per date range |
| **6. Unique Prospects Open** | Leads that opened, with open counts per date range |
| **7. HE Brand-Domain** | High Engagement domains (≥3 opens) per date range |
| **8. HE Prospects** | High Engagement individual leads per date range |
| **9. HE per Company** | HE domains with all HE prospect emails listed |
| **10. Subject Line Analysis** | Subject performance: sends, opens, HE rate across all date ranges |

### Salesforce + Smartlead Join Sheets

| Sheet | Description |
|---|---|
| **11. Lead Summary** | All SF High Intent leads with their Smartlead stats |
| **12. Campaign Breakdown** | Per campaign: SF lead count, HE count, open rates |
| **13. SF Subject Analysis** | Subject line performance for SF leads only |
| **14. HE Sequence Detail** | Row-per-email-sequence for every HE SF lead |
| **15. Not Found in SL** | SF leads with no matching Smartlead activity |

---

## Key Definitions

| Term | Definition |
|---|---|
| **HE (High Engagement)** | Lead with 3 or more email opens |
| **Brands Engaged** | Unique company domains that received at least one email |
| **Unique Brand Open** | Unique domains where at least one prospect opened |
| **Seed Campaign** | The starting campaign — script analyzes this + all campaigns created after it |

---

## Notes

- Script uses rate limiting (7 requests per 2.1 seconds) to avoid Smartlead API throttling
- Excel file is saved at the end — if script crashes mid-run, no partial file is saved
- SF credentials are optional — leave empty to run Smartlead-only analysis
- `THREADS = 3` is recommended — increase carefully to avoid API rate limits

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `simple_salesforce not installed` | Run `pip install simple-salesforce` |
| SF credentials error | Check username, password, security token in SF Settings |
| `429 Too Many Requests` | Reduce `THREADS` to `1` or `2` |
| Seed campaign not found | Double-check `SEED_CAMPAIGN_ID` — it must exist in your Smartlead account |
| Empty sheets in Excel | Check date ranges match your actual campaign send dates |
