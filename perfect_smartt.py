"""
Smartlead Email Engagement Analysis - v6 FULL MERGED
======================================================
Combines Smartlead v6 drill-down analysis WITH Salesforce High Intent lead joining.

Excel Output:
── Smartlead Sheets ──
  Sheet 1:  Summary                  - Metric totals per date range
  Sheet 2:  Per Campaign             - Per campaign breakdown
  Sheet 3:  Brands Engaged           - All unique domains per date range
  Sheet 4:  Unique Brand Open        - Domains that opened per date range
  Sheet 5:  Prospects Engaged        - All lead emails per date range
  Sheet 6:  Unique Prospects Open    - Leads that opened per date range
  Sheet 7:  HE Brand-Domain          - HE domains per date range
  Sheet 8:  HE Prospects             - HE lead emails per date range
  Sheet 9:  HE per Company           - HE domains (company level) per date range
  Sheet 10: Subject Line Analysis    - Subject performance across all date ranges

── Salesforce + Smartlead Join Sheets ──
  Sheet 11: Lead Summary             - All SF High Intent leads + SL stats
  Sheet 12: Campaign Breakdown       - Per campaign SF lead & HE counts
  Sheet 13: SF Subject Analysis      - Subject performance for SF HE leads only
  Sheet 14: HE Sequence Detail       - Row-per-sequence for every HE SF lead
  Sheet 15: Not Found in SL          - SF leads with no Smartlead match
"""

import requests
import time
import threading
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque, defaultdict

# ─── SMARTLEAD CONFIG ────────────────────────────────────────────────────
API_KEY          = ""
BASE_URL         = "https://server.smartlead.ai/api/v1"
THREADS          = 3
SEED_CAMPAIGN_ID = 2830812   # Mystery Rewards - US — analyze this + all newer

# ─── SALESFORCE CONFIG ───────────────────────────────────────────────────
SF_USERNAME    = ""
SF_PASSWORD    = ""
SF_TOKEN       = ""     # Security token from SF settings
SF_DOMAIN      = "login"       

# ─── DATE RANGES (Smartlead weekly breakdown) ────────────────────────────
DATE_RANGES = [
    ("Jan 1-7",    "2026-01-01", "2026-01-07"),
    ("Jan 8-14",   "2026-01-08", "2026-01-14"),
    ("Jan 15-21",  "2026-01-15", "2026-01-21"),
    ("Jan 22-28",  "2026-01-22", "2026-01-28"),
    ("Jan 29-31",  "2026-01-29", "2026-01-31"),
    ("Feb 1-7",    "2026-02-01", "2026-02-07"),
    ("Feb 8-14",   "2026-02-08", "2026-02-14"),
    ("Feb 15-21",  "2026-02-15", "2026-02-21"),
    ("Feb 22-28",  "2026-02-22", "2026-02-28"),
    ("Mar 1-7",    "2026-03-01", "2026-03-07"),
    ("Mar 8-14",   "2026-03-08", "2026-03-14"),
    ("Mar 15-17",  "2026-03-15", "2026-03-17"),
]
LABELS = [l for l, _, _ in DATE_RANGES]

# ─── SF DATE RANGE (full period for Salesforce query) ───────────────────
DATE_FROM    = "2026-01-01"
DATE_TO      = "2026-03-17"
SF_DATE_FROM = "2026-01-01T00:00:00Z"
SF_DATE_TO   = "2026-03-17T23:59:59Z"
# ─────────────────────────────────────────────────────────────────────────


# ══════════════════════════════════════════════════════════════════════════
# SHARED UTILITIES
# ══════════════════════════════════════════════════════════════════════════

_rl_lock  = threading.Lock()
_rl_times = deque()
MAX_REQ, WINDOW = 7, 2.1

def _wait_for_slot():
    while True:
        with _rl_lock:
            now = time.time()
            while _rl_times and now - _rl_times[0] >= WINDOW:
                _rl_times.popleft()
            if len(_rl_times) < MAX_REQ:
                _rl_times.append(now)
                return
            sleep_until = _rl_times[0] + WINDOW
        time.sleep(max(0, sleep_until - time.time()) + 0.05)

def get_domain(email):
    if not email or "@" not in email:
        return None
    return email.strip().lower().split("@")[-1]

def safe_int(val, default=0):
    try:    return int(val or default)
    except: return default

def safe_str(val):
    if val is None or str(val).strip().lower() in ("none", "null", ""):
        return ""
    return str(val).strip()


# ══════════════════════════════════════════════════════════════════════════
# STEP 1: SALESFORCE — Fetch High Intent Leads
# ══════════════════════════════════════════════════════════════════════════

def fetch_sf_leads():
    """Fetch SF leads: LeadSource=Email, Sub_Channel__c=High Intent."""
    try:
        from simple_salesforce import Salesforce
    except ImportError:
        print("   ⚠️  simple_salesforce not installed — skipping SF step.")
        print("       Run: pip install simple-salesforce")
        return []

    if not SF_USERNAME:
        print("   ⚠️  SF credentials not configured — skipping SF step.")
        return []

    print("🔐 Connecting to Salesforce...")
    sf = Salesforce(
        username=SF_USERNAME,
        password=SF_PASSWORD,
        security_token=SF_TOKEN,
        domain=SF_DOMAIN
    )
    print("   ✅ Connected!")

    query = f"""
        SELECT
            Id, FirstName, LastName, Name, Email, Company,
            LeadSource, Sub_Channel__c, Status, CreatedDate,
            OwnerId, Owner.Name, Title, Phone, Industry,
            Country, ConvertedDate, IsConverted, Rating
        FROM Lead
        WHERE
            LeadSource = 'Email'
            AND Sub_Channel__c = 'High Intent'
            AND CreatedDate >= {SF_DATE_FROM}
            AND CreatedDate <= {SF_DATE_TO}
        ORDER BY CreatedDate ASC
    """

    print(f"📋 Querying SF leads (Email / High Intent / {DATE_FROM} → {DATE_TO})...")
    result  = sf.query_all(query)
    records = result.get("records", [])
    print(f"   Found {len(records)} leads in Salesforce\n")

    leads = []
    for r in records:
        email       = safe_str(r.get("Email")).lower()
        created_raw = safe_str(r.get("CreatedDate"))
        leads.append({
            "sf_id":          safe_str(r.get("Id")),
            "name":           safe_str(r.get("Name")),
            "first_name":     safe_str(r.get("FirstName")),
            "last_name":      safe_str(r.get("LastName")),
            "email":          email,
            "company":        safe_str(r.get("Company")),
            "domain":         get_domain(email),
            "title":          safe_str(r.get("Title")),
            "phone":          safe_str(r.get("Phone")),
            "industry":       safe_str(r.get("Industry")),
            "country":        safe_str(r.get("Country")),
            "lead_source":    safe_str(r.get("LeadSource")),
            "sub_channel":    safe_str(r.get("Sub_Channel__c")),
            "status":         safe_str(r.get("Status")),
            "rating":         safe_str(r.get("Rating")),
            "created_date":   created_raw[:10] if created_raw else "",
            "owner":          safe_str((r.get("Owner") or {}).get("Name")),
            "is_converted":   r.get("IsConverted", False),
            "converted_date": safe_str(r.get("ConvertedDate") or "")[:10],
        })
    return leads


# ══════════════════════════════════════════════════════════════════════════
# STEP 2: SMARTLEAD — API helpers
# ══════════════════════════════════════════════════════════════════════════

def api_get(endpoint, params=None, retries=5):
    url = f"{BASE_URL}/{endpoint}"
    p   = {"api_key": API_KEY}
    if params: p.update(params)
    for attempt in range(retries):
        _wait_for_slot()
        try:
            resp = requests.get(url, params=p, timeout=30)
            if resp.status_code == 429:
                wait = 10 + (attempt * 5)
                print(f"    ⚠️  429 → waiting {wait}s...")
                time.sleep(wait)
                continue
            if resp.status_code in (400, 404): return {}
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.Timeout:
            time.sleep(2)
        except Exception as e:
            if attempt == retries - 1:
                print(f"    ❌ {endpoint}: {e}")
                return {}
            time.sleep(1)
    return {}

def get_analytics_by_date(cid, start, end):
    return api_get(f"campaigns/{cid}/analytics-by-date",
                   {"start_date": start, "end_date": end})

def get_all_statistics(cid, cname="Campaign"):
    rows, offset, limit = [], 0, 100
    while True:
        data = api_get(f"campaigns/{cid}/statistics",
                       {"offset": offset, "limit": limit})
        if not data or not isinstance(data, dict): break
        batch = [r for r in data.get("data", []) if isinstance(r, dict)]
        if not batch: break
        rows.extend(batch)
        total = int(data.get("total_stats", 0) or 0)
        print(f"    📄 {cname[:30]} | offset {offset} → {len(batch)} rows")
        offset += limit
        if offset >= total: break
        time.sleep(0.3)

    # Attach campaign info for SF-join step
    for r in rows:
        r["_campaign_name"] = cname
        r["_campaign_id"]   = cid
    return rows

def get_all_campaigns():
    print("📋 Fetching campaigns...")
    data = api_get("campaigns")
    if not isinstance(data, list):
        print("ERROR fetching campaigns"); exit(1)
    print(f"   Found {len(data)} total campaigns")
    return data

def filter_from_seed(camps, seed_id):
    seed_date = None
    for c in camps:
        if c["id"] == seed_id:
            seed_date = c.get("created_at", "")
            print(f"   Seed: [{seed_id}] '{c['name']}' → {seed_date[:10]}")
            break
    if not seed_date:
        print(f"   ⚠️  Seed {seed_id} not found — using ALL campaigns")
        return camps
    filtered = [c for c in camps if c.get("created_at", "") >= seed_date]
    print(f"   Campaigns to analyze: {len(filtered)}")
    return filtered


# ══════════════════════════════════════════════════════════════════════════
# STEP 3: Per-campaign metrics computation
# ══════════════════════════════════════════════════════════════════════════

def compute_domain_metrics(stats_rows, campaign_name):
    """
    Returns per date range:
    - lead_data, domain_data, he_leads, he_domains, subject_data
    """
    result = {}

    for label, start, end in DATE_RANGES:
        in_range = [
            r for r in stats_rows
            if isinstance(r.get("sent_time"), str)
            and len(r["sent_time"]) >= 10
            and start <= r["sent_time"][:10] <= end
        ]

        lead_data   = defaultdict(lambda: {"domain": None, "opens": 0, "campaigns": set()})
        domain_data = defaultdict(lambda: {
            "prospects_sent": set(), "prospects_opened": set(), "campaigns": set()
        })
        subject_data = defaultdict(lambda: {
            "sends": 0, "total_opens": 0,
            "unique_openers": set(), "unique_leads_sent": set(),
            "he_leads": set(), "campaigns": set(),
        })

        for r in in_range:
            email   = (r.get("lead_email") or "").strip().lower()
            opens   = int(r.get("open_count", 0) or 0)
            domain  = get_domain(email)
            subject = (r.get("email_subject") or "").strip()
            if not email or not domain:
                continue

            lead_data[email]["domain"] = domain
            lead_data[email]["opens"] += opens
            lead_data[email]["campaigns"].add(campaign_name)

            domain_data[domain]["prospects_sent"].add(email)
            domain_data[domain]["campaigns"].add(campaign_name)
            if opens > 0:
                domain_data[domain]["prospects_opened"].add(email)

            if subject:
                sd = subject_data[subject]
                sd["sends"]             += 1
                sd["total_opens"]       += opens
                sd["unique_leads_sent"].add(email)
                sd["campaigns"].add(campaign_name)
                if opens > 0:
                    sd["unique_openers"].add(email)

        # HE leads (opens >= 3)
        he_leads = {e: d for e, d in lead_data.items() if d["opens"] >= 3}

        # Mark HE leads in subject data
        for email in he_leads:
            for sd in subject_data.values():
                if email in sd["unique_leads_sent"]:
                    sd["he_leads"].add(email)

        # HE domains
        he_domains = defaultdict(lambda: {
            "campaigns": set(), "he_prospects": set(), "max_opens": 0
        })
        for email, d in he_leads.items():
            dom = d["domain"]
            he_domains[dom]["campaigns"].add(campaign_name)
            he_domains[dom]["he_prospects"].add(email)
            he_domains[dom]["max_opens"] = max(he_domains[dom]["max_opens"], d["opens"])

        result[label] = {
            "lead_data":    lead_data,
            "domain_data":  domain_data,
            "he_leads":     he_leads,
            "he_domains":   dict(he_domains),
            "subject_data": dict(subject_data),
        }

    return result


def analyze_campaign(campaign):
    cid, cname = campaign["id"], campaign["name"]

    analytics_by_range = {}
    for label, start, end in DATE_RANGES:
        a = get_analytics_by_date(cid, start, end)
        analytics_by_range[label] = {
            "sent_count": int(a.get("sent_count", 0) or 0),
            "open_count": int(a.get("open_count", 0) or 0),
        }
        time.sleep(0.1)

    stats   = get_all_statistics(cid, cname)
    metrics = compute_domain_metrics(stats, cname) if stats else {}

    ranges = {}
    for label, start, end in DATE_RANGES:
        a = analytics_by_range.get(label, {})
        m = metrics.get(label, {})
        ranges[label] = {
            "sent_count":   a.get("sent_count",  0),
            "open_count":   a.get("open_count",  0),
            "lead_data":    m.get("lead_data",   {}),
            "domain_data":  m.get("domain_data", {}),
            "he_leads":     m.get("he_leads",    {}),
            "he_domains":   m.get("he_domains",  {}),
            "subject_data": m.get("subject_data",{}),
        }

    print(f"    ✅ [{cid}] {cname}")
    return {"id": cid, "name": cname, "ranges": ranges, "raw_stats": stats}


# ══════════════════════════════════════════════════════════════════════════
# STEP 4: Global aggregation across all campaigns
# ══════════════════════════════════════════════════════════════════════════

def aggregate_all(results):
    detail = {label: {
        "domains_engaged":  defaultdict(lambda: {"campaigns": set(), "prospects": set()}),
        "domains_opened":   defaultdict(lambda: {"campaigns": set(), "prospects_opened": set()}),
        "leads_sent":       defaultdict(lambda: {"domain": None, "campaigns": set()}),
        "leads_opened":     defaultdict(lambda: {"domain": None, "campaigns": set(), "total_opens": 0}),
        "he_leads":         defaultdict(lambda: {"domain": None, "campaigns": set(), "total_opens": 0}),
        "he_domains":       defaultdict(lambda: {
            "campaigns": set(), "he_prospects": set(), "max_opens": 0
        }),
        "subject_data":     defaultdict(lambda: {
            "sends": 0, "total_opens": 0,
            "unique_openers": set(), "unique_leads_sent": set(),
            "he_leads": set(), "campaigns": set(),
        }),
        "sent_total": 0,
        "open_total": 0,
    } for label in LABELS}

    for cr in results:
        if not cr: continue
        cname = cr["name"]
        for label in LABELS:
            r  = cr["ranges"].get(label, {})
            dl = detail[label]

            dl["sent_total"] += r.get("sent_count", 0)
            dl["open_total"] += r.get("open_count", 0)

            for domain, dd in r.get("domain_data", {}).items():
                dl["domains_engaged"][domain]["campaigns"].add(cname)
                dl["domains_engaged"][domain]["prospects"] |= dd["prospects_sent"]
                if dd["prospects_opened"]:
                    dl["domains_opened"][domain]["campaigns"].add(cname)
                    dl["domains_opened"][domain]["prospects_opened"] |= dd["prospects_opened"]

            for email, ld in r.get("lead_data", {}).items():
                dl["leads_sent"][email]["domain"] = ld["domain"]
                dl["leads_sent"][email]["campaigns"].add(cname)
                if ld["opens"] > 0:
                    dl["leads_opened"][email]["domain"] = ld["domain"]
                    dl["leads_opened"][email]["campaigns"].add(cname)
                    dl["leads_opened"][email]["total_opens"] += ld["opens"]

            for email, hd in r.get("he_leads", {}).items():
                dl["he_leads"][email]["domain"] = hd["domain"]
                dl["he_leads"][email]["campaigns"].add(cname)
                dl["he_leads"][email]["total_opens"] += hd["opens"]

            for domain, hdd in r.get("he_domains", {}).items():
                dl["he_domains"][domain]["campaigns"].add(cname)
                dl["he_domains"][domain]["he_prospects"] |= hdd["he_prospects"]
                dl["he_domains"][domain]["max_opens"] = max(
                    dl["he_domains"][domain]["max_opens"], hdd["max_opens"]
                )

            for subject, sd in r.get("subject_data", {}).items():
                gs = dl["subject_data"][subject]
                gs["sends"]             += sd["sends"]
                gs["total_opens"]       += sd["total_opens"]
                gs["unique_openers"]    |= sd["unique_openers"]
                gs["unique_leads_sent"] |= sd["unique_leads_sent"]
                gs["he_leads"]          |= sd["he_leads"]
                gs["campaigns"]         |= sd["campaigns"]

    totals = {}
    for label in LABELS:
        dl = detail[label]
        totals[label] = {
            "brands_engaged":          len(dl["domains_engaged"]),
            "unique_brand_open":       len(dl["domains_opened"]),
            "prospects_engaged":       dl["sent_total"],
            "unique_prospects_open":   dl["open_total"],
            "he_brand":                len(dl["he_domains"]),
            "he_prospect":             len(dl["he_leads"]),
            "he_prospect_per_company": len(dl["he_domains"]),
        }

    return totals, detail


# ══════════════════════════════════════════════════════════════════════════
# STEP 5: SF Lead × Smartlead Join
# ══════════════════════════════════════════════════════════════════════════

def build_email_index(results):
    """Build email → list of stat rows index from all campaign results."""
    email_index = defaultdict(list)
    for cr in results:
        if not cr: continue
        for row in cr.get("raw_stats", []):
            email = safe_str(row.get("lead_email")).lower()
            if email:
                email_index[email].append(row)
    print(f"   SL email index: {len(email_index):,} unique emails")
    return email_index


def analyze_sf_lead(lead, email_index):
    """Join one SF lead with all their Smartlead activity."""
    email = lead["email"]
    rows  = email_index.get(email, [])

    if not rows:
        return {**lead, "found_in_sl": False}

    in_range = [
        r for r in rows
        if isinstance(r.get("sent_time"), str)
        and DATE_FROM <= r["sent_time"][:10] <= DATE_TO
    ]

    campaigns_seen   = {}
    total_opens      = 0
    total_clicks     = 0
    sequences_detail = []
    seen_seq_keys    = set()

    for r in in_range:
        opens    = safe_int(r.get("open_count"))
        clicks   = safe_int(r.get("click_count"))
        seq_no   = safe_str(r.get("sequence_number")) or "?"
        try:    seq_no_int = int(seq_no)
        except: seq_no_int = seq_no
        subject  = safe_str(r.get("email_subject"))
        s_date   = safe_str(r.get("sent_time"))[:10]
        open_t   = safe_str(r.get("open_time"))
        if open_t and "T" in open_t:
            open_t = open_t[:16].replace("T", " ")
        camp    = safe_str(r.get("_campaign_name"))
        camp_id = safe_str(r.get("_campaign_id"))
        is_unsub = safe_str(r.get("is_unsubscribed")).lower() == "true"
        is_bnc   = safe_str(r.get("is_bounced")).lower() == "true"

        total_opens  += opens
        total_clicks += clicks

        if camp not in campaigns_seen:
            campaigns_seen[camp] = {"id": camp_id, "opens": 0, "sends": 0}
        campaigns_seen[camp]["opens"] += opens
        campaigns_seen[camp]["sends"] += 1

        key = (camp, str(seq_no), s_date)
        if key not in seen_seq_keys:
            seen_seq_keys.add(key)
            sequences_detail.append({
                "campaign":   camp,
                "seq_no":     seq_no_int,
                "subject":    subject,
                "sent_date":  s_date,
                "open_time":  open_t,
                "opens":      opens,
                "clicks":     clicks,
                "is_unsub":   is_unsub,
                "is_bounced": is_bnc,
            })

    is_he = total_opens >= 3
    sequences_detail.sort(key=lambda s: (
        s.get("sent_date", ""), str(s.get("seq_no", "")).zfill(5)
    ))
    best_seq = max(sequences_detail, key=lambda s: s["opens"]) if sequences_detail else {}

    return {
        **lead,
        "found_in_sl":       True,
        "sl_campaigns":      ", ".join(sorted(campaigns_seen.keys())),
        "sl_num_campaigns":  len(campaigns_seen),
        "sl_total_sends":    len(in_range),
        "sl_total_opens":    total_opens,
        "sl_total_clicks":   total_clicks,
        "sl_is_he":          is_he,
        "sl_he_label":       "✅ HE" if is_he else "No",
        "sl_best_subject":   best_seq.get("subject", ""),
        "sl_best_seq_opens": best_seq.get("opens", 0),
        "sl_first_sent":     sequences_detail[0]["sent_date"] if sequences_detail else "",
        "sl_last_sent":      sequences_detail[-1]["sent_date"] if sequences_detail else "",
        "sl_sequences":      sequences_detail,
        "sl_campaigns_data": campaigns_seen,
    }


# ══════════════════════════════════════════════════════════════════════════
# STEP 6: Build Excel
# ══════════════════════════════════════════════════════════════════════════

def save_excel(totals, detail, camp_results, analyzed_leads):
    labels = LABELS
    fname  = f"Smartlead_FullAnalysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    metric_keys = [
        ("brands_engaged",          "Brands Engaged (unique domains)"),
        ("unique_brand_open",       "Unique Brand Open (unique domains)"),
        ("prospects_engaged",       "Prospects Engaged"),
        ("unique_prospects_open",   "Unique Prospects Open"),
        ("he_brand",                "High-Engagement (Brand/Domain)"),
        ("he_prospect",             "High-Engagement (Prospect)"),
        ("he_prospect_per_company", "High-Engagement (Prospect per Company)"),
    ]

    with pd.ExcelWriter(fname, engine="openpyxl") as writer:

        # ── Sheet 1: Summary ─────────────────────────────────────────────
        df_sum = pd.DataFrame({
            "Metric": [name for _, name in metric_keys],
            **{label: [totals[label][key] for key, _ in metric_keys] for label in labels}
        })
        df_sum.to_excel(writer, sheet_name="Summary", index=False)
        print("  ✅ Sheet 1:  Summary")

        # ── Sheet 2: Per Campaign ────────────────────────────────────────
        camp_rows = []
        for cr in sorted(camp_results, key=lambda x: x.get("id", 0)):
            if not cr: continue
            row = {"Campaign ID": cr["id"], "Campaign Name": cr["name"]}
            for label in labels:
                r = cr["ranges"].get(label, {})
                row[f"{label} | Sent"]         = r.get("sent_count", 0)
                row[f"{label} | Opens"]        = r.get("open_count", 0)
                row[f"{label} | Brands"]       = len(r.get("domain_data", {}))
                row[f"{label} | HE Prospects"] = len(r.get("he_leads", {}))
                row[f"{label} | HE Companies"] = len(r.get("he_domains", {}))
            camp_rows.append(row)
        pd.DataFrame(camp_rows).to_excel(writer, sheet_name="Per Campaign", index=False)
        print("  ✅ Sheet 2:  Per Campaign")

        # ── Sheet 3: Brands Engaged ──────────────────────────────────────
        rows = []
        all_domains = set()
        for label in labels:
            all_domains |= set(detail[label]["domains_engaged"].keys())
        for domain in sorted(all_domains):
            row = {"Domain": domain}
            total, campaigns_all = 0, set()
            for label in labels:
                d = detail[label]["domains_engaged"].get(domain)
                if d:
                    row[f"{label} - Prospects Reached"] = len(d["prospects"])
                    row[f"{label} - Campaigns"]         = len(d["campaigns"])
                    total += len(d["prospects"])
                    campaigns_all |= d["campaigns"]
                else:
                    row[f"{label} - Prospects Reached"] = 0
                    row[f"{label} - Campaigns"]         = 0
            row["Total Prospects (all ranges)"] = total
            row["All Campaigns"]                = ", ".join(sorted(campaigns_all))
            rows.append(row)
        pd.DataFrame(rows).to_excel(writer, sheet_name="Brands Engaged", index=False)
        print(f"  ✅ Sheet 3:  Brands Engaged ({len(rows)} domains)")

        # ── Sheet 4: Unique Brand Open ────────────────────────────────────
        rows = []
        all_domains_open = set()
        for label in labels:
            all_domains_open |= set(detail[label]["domains_opened"].keys())
        for domain in sorted(all_domains_open):
            row = {"Domain": domain}
            total, campaigns_all = 0, set()
            for label in labels:
                d = detail[label]["domains_opened"].get(domain)
                if d:
                    row[f"{label} - Prospects Opened"] = len(d["prospects_opened"])
                    total += len(d["prospects_opened"])
                    campaigns_all |= d["campaigns"]
                else:
                    row[f"{label} - Prospects Opened"] = 0
            row["Total Opens (all ranges)"] = total
            row["All Campaigns"]            = ", ".join(sorted(campaigns_all))
            rows.append(row)
        rows.sort(key=lambda x: x["Total Opens (all ranges)"], reverse=True)
        pd.DataFrame(rows).to_excel(writer, sheet_name="Unique Brand Open", index=False)
        print(f"  ✅ Sheet 4:  Unique Brand Open ({len(rows)} domains)")

        # ── Sheet 5: Prospects Engaged ────────────────────────────────────
        rows = []
        all_leads = set()
        for label in labels:
            all_leads |= set(detail[label]["leads_sent"].keys())
        for email in sorted(all_leads):
            row = {"Email": email}
            dom, cams, appeared = None, set(), 0
            for label in labels:
                d = detail[label]["leads_sent"].get(email)
                if d:
                    row[label] = "✓"; dom = d["domain"]; cams |= d["campaigns"]; appeared += 1
                else:
                    row[label] = ""
            row["Domain"]               = dom or ""
            row["Date Ranges Appeared"] = appeared
            row["Campaigns"]            = ", ".join(sorted(cams))
            rows.append(row)
        pd.DataFrame(rows).to_excel(writer, sheet_name="Prospects Engaged", index=False)
        print(f"  ✅ Sheet 5:  Prospects Engaged ({len(rows)} leads)")

        # ── Sheet 6: Unique Prospects Open ────────────────────────────────
        rows = []
        all_openers = set()
        for label in labels:
            all_openers |= set(detail[label]["leads_opened"].keys())
        for email in sorted(all_openers):
            row = {"Email": email}
            dom, cams, total_opens = None, set(), 0
            for label in labels:
                d = detail[label]["leads_opened"].get(email)
                if d:
                    opens = d["total_opens"]
                    row[f"{label} - Opens"] = opens
                    dom = d["domain"]; cams |= d["campaigns"]; total_opens += opens
                else:
                    row[f"{label} - Opens"] = 0
            row["Domain"]      = dom or ""
            row["Total Opens"] = total_opens
            row["Campaigns"]   = ", ".join(sorted(cams))
            rows.append(row)
        rows.sort(key=lambda x: x["Total Opens"], reverse=True)
        pd.DataFrame(rows).to_excel(writer, sheet_name="Unique Prospects Open", index=False)
        print(f"  ✅ Sheet 6:  Unique Prospects Open ({len(rows)} leads)")

        # ── Sheet 7: HE Brand-Domain ──────────────────────────────────────
        rows = []
        all_he_domains = set()
        for label in labels:
            all_he_domains |= set(detail[label]["he_domains"].keys())
        for domain in sorted(all_he_domains):
            row = {"Domain": domain}
            cams, total_he, max_opens = set(), 0, 0
            for label in labels:
                d = detail[label]["he_domains"].get(domain)
                if d:
                    he_count = len(d["he_prospects"])
                    row[f"{label} - HE Prospects"] = he_count
                    row[f"{label} - Max Opens"]    = d["max_opens"]
                    cams |= d["campaigns"]; total_he += he_count
                    max_opens = max(max_opens, d["max_opens"])
                else:
                    row[f"{label} - HE Prospects"] = 0
                    row[f"{label} - Max Opens"]    = 0
            row["Total HE Prospects (all ranges)"] = total_he
            row["Max Opens Ever"]                  = max_opens
            row["Campaigns"]                       = ", ".join(sorted(cams))
            rows.append(row)
        rows.sort(key=lambda x: x["Total HE Prospects (all ranges)"], reverse=True)
        pd.DataFrame(rows).to_excel(writer, sheet_name="HE Brand-Domain", index=False)
        print(f"  ✅ Sheet 7:  HE Brand-Domain ({len(rows)} domains)")

        # ── Sheet 8: HE Prospects ─────────────────────────────────────────
        rows = []
        all_he_leads = set()
        for label in labels:
            all_he_leads |= set(detail[label]["he_leads"].keys())
        for email in sorted(all_he_leads):
            row = {"Email": email}
            dom, cams, total_opens = None, set(), 0
            for label in labels:
                d = detail[label]["he_leads"].get(email)
                if d:
                    row[f"{label} - Opens"] = d["total_opens"]
                    dom = d["domain"]; cams |= d["campaigns"]; total_opens += d["total_opens"]
                else:
                    row[f"{label} - Opens"] = 0
            row["Domain"]      = dom or ""
            row["Total Opens"] = total_opens
            row["Campaigns"]   = ", ".join(sorted(cams))
            rows.append(row)
        rows.sort(key=lambda x: x["Total Opens"], reverse=True)
        pd.DataFrame(rows).to_excel(writer, sheet_name="HE Prospects", index=False)
        print(f"  ✅ Sheet 8:  HE Prospects ({len(rows)} leads)")

        # ── Sheet 9: HE per Company ───────────────────────────────────────
        rows = []
        for domain in sorted(all_he_domains):
            row = {"Domain": domain}
            cams, all_he_emails, total_appearances = set(), set(), 0
            for label in labels:
                d = detail[label]["he_domains"].get(domain)
                if d:
                    row[f"{label} - HE Count"]  = len(d["he_prospects"])
                    row[f"{label} - HE Emails"] = ", ".join(sorted(d["he_prospects"]))
                    cams |= d["campaigns"]; all_he_emails |= d["he_prospects"]; total_appearances += 1
                else:
                    row[f"{label} - HE Count"]  = 0
                    row[f"{label} - HE Emails"] = ""
            row["Total Date Ranges as HE"]   = total_appearances
            row["All HE Prospect Emails"]    = ", ".join(sorted(all_he_emails))
            row["Total Unique HE Prospects"] = len(all_he_emails)
            row["Campaigns"]                 = ", ".join(sorted(cams))
            rows.append(row)
        rows.sort(key=lambda x: x["Total Unique HE Prospects"], reverse=True)
        pd.DataFrame(rows).to_excel(writer, sheet_name="HE per Company", index=False)
        print(f"  ✅ Sheet 9:  HE per Company ({len(rows)} companies)")

        # ── Sheet 10: Subject Line Analysis ──────────────────────────────
        all_subjects = set()
        for label in labels:
            all_subjects |= set(detail[label]["subject_data"].keys())

        subj_rows = []
        for subject in sorted(all_subjects):
            row = {"Email Subject": subject}
            g_sends, g_opens = 0, 0
            g_openers, g_sent, g_he, g_camps = set(), set(), set(), set()
            for label in labels:
                sd = detail[label]["subject_data"].get(subject)
                if sd:
                    row[f"{label} - Sends"]          = sd["sends"]
                    row[f"{label} - Opens"]          = sd["total_opens"]
                    row[f"{label} - Unique Openers"] = len(sd["unique_openers"])
                    g_sends  += sd["sends"]; g_opens += sd["total_opens"]
                    g_openers |= sd["unique_openers"]; g_sent |= sd["unique_leads_sent"]
                    g_he |= sd["he_leads"]; g_camps |= sd["campaigns"]
                else:
                    row[f"{label} - Sends"]          = 0
                    row[f"{label} - Opens"]          = 0
                    row[f"{label} - Unique Openers"] = 0
            row["Total Sends"]       = g_sends
            row["Total Opens"]       = g_opens
            row["Unique Openers"]    = len(g_openers)
            row["Unique Leads Sent"] = len(g_sent)
            row["HE Leads"]          = len(g_he)
            row["Open Rate %"]       = round(len(g_openers) / g_sends * 100, 1) if g_sends else 0
            row["HE Rate %"]         = round(len(g_he) / g_sends * 100, 1) if g_sends else 0
            row["Campaigns"]         = ", ".join(sorted(g_camps))
            subj_rows.append(row)

        subj_rows.sort(key=lambda x: (-x["HE Leads"], -x["Unique Openers"]))
        summary_cols   = ["Email Subject", "Total Sends", "Total Opens",
                          "Unique Openers", "Unique Leads Sent", "HE Leads",
                          "Open Rate %", "HE Rate %", "Campaigns"]
        per_range_cols = [f"{l} - {m}" for l in labels
                          for m in ["Sends", "Opens", "Unique Openers"]]
        all_cols = summary_cols + per_range_cols
        df_subj  = pd.DataFrame(subj_rows)
        df_subj  = df_subj[[c for c in all_cols if c in df_subj.columns]]
        df_subj.to_excel(writer, sheet_name="Subject Line Analysis", index=False)
        print(f"  ✅ Sheet 10: Subject Line Analysis ({len(subj_rows)} subjects)")

        # ══ SALESFORCE JOIN SHEETS (Sheets 11-15) ════════════════════════
        if not analyzed_leads:
            print("  ⏭️  Sheets 11-15: Skipped (no Salesforce data)")
        else:
            found     = [l for l in analyzed_leads if l.get("found_in_sl")]
            not_found = [l for l in analyzed_leads if not l.get("found_in_sl")]

            # ── Sheet 11: Lead Summary ────────────────────────────────────
            rows = []
            for l in analyzed_leads:
                rows.append({
                    "SF ID":              l["sf_id"],
                    "Name":               l["name"],
                    "Email":              l["email"],
                    "Company":            l["company"],
                    "Domain":             l.get("domain") or "",
                    "Title":              l["title"],
                    "Phone":              l["phone"],
                    "Industry":           l["industry"],
                    "Country":            l["country"],
                    "Lead Source":        l["lead_source"],
                    "Sub Channel":        l["sub_channel"],
                    "SF Status":          l["status"],
                    "SF Rating":          l["rating"],
                    "Owner":              l["owner"],
                    "SF Created Date":    l["created_date"],
                    "Converted?":         "Yes" if l["is_converted"] else "No",
                    "Converted Date":     l["converted_date"],
                    "Found in Smartlead": "Yes" if l.get("found_in_sl") else "❌ No",
                    "SL Campaigns":       l.get("sl_campaigns", ""),
                    "# Campaigns":        l.get("sl_num_campaigns", 0),
                    "Total Sends":        l.get("sl_total_sends", 0),
                    "Total Opens":        l.get("sl_total_opens", 0),
                    "Total Clicks":       l.get("sl_total_clicks", 0),
                    "HE Status":          l.get("sl_he_label", "Not in SL"),
                    "Best Subject Line":  l.get("sl_best_subject", ""),
                    "Best Seq Opens":     l.get("sl_best_seq_opens", 0),
                    "First Email Sent":   l.get("sl_first_sent", ""),
                    "Last Email Sent":    l.get("sl_last_sent", ""),
                })
            df11 = pd.DataFrame(rows)
            df11 = df11.sort_values(["HE Status", "Total Opens"], ascending=[True, False])
            df11.to_excel(writer, sheet_name="Lead Summary", index=False)
            print(f"  ✅ Sheet 11: Lead Summary ({len(rows)} SF leads)")

            # ── Sheet 12: Campaign Breakdown ──────────────────────────────
            camp_agg = defaultdict(lambda: {
                "total_leads": 0, "he_leads": 0, "total_opens": 0, "total_sends": 0
            })
            for l in found:
                for camp, cd in l.get("sl_campaigns_data", {}).items():
                    camp_agg[camp]["total_leads"] += 1
                    camp_agg[camp]["total_opens"] += cd["opens"]
                    camp_agg[camp]["total_sends"] += cd["sends"]
                    if l.get("sl_is_he"):
                        camp_agg[camp]["he_leads"] += 1
            camp_rows12 = []
            for camp, cd in sorted(camp_agg.items(), key=lambda x: -x[1]["he_leads"]):
                tl = cd["total_leads"]
                camp_rows12.append({
                    "Campaign":           camp,
                    "Total SF Leads":     tl,
                    "HE Leads":           cd["he_leads"],
                    "Non-HE Leads":       tl - cd["he_leads"],
                    "HE Rate %":          round(cd["he_leads"] / tl * 100, 1) if tl else 0,
                    "Total Emails Sent":  cd["total_sends"],
                    "Total Opens":        cd["total_opens"],
                    "Avg Opens per Lead": round(cd["total_opens"] / tl, 1) if tl else 0,
                })
            pd.DataFrame(camp_rows12).to_excel(writer, sheet_name="Campaign Breakdown", index=False)
            print(f"  ✅ Sheet 12: Campaign Breakdown ({len(camp_rows12)} campaigns)")

            # ── Sheet 13: SF Subject Analysis ────────────────────────────
            subj_agg = defaultdict(lambda: {
                "he_leads": set(), "openers": set(), "all_leads": set(),
                "total_opens": 0, "sends": 0, "campaigns": set()
            })
            for l in found:
                email = l["email"]
                for seq in l.get("sl_sequences", []):
                    subj = seq.get("subject", "").strip()
                    if not subj: continue
                    subj_agg[subj]["all_leads"].add(email)
                    subj_agg[subj]["sends"]       += 1
                    subj_agg[subj]["total_opens"] += seq.get("opens", 0)
                    subj_agg[subj]["campaigns"].add(seq.get("campaign", ""))
                    if seq.get("opens", 0) > 0:
                        subj_agg[subj]["openers"].add(email)
                    if l.get("sl_is_he"):
                        subj_agg[subj]["he_leads"].add(email)
            subj_rows13 = []
            for subj, sd in subj_agg.items():
                sends = sd["sends"]
                subj_rows13.append({
                    "Email Subject":     subj,
                    "HE Leads (unique)": len(sd["he_leads"]),
                    "Unique Openers":    len(sd["openers"]),
                    "Unique Leads Sent": len(sd["all_leads"]),
                    "Total Sends":       sends,
                    "Total Opens":       sd["total_opens"],
                    "Open Rate %":       round(len(sd["openers"]) / sends * 100, 1) if sends else 0,
                    "HE Rate %":         round(len(sd["he_leads"]) / sends * 100, 1) if sends else 0,
                    "Campaigns":         ", ".join(sorted(sd["campaigns"])),
                })
            subj_rows13.sort(key=lambda x: (-x["HE Leads (unique)"], -x["Unique Openers"]))
            pd.DataFrame(subj_rows13).to_excel(writer, sheet_name="SF Subject Analysis", index=False)
            print(f"  ✅ Sheet 13: SF Subject Analysis ({len(subj_rows13)} subjects)")

            # ── Sheet 14: HE Sequence Detail ──────────────────────────────
            he_sf_leads = [l for l in found if l.get("sl_is_he")]
            seq_rows14  = []
            for l in sorted(he_sf_leads, key=lambda x: x.get("domain", "") or ""):
                for seq in l.get("sl_sequences", []):
                    opens = seq.get("opens", 0)
                    seq_rows14.append({
                        "Domain":           l.get("domain") or "",
                        "Company":          l["company"],
                        "Name":             l["name"],
                        "Email":            l["email"],
                        "SF Status":        l["status"],
                        "Converted?":       "Yes" if l["is_converted"] else "No",
                        "Campaign":         seq.get("campaign", ""),
                        "Sequence #":       seq.get("seq_no", "?"),
                        "Sent Date":        seq.get("sent_date", ""),
                        "Subject":          seq.get("subject", ""),
                        "Opens":            opens,
                        "Opened?":          f"✅ Yes ({opens})" if opens > 0 else "❌ No",
                        "First Open Time":  seq.get("open_time", ""),
                        "Clicks":           seq.get("clicks", 0),
                        "Unsubscribed":     "Yes" if seq.get("is_unsub") else "No",
                        "Bounced":          "Yes" if seq.get("is_bounced") else "No",
                        "Lead Total Opens": l.get("sl_total_opens", 0),
                    })
            seq_rows14.sort(key=lambda x: (
                x["Domain"], x["Email"],
                x.get("Sent Date", ""), str(x.get("Sequence #", "")).zfill(5)
            ))
            pd.DataFrame(seq_rows14).to_excel(writer, sheet_name="HE Sequence Detail", index=False)
            print(f"  ✅ Sheet 14: HE Sequence Detail ({len(seq_rows14)} rows, {len(he_sf_leads)} HE leads)")

            # ── Sheet 15: Not Found in SL ─────────────────────────────────
            nf_rows = []
            for l in not_found:
                nf_rows.append({
                    "SF ID":           l["sf_id"],
                    "Name":            l["name"],
                    "Email":           l["email"],
                    "Company":         l["company"],
                    "Domain":          l.get("domain") or "",
                    "Title":           l["title"],
                    "SF Status":       l["status"],
                    "Created Date":    l["created_date"],
                    "Owner":           l["owner"],
                    "Converted?":      "Yes" if l["is_converted"] else "No",
                    "Possible Reason": "Email not in any Smartlead campaign statistics",
                })
            pd.DataFrame(nf_rows).to_excel(writer, sheet_name="Not Found in SL", index=False)
            print(f"  ✅ Sheet 15: Not Found in SL ({len(nf_rows)} leads)")

    print(f"\n💾 Saved: {fname}")
    return fname


# ══════════════════════════════════════════════════════════════════════════
# PRINT HELPERS
# ══════════════════════════════════════════════════════════════════════════

def print_sl_table(totals):
    C   = 14
    div = "=" * (42 + C * len(LABELS))
    print("\n" + div)
    print("  SMARTLEAD EMAIL ENGAGEMENT ANALYSIS - 2026")
    print(div)
    print(f"  {'Metric':<40}" + "".join(f"{l:^{C}}" for l in LABELS))
    print("-" * (42 + C * len(LABELS)))
    for key, name in [
        ("brands_engaged",          "Brands Engaged (unique domains)"),
        ("unique_brand_open",       "Unique Brand Open (unique domains)"),
        ("prospects_engaged",       "Prospects Engaged"),
        ("unique_prospects_open",   "Unique Prospects Open"),
        ("he_brand",                "High-Engagement (Brand/Domain)"),
        ("he_prospect",             "High-Engagement (Prospect)"),
        ("he_prospect_per_company", "High-Engagement (Prospect per Company)"),
    ]:
        print(f"  {name:<40}" + "".join(f"{totals[l][key]:^{C},}" for l in LABELS))
    print(div)
    print()

def print_sf_summary(analyzed):
    if not analyzed: return
    found    = [l for l in analyzed if l.get("found_in_sl")]
    he_leads = [l for l in found if l.get("sl_is_he")]
    not_fnd  = [l for l in analyzed if not l.get("found_in_sl")]
    print("\n" + "="*60)
    print("  SALESFORCE HIGH INTENT — RESULTS SUMMARY")
    print("="*60)
    print(f"  Total SF Leads (Email / High Intent):  {len(analyzed):,}")
    print(f"  Found in Smartlead:                    {len(found):,}")
    print(f"  NOT found in Smartlead:                {len(not_fnd):,}")
    print(f"  HE Leads (opens >= 3):                 {len(he_leads):,}")
    if found:
        print(f"  HE Rate (of those found):              {len(he_leads)/len(found)*100:.1f}%")
    print("="*60 + "\n")


# ══════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════

def main():
    print("🚀 Smartlead Full Analysis — v6 + SF Join + Subject Analysis\n")
    print(f"   Period: {DATE_FROM} → {DATE_TO}\n")

    # ── STEP 1: Salesforce leads (optional — skipped if creds not set) ──
    sf_leads = fetch_sf_leads()

    # ── STEP 2: Get & filter campaigns ──────────────────────────────────
    camps = filter_from_seed(get_all_campaigns(), SEED_CAMPAIGN_ID)
    t0    = time.time()
    results, failed = [], []

    # ── STEP 3: Analyze all campaigns in parallel ────────────────────────
    print(f"\n⚡ Processing {len(camps)} campaigns ({THREADS} threads)...\n")
    with ThreadPoolExecutor(max_workers=THREADS) as ex:
        futures = {ex.submit(analyze_campaign, c): c for c in camps}
        done    = 0
        for future in as_completed(futures):
            c = futures[future]; done += 1
            try:
                results.append(future.result())
                print(f"  [{done:2}/{len(camps)}] ✅  {c['name']}")
            except Exception as e:
                print(f"  [{done:2}/{len(camps)}] ❌  {c['name']} — {e}")
                failed.append(c)

    print(f"\n⏱️  Done in {time.time()-t0:.0f}s | ✅ {len(results)} | ❌ {len(failed)}\n")

    # ── STEP 4: Aggregate Smartlead data ─────────────────────────────────
    totals, detail = aggregate_all(results)
    print_sl_table(totals)

    # ── STEP 5: Join SF leads with SL data ───────────────────────────────
    analyzed_leads = []
    if sf_leads:
        print(f"🔗 Building SL email index...")
        email_index = build_email_index(results)
        print(f"🔗 Joining {len(sf_leads)} SF leads with Smartlead data...")
        analyzed_leads = [analyze_sf_lead(lead, email_index) for lead in sf_leads]
        print_sf_summary(analyzed_leads)

    # ── STEP 6: Export Excel ─────────────────────────────────────────────
    print("📊 Building Excel sheets...")
    save_excel(totals, detail, results, analyzed_leads)

    if failed:
        print("\n⚠️  Failed campaigns:", [c["name"] for c in failed])

    print("\n✨ Done!")

if __name__ == "__main__":
    main()