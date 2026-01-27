# ABOUTME: Analysis of all eligibility-related metrics (IPC, SPI, SBIRT, Referral, FWS)
# ABOUTME: Covers indicators 10, 11 and related eligibility tracking

# Eligibility Metrics Analysis

## Overview

Multiple indicators relate to client eligibility for various interventions based on mwTool screening results.

| Metric ID | Description | Indicator # |
|-----------|-------------|-------------|
| `clients_eligible_for_ipc` | Common mental health disorders | 11 |
| `clients_eligible_spi` | Suicide risk | 10 |
| `clients_sbirt_mi_eligible` | Alcohol/drug problems | 9 |
| `clients_eligible_referral` | Severe mental health (needs referral) | 10 |
| `clients_eligible_fws` | Financial wellness services | - |

---

## Current Implementation

### clients_eligible_for_ipc

**Source:** `dbt/models/metrics/clients_eligible_for_ipc.sql`

**Logic:**
```sql
-- Extracts no-mental-problem from mwTool questionnaire
-- When no_mental_problem = false, patient is eligible for IPC
(jsonb_path_query_first(
  qr.items::jsonb,
  '$.**.item[*] ? (@.linkId == "no-mental-problem").answer[0].valueBoolean'
))::boolean as no_mental_problem
...
WHERE mw.no_mental_problem = false
```

### clients_accepted_ipc

**Source:** `dbt/models/metrics/clients_accepted_ipc.sql`

**Logic:** Tracks clients who actually started IPC (have a session record).

---

## Database Test Results

### Query: Eligibility Counts (Current Date)
```sql
SELECT
  'clients_eligible_for_ipc' as metric,
  count(distinct subject_patient_id) as count
FROM engage_analytics.clients_eligible_for_ipc
WHERE period_date = current_date
UNION ALL
SELECT 'clients_eligible_spi', count(distinct subject_patient_id)
FROM engage_analytics.clients_eligible_spi WHERE period_date = current_date
UNION ALL
SELECT 'clients_sbirt_mi_eligible', count(distinct subject_patient_id)
FROM engage_analytics.clients_sbirt_mi_eligible WHERE period_date = current_date
UNION ALL
SELECT 'clients_eligible_referral', count(distinct subject_patient_id)
FROM engage_analytics.clients_eligible_referral WHERE period_date = current_date
UNION ALL
SELECT 'clients_eligible_fws', count(distinct subject_patient_id)
FROM engage_analytics.clients_eligible_fws WHERE period_date = current_date;
```

**Results:**
| metric | count |
|--------|-------|
| clients_eligible_for_ipc | 50 |
| clients_eligible_spi | 50 |
| clients_sbirt_mi_eligible | 50 |
| clients_eligible_referral | 50 |
| clients_eligible_fws | 43 |

### Query: Acceptance/Uptake Analysis
```sql
-- Compare eligible vs accepted for IPC
WITH eligible AS (
  SELECT DISTINCT subject_patient_id
  FROM engage_analytics.clients_eligible_for_ipc
),
accepted AS (
  SELECT DISTINCT subject_patient_id
  FROM engage_analytics.clients_accepted_ipc
)
SELECT
  count(distinct e.subject_patient_id) as eligible,
  count(distinct a.subject_patient_id) as accepted,
  round(count(distinct a.subject_patient_id)::numeric /
        nullif(count(distinct e.subject_patient_id), 0) * 100, 1) as acceptance_rate
FROM eligible e
LEFT JOIN accepted a USING (subject_patient_id);
```

---

## Analysis

### Implementation Status: **PARTIALLY IMPLEMENTED**

### What Works:
1. **Eligibility tracking** for all 5 intervention types
2. **Cumulative counts** via date spine (tracks eligibility over time)
3. **JSON extraction workaround** for broken mwTool outcome columns

### What's Missing:

1. **"Offered" vs "Eligible"**: Spec indicator #11 asks for "clients offered EBT" but we track "clients eligible". These are different:
   - Eligible = mwTool screening suggests they need intervention
   - Offered = provider actually presented the intervention option
   - No data field exists for "offered"

2. **Acceptance Rate Metric**: Have `clients_accepted_*` views but no acceptance rate metric in the catalog

3. **Organization Breakdown**: All counts show NULL organization (data quality issue)

---

## Recommendations

1. **Add acceptance rate metrics**:
   ```yaml
   - id: ipc_acceptance_rate
     numerator: "count from clients_accepted_ipc"
     denominator: "count from clients_eligible_for_ipc"
     unit: percent
   ```

2. **Clarify terminology** with stakeholders:
   - What constitutes "offered"?
   - Is there a questionnaire field for this?

3. **Fix organization assignment** to enable org-level disaggregation

---

## Checklist

- [x] Eligibility models created (5 intervention types)
- [x] Acceptance models created (IPC, SBIRT, Referral, FWS)
- [x] Outputs to fct_metrics_long
- [ ] Acceptance rate metrics added to catalog
- [ ] "Offered" tracking clarified/implemented
- [ ] Organization disaggregation working
- [ ] Unit tests written
