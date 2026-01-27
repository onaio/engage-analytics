# ABOUTME: Analysis of Active Users indicator comparing spec to implementation
# ABOUTME: Includes test queries and findings from database validation

# Indicator 01: Active Users

## Specification Summary

| Field | Value |
|-------|-------|
| **Domain** | System Use |
| **Indicator** | Active users |
| **Data Type** | Count |
| **Frequency** | Monthly |
| **Description** | Health workers who submitted at least one report in the period |

### Formula
Count the number of users that have submitted at least 1 record during the reporting period. This can also be set to a certain threshold (e.g., 30 visits or some predefined target).

### FHIR Notes
Encounter represents an activity of the Practitioner. Check `Encounter.period` for date/time. Check which Encounter belongs to which Practitioner in `Encounter.participant` with `Encounter.participant.type` equals `ADM` code.

---

## Current Implementation

### Metric ID: `active_providers`

**Source Model:** `dbt/models/metrics/active_providers.sql`

**Implementation Logic:**
```sql
-- Counts practitioners who submitted QuestionnaireResponses in past 30 days
with qr_activity as (
  select
    author_practitioner_id as practitioner_id,
    max(meta_lastupdated::date) as last_activity_date,
    count(*) as qr_count_30d
  from stg_questionnaire_response
  where meta_lastupdated::date >= current_date - interval '30 days'
    and author_practitioner_id is not null
  group by 1
)
select
  p.id as practitioner_id,
  p.organization_id,
  case when qa.practitioner_id is not null then true else false end as is_active_30d
from practitioners p
left join qr_activity qa on qa.practitioner_id = p.id
where p.active = 'true'
```

**Metrics Catalog Entry:**
```yaml
- id: active_providers
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: active_providers
  expression: "count(distinct case when is_active_30d then practitioner_id end)"
  description: "Providers who submitted QRs in past 30 days"
  version: v1
```

---

## Database Test Results

### Query: Active Providers by Organization
```sql
SELECT
  organization_id,
  count(*) as total_practitioners,
  count(*) filter (where is_active_30d = true) as active_30d
FROM engage_analytics.active_providers
GROUP BY organization_id;
```

**Results:**
| organization_id | total_practitioners | active_30d |
|-----------------|---------------------|------------|
| 526e3168-... | 2 | 0 |
| 602ec278-... | 1 | 0 |
| 8223aa6f-... | 1 | 0 |
| 92dd11a8-... | 1 | 0 |
| (null) | 66 | 0 |

### Query: Metrics Table Output
```sql
SELECT period_date, organization_id, metric_id, value
FROM engage_analytics.fct_metrics_long
WHERE metric_id = 'active_providers';
```

**Results:** 5 records showing 0 active providers (data is stale - no activity in last 30 days)

---

## Analysis

### Implementation Status: **IMPLEMENTED**

### Issues Found:

1. **Naming Mismatch**: Spec says "Active Users" but implementation uses "active_providers". These are the same concept (practitioners = users = providers), but naming could be clearer.

2. **Activity Definition**: Implementation uses QuestionnaireResponse submissions, not Encounters as suggested in FHIR notes. This is likely correct since QR submissions are the primary activity.

3. **Rolling Window**: Uses a rolling 30-day window from `current_date`, which matches the monthly reporting requirement.

4. **Data Quality Issue**: 66 practitioners have NULL organization_id. These should be investigated.

5. **No Recent Activity**: All active_30d counts are 0, indicating no QR submissions in the last 30 days (data may be test/historical data).

### Gaps:

1. **Threshold Option**: Spec mentions optional threshold (e.g., "30 visits or some predefined target"). Current implementation only checks for any activity (>=1), not configurable thresholds.

2. **Disaggregation**: No disaggregation implemented (spec didn't specify any).

---

## Recommendations

1. **Consider adding parameterized threshold** for "active" definition
2. **Investigate NULL organization_ids** in practitioners table
3. **Add historical tracking** - current implementation only shows current_date snapshot

---

## Checklist

- [x] SQL model created (`active_providers.sql`)
- [x] Included in metrics catalog
- [x] Outputs to `fct_metrics_long`
- [ ] Unit tests written
- [ ] Validated against sample data (data is stale)
- [ ] Threshold configuration added
