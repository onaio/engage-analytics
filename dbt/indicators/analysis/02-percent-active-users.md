# ABOUTME: Analysis of Percent Active Users indicator comparing spec to implementation
# ABOUTME: Includes test queries and findings from database validation

# Indicator 02: Percent of Active Users

## Specification Summary

| Field | Value |
|-------|-------|
| **Domain** | System Use |
| **Indicator** | Percent of active users |
| **Data Type** | Percent |
| **Frequency** | Monthly |
| **Description** | Percentage of users who used the system in the past month |

### Formula
`# active users / # total users`

---

## Current Implementation

### Metric ID: `percent_active_providers`

**Metrics Catalog Entry:**
```yaml
- id: percent_active_providers
  unit: percent
  grain: day
  entity_keys: [organization_id]
  source_model: active_providers
  numerator: "count(distinct case when is_active_30d then practitioner_id end)"
  denominator: "nullif(count(distinct practitioner_id), 0)"
  description: "Percentage of providers active in past 30 days"
  version: v1
```

**Formula Applied:**
`(numerator / denominator) * 100`

---

## Database Test Results

### Query: Percent Active Providers
```sql
SELECT period_date, organization_id, metric_id, value
FROM engage_analytics.fct_metrics_long
WHERE metric_id = 'percent_active_providers';
```

**Results:**
| period_date | organization_id | value |
|-------------|-----------------|-------|
| 2026-01-27 | (null) | 0.00 |
| 2026-01-27 | 526e3168-... | 0.00 |
| 2026-01-27 | 602ec278-... | 0.00 |
| 2026-01-27 | 8223aa6f-... | 0.00 |
| 2026-01-27 | 92dd11a8-... | 0.00 |

All values are 0% because no practitioners have activity in the last 30 days.

---

## Analysis

### Implementation Status: **IMPLEMENTED**

### Verification:
- Uses same `active_providers` source model as Indicator 01
- Correctly calculates percentage using numerator/denominator pattern
- Handles division by zero with `nullif`

### Issues Found:
1. **Same data quality issues as Indicator 01** - 66 practitioners with NULL organization_id
2. **Stale data** - all percentages are 0% due to no recent activity

---

## Recommendations
1. No changes needed to logic
2. Data freshness needs attention

---

## Checklist

- [x] SQL model created (uses `active_providers.sql`)
- [x] Included in metrics catalog
- [x] Outputs to `fct_metrics_long`
- [ ] Unit tests written
- [ ] Validated against sample data (data is stale)
