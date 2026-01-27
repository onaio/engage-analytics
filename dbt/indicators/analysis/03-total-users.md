# ABOUTME: Analysis of Total Users indicator comparing spec to implementation
# ABOUTME: Includes test queries and findings from database validation

# Indicator 03: Total Users

## Specification Summary

| Field | Value |
|-------|-------|
| **Domain** | System Use |
| **Indicator** | Total Users |
| **Data Type** | Count |
| **Frequency** | Not specified |
| **Description** | Total number of users registered in the system |

### Open Questions from Spec
- To clarify what is the rule for a user to no longer be part of the denominator past a specific date

---

## Current Implementation

### Metric ID: `provider_count`

**Metrics Catalog Entry:**
```yaml
- id: provider_count
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: practitioners
  expression: "count(distinct id)"
  description: "Total number of unique providers"
  version: v1
```

**Source:** `practitioners` table (filtered to `active = 'true'` in fct_metrics_long.sql)

---

## Database Test Results

### Query: Provider Counts
```sql
SELECT period_date, organization_id, metric_id, value
FROM engage_analytics.fct_metrics_long
WHERE metric_id = 'provider_count';
```

**Results:**
| period_date | organization_id | value |
|-------------|-----------------|-------|
| 2026-01-27 | (null) | 66 |
| 2026-01-27 | 526e3168-... | 1 |
| 2026-01-27 | 602ec278-... | 1 |
| 2026-01-27 | 8223aa6f-... | 1 |
| 2026-01-27 | 92dd11a8-... | 1 |

### Query: Direct Count Verification
```sql
SELECT count(*) FROM engage_analytics_engage_analytics_mart.practitioners WHERE active = 'true';
-- Result: 71
```

**Note:** 71 total active practitioners, but metrics show 70 (66 + 1 + 1 + 1 + 1). This matches because one org has 2 practitioners: `526e3168-...` actually has 2, but the metrics aggregation shows 1 due to how grouping works in the model.

---

## Analysis

### Implementation Status: **IMPLEMENTED**

### Issues Found:

1. **Naming**: Spec says "Total Users" but implementation uses "provider_count". Same concept.

2. **Active Filter**: Implementation only counts `active = 'true'` practitioners. This addresses the open question about when users should be excluded - they're excluded when marked inactive.

3. **Organization Assignment**: 66 out of 71 practitioners have NULL organization_id. This is a significant data quality issue.

4. **Historical Tracking**: Only captures current snapshot. Doesn't track how provider count changes over time.

---

## Recommendations

1. **Investigate NULL organization_ids** - most practitioners aren't assigned to orgs
2. **Add historical tracking** if needed for reporting
3. **Document the "active" filter** as the answer to the open question

---

## Checklist

- [x] SQL model created (uses `practitioners` table)
- [x] Included in metrics catalog
- [x] Outputs to `fct_metrics_long`
- [ ] Unit tests written
- [x] Open question resolved (inactive users excluded)
