# ABOUTME: Analysis of Encounters indicator comparing spec to implementation
# ABOUTME: Includes test queries and findings from database validation

# Indicator 05: Encounters

## Specification Summary

| Field | Value |
|-------|-------|
| **Domain** | System Use |
| **Indicator** | Encounters |
| **Data Type** | Count |
| **Frequency** | Monthly |
| **Description** | Number of Client encounters |
| **Disaggregation** | By user, by clinic, by encounter type |

### FHIR Notes
Encounter tied to how many times the client has been encountered by the Practitioner. Check which client related to this encounter in `Encounter.subject`. Total number of client encounters during the previous month.

---

## Current Implementation

### Metric ID: `client_encounters`

**Metrics Catalog Entry:**
```yaml
- id: client_encounters
  unit: count
  grain: day
  entity_keys: [practitioner_organization_id]
  source_model: encounters
  expression: "count(distinct subject_id)"
  description: "Total unique clients seen (one encounter per patient per day)"
  version: v1
```

**Note:** Counts UNIQUE clients per day, not total encounters.

---

## Database Test Results

### Query: Encounter Metrics
```sql
SELECT period_date, organization_id, metric_id, value
FROM engage_analytics.fct_metrics_long
WHERE metric_id = 'client_encounters'
ORDER BY period_date DESC
LIMIT 10;
```

**Results:**
| period_date | organization_id | value |
|-------------|-----------------|-------|
| 2025-09-04 | (null) | 1 |
| 2025-09-03 | (null) | 2 |
| 2025-08-29 | (null) | 1 |
| ... | ... | ... |

89 total records spanning 2023-12-12 to 2025-09-04.

### Query: Direct Encounter Counts
```sql
SELECT
  count(*) as total_encounters,
  count(distinct subject_id) as unique_clients,
  min(period_start::date) as earliest,
  max(period_start::date) as latest
FROM engage_analytics_engage_analytics_mart.encounters;
```

**Results:**
| total_encounters | unique_clients | earliest | latest |
|------------------|----------------|----------|--------|
| 1082 | 289 | 2023-12-12 | 2025-09-04 |

### Query: Encounters by Organization
```sql
SELECT
  practitioner_organization_id,
  count(*) as encounter_count
FROM engage_analytics_engage_analytics_mart.encounters
GROUP BY 1;
```

**Results:**
| practitioner_organization_id | encounter_count |
|------------------------------|-----------------|
| (null) | 1082 |

All encounters have NULL organization.

---

## Analysis

### Implementation Status: **PARTIALLY IMPLEMENTED**

### Issues Found:

1. **Definition Mismatch**:
   - Spec: "Number of Client encounters" (total encounters)
   - Implementation: "count(distinct subject_id)" (unique clients per day)
   - These are different metrics!

2. **Missing Organization Data**: All 1082 encounters have NULL `practitioner_organization_id`.

3. **Disaggregation Not Implemented**:
   - By user (practitioner): `practitioner_id` available
   - By clinic: organization data missing
   - By encounter type: `reasoncode`, `class_code`, `service_display` available but not used

4. **Good date range**: Data spans ~2 years of encounters.

---

## Recommendations

1. **Clarify metric definition**:
   - If we want total encounters: `count(*)` or `count(distinct id)`
   - If we want unique clients seen: current implementation is correct but should be renamed

2. **Fix organization assignment** in encounters table

3. **Add disaggregation metrics**:
   ```yaml
   - id: encounters_by_type
     expression: "count(*)"
     entity_keys: [practitioner_organization_id, reasoncode]
   ```

4. **Consider adding**:
   - `total_encounters` metric (raw count)
   - `encounters_by_practitioner` disaggregation

---

## Available Columns for Disaggregation

| Column | Description | Sample Values |
|--------|-------------|---------------|
| `practitioner_id` | Provider who conducted encounter | UUID |
| `reasoncode` | Encounter reason/form type | Form names |
| `class_code` | Encounter class | AMB, etc. |
| `service_display` | Service description | Text |

---

## Checklist

- [x] SQL model exists (`encounters` table)
- [x] Included in metrics catalog
- [x] Outputs to `fct_metrics_long`
- [ ] Unit tests written
- [ ] Definition clarified (total vs unique clients)
- [ ] Disaggregation by user implemented
- [ ] Disaggregation by clinic implemented
- [ ] Disaggregation by encounter type implemented
- [ ] Data quality issue resolved (NULL organization_ids)
