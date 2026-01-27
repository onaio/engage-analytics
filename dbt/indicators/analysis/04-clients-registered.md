# ABOUTME: Analysis of Clients Registered indicator comparing spec to implementation
# ABOUTME: Includes test queries and findings from database validation

# Indicator 04: Clients Registered

## Specification Summary

| Field | Value |
|-------|-------|
| **Domain** | System Use |
| **Indicator** | Clients registered |
| **Data Type** | Count |
| **Frequency** | Monthly |
| **Description** | Number of Clients registered |
| **Disaggregation** | By user, by clinic, location (inside/outside clinic) |

### Formula
`# Clients registered in the period`

---

## Current Implementation

### Metric ID: `patient_count`

**Metrics Catalog Entry:**
```yaml
- id: patient_count
  unit: count
  grain: day
  entity_keys: [practitioner_organization_id]
  source_model: patient
  expression: "count(distinct id)"
  description: "Total number of unique patients"
  version: v1
```

**Note:** Uses `practitioner_organization_id` as entity key (from patient table).

---

## Database Test Results

### Query: Patient Count from Metrics
```sql
SELECT period_date, organization_id, metric_id, value
FROM engage_analytics.fct_metrics_long
WHERE metric_id = 'patient_count';
```

**Results:**
| period_date | organization_id | value |
|-------------|-----------------|-------|
| 2026-01-27 | (null) | 400 |

Only one row with NULL organization - all patients grouped together.

### Query: Direct Patient Count
```sql
SELECT
  practitioner_organization_id,
  count(*) as patient_count
FROM engage_analytics_engage_analytics_mart.patient
WHERE active = 'true'
GROUP BY 1;
```

**Results:**
| practitioner_organization_id | patient_count |
|------------------------------|---------------|
| (null) | 400 |

All 400 patients have NULL `practitioner_organization_id`.

### Query: Patient Table Structure
```sql
SELECT column_name FROM information_schema.columns
WHERE table_name = 'patient' AND table_schema = 'engage_analytics_engage_analytics_mart';
```

Available columns include:
- `practitioner_id`
- `practitioner_organization_id`
- `practitioner_careteam_id`
- `registration_date`
- `location_id`

---

## Analysis

### Implementation Status: **PARTIALLY IMPLEMENTED**

### Issues Found:

1. **Missing Organization Assignment**: All 400 patients have NULL `practitioner_organization_id`. This breaks the disaggregation by clinic/organization.

2. **No Period Filtering**: Current implementation counts ALL patients, not patients registered in a specific period. Should use `registration_date` to filter by period.

3. **Disaggregation Not Implemented**:
   - By user (practitioner): `practitioner_id` available but not used
   - By clinic: `practitioner_organization_id` available but all NULL
   - By location: `location_id` available but not used in metrics

4. **Total vs. Period Count**: Spec asks for "Clients registered in the period" but implementation gives total count.

---

## Recommendations

1. **Fix patient-organization relationship** - investigate why `practitioner_organization_id` is NULL for all patients

2. **Add period-based filtering** - create a version that counts new registrations per period:
   ```sql
   count(distinct case when registration_date >= period_start
                       and registration_date < period_end
                  then id end)
   ```

3. **Add disaggregation dimensions**:
   - By practitioner
   - By organization (once data quality fixed)
   - By location

---

## Checklist

- [x] SQL model exists (`patient` table used)
- [x] Included in metrics catalog
- [x] Outputs to `fct_metrics_long`
- [ ] Unit tests written
- [ ] Period filtering implemented
- [ ] Disaggregation by user implemented
- [ ] Disaggregation by clinic implemented
- [ ] Disaggregation by location implemented
- [ ] Data quality issue resolved (NULL organization_ids)
