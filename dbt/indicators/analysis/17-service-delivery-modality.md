# ABOUTME: Analysis of Service Delivery Modality indicator comparing spec to implementation
# ABOUTME: Includes test queries and findings from database validation

# Indicator 17: Service Delivery Modality

## Specification Summary

| Field | Value |
|-------|-------|
| **Domain** | Treatment Initiated |
| **Module** | Each module's session |
| **Indicator** | Service delivery modality |
| **Data Type** | Count & Percent |
| **Description** | Service delivery modality: phone telehealth, video telehealth, in-person, or hybrid |

### Formula
- Numerator: Number of sessions of each modality in a month
- Denominator: Number of total sessions in a month

### FHIR Notes
Encounter tied to the Service delivery modality. Check modality in `Encounter.type`. Check client in `Encounter.subject`. Check form in `Encounter.reasonCode`.

---

## Current Implementation

### Metrics Implemented

```yaml
- id: encounters_in_person
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: encounters_by_delivery_format
  expression: "count(distinct case when format_you_deliver = 'In-person' then subject_patient_id end)"
  description: "Daily in-person encounters"

- id: encounters_video_telehealth
  expression: "count(distinct case when format_you_deliver = 'Video telehealth' then subject_patient_id end)"
  description: "Daily video telehealth encounters"

- id: encounters_phone_telehealth
  expression: "count(distinct case when format_you_deliver = 'Phone telehealth' then subject_patient_id end)"
  description: "Daily phone telehealth encounters"
```

### Source Model: `encounters_by_delivery_format.sql`

Extracts delivery format from questionnaire responses.

---

## Database Test Results

### Query: Delivery Modality Distribution
```sql
SELECT
  format_you_deliver,
  count(*) as cnt
FROM engage_analytics.encounters_by_delivery_format
GROUP BY 1
ORDER BY cnt DESC;
```

**Results:**
| format_you_deliver | cnt |
|--------------------|-----|
| In-person | 62 |
| Video telehealth | 58 |
| Phone telehealth | 11 |

**Total:** 131 sessions with delivery format recorded.

### Query: Metrics Output
```sql
SELECT period_date, metric_id, value
FROM engage_analytics.fct_metrics_long
WHERE metric_id LIKE 'encounters_%telehealth' OR metric_id = 'encounters_in_person'
ORDER BY period_date DESC, metric_id
LIMIT 15;
```

**Results:** 54 records per modality type, spanning 2024-05-08 to 2025-09-04.

### Query: Sample Day Breakdown
```sql
SELECT period_date, metric_id, value
FROM engage_analytics.fct_metrics_long
WHERE metric_id IN ('encounters_in_person', 'encounters_video_telehealth', 'encounters_phone_telehealth')
AND period_date = '2025-09-04';
```

| period_date | metric_id | value |
|-------------|-----------|-------|
| 2025-09-04 | encounters_in_person | 1 |
| 2025-09-04 | encounters_phone_telehealth | 0 |
| 2025-09-04 | encounters_video_telehealth | 0 |

---

## Analysis

### Implementation Status: **IMPLEMENTED**

### What Works Well:

1. **All three modalities tracked**: In-person, video telehealth, phone telehealth
2. **Daily granularity**: Can aggregate to monthly as needed
3. **Clean categorization**: Modality values are consistent strings

### Issues Found:

1. **No Percentage Metric**: Spec asks for both Count AND Percent. Currently only counts are implemented.

2. **Unique Clients vs Sessions**: Implementation counts `distinct subject_patient_id` (unique clients per modality per day), not total sessions.

3. **Missing "Hybrid" Option**: Spec mentions "hybrid" as a modality but it's not in the data or metrics.

4. **No Organization Disaggregation**: All records show NULL organization_id in source data.

---

## Recommendations

1. **Add percentage metrics**:
   ```yaml
   - id: pct_encounters_in_person
     numerator: "count(distinct case when format_you_deliver = 'In-person' then subject_patient_id end)"
     denominator: "nullif(count(distinct subject_patient_id), 0)"
     description: "Percentage of encounters delivered in-person"
   ```

2. **Clarify count definition**: If we want total sessions (not unique clients), change to `count(*)` instead of `count(distinct subject_patient_id)`

3. **Investigate hybrid**: Check if "hybrid" exists in the questionnaire options or if it's a combination that needs calculation

---

## Checklist

- [x] SQL model created (`encounters_by_delivery_format.sql`)
- [x] Count metrics implemented (3 modalities)
- [x] Outputs to `fct_metrics_long`
- [ ] Percentage metrics implemented
- [ ] Hybrid modality handled
- [ ] Unit tests written
- [x] Data validated (131 records with delivery format)
