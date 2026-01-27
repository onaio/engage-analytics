# ABOUTME: Analysis of Assessment Risk Level indicators (severe, moderate-high, low)
# ABOUTME: Covers indicators 18, 19, 20 for PHQ-9, GAD-7, DAST, C-SSRS, AUDIT scores

# Indicators 18-20: Assessment Risk Levels

## Specification Summary

| # | Indicator | Description |
|---|-----------|-------------|
| 18 | Assessment Severe Risk | % clients scoring severe in mwTool, PHQ-9, GAD-7, DAST, C-SSRS, AUDIT |
| 19 | Assessment Moderate-High Risk | % clients scoring moderate (not severe) |
| 20 | Assessment Low Risk | % clients scoring low (not moderate or severe) |

### Data Type
Percent

### FHIR Notes
Observation tied to risk level. Check:
- `Observation.interpretation` for risk level meaning
- `Observation.valueQuantity` for risk level numeric
- `Observation.code` for which form
- `Observation.subject` for which client

---

## Current Implementation

### Metric Status: **NOT IMPLEMENTED**

No metrics currently calculate risk level distributions.

### Available Assessment Data

**PHQ-9 (Depression)**
- Tables: `qr_phq_9_s1` through `qr_phq_9_s4`
- Severity thresholds: 0-4 minimal, 5-9 mild, 10-14 moderate, 15-19 mod-severe, 20-27 severe

**GAD-7 (Anxiety)**
- Tables: `qr_gad_7_s1` through `qr_gad_7_s4`
- Severity thresholds: 0-4 minimal, 5-9 mild, 10-14 moderate, 15-21 severe

**Other Assessments**
- `qr_sf_pcl_5_s1-s4` - PC-PTSD (SF-PCL-5)
- `qr_mood_rating_s1-s4` - Mood Rating Scale
- `qr_sbirt` - Contains DAST/AUDIT items

---

## Database Test Results

### Query: Check PHQ-9 Column Structure
```sql
SELECT column_name
FROM information_schema.columns
WHERE table_name = 'qr_phq_9_s1'
AND column_name LIKE '%common_mental%';
```

**PHQ-9 Question Columns:**
- `common_mental_health_symptoms_little_interest_or_pleasure_in__2`
- `common_mental_health_symptoms_feeling_down_depressed_or_hopel_2`
- `common_mental_health_symptoms_trouble_falling_or_staying_asle_2`
- `common_mental_health_symptoms_feeling_tired_or_having_little__2`
- `common_mental_health_symptoms_poor_appetite_or_overeating_2`
- `common_mental_health_symptoms_feeling_bad_about_yourself_or_t_2`
- `common_mental_health_symptoms_trouble_concentrating_on_things_2`
- `common_mental_health_symptoms_moving_or_speaking_so_slowly_th_2`
- `common_mental_health_symptoms_thoughts_that_you_would_be_bett_2`

**Note:** No pre-calculated total score column exists!

### Query: Sample PHQ-9 Values
```sql
SELECT
  "common_mental_health_symptoms_little_interest_or_pleasure_in__2" as q1,
  "common_mental_health_symptoms_feeling_down_depressed_or_hopel_2" as q2
FROM engage_analytics_engage_analytics_mart.qr_phq_9_s1
LIMIT 5;
```

Values are text responses that need to be mapped to numeric scores.

---

## Analysis

### Implementation Status: **NOT IMPLEMENTED**

### Key Challenges:

1. **No Total Score Calculated**: Individual question responses exist but no sum/total score

2. **Text to Numeric Mapping Needed**: Response values are text (e.g., "Not at all", "Several days", etc.) that need mapping:
   - "Not at all" = 0
   - "Several days" = 1
   - "More than half the days" = 2
   - "Nearly every day" = 3

3. **Multiple Assessments**: Need to handle PHQ-9, GAD-7, DAST, AUDIT, C-SSRS each with different scoring rules

4. **Session Aggregation**: Multiple sessions per client - need to decide:
   - First session only?
   - Most recent?
   - Any session with severe score?

### What's Needed:

1. **Scoring Intermediate Model**: `int_assessment_scores.sql`
   ```sql
   WITH phq9_scored AS (
     SELECT
       subject_patient_id,
       qr_id,
       -- Map each question to numeric and sum
       (CASE q1 WHEN 'Not at all' THEN 0 WHEN 'Several days' THEN 1 ... END) +
       (CASE q2 ...) + ... as phq9_total,
       CASE
         WHEN phq9_total >= 20 THEN 'severe'
         WHEN phq9_total >= 15 THEN 'moderately_severe'
         WHEN phq9_total >= 10 THEN 'moderate'
         WHEN phq9_total >= 5 THEN 'mild'
         ELSE 'minimal'
       END as phq9_severity
     FROM qr_phq_9_s1
   )
   ```

2. **Risk Level Metrics**:
   ```yaml
   - id: pct_clients_severe_risk
     numerator: "clients with ANY assessment at severe level"
     denominator: "total clients assessed"

   - id: pct_clients_moderate_risk
     numerator: "clients with highest level = moderate (not severe)"
     denominator: "total clients assessed"

   - id: pct_clients_low_risk
     numerator: "clients with all assessments at low/minimal"
     denominator: "total clients assessed"
   ```

---

## Standard Scoring Thresholds

### PHQ-9 (Depression)
| Score | Severity |
|-------|----------|
| 0-4 | Minimal |
| 5-9 | Mild |
| 10-14 | Moderate |
| 15-19 | Moderately Severe |
| 20-27 | Severe |

### GAD-7 (Anxiety)
| Score | Severity |
|-------|----------|
| 0-4 | Minimal |
| 5-9 | Mild |
| 10-14 | Moderate |
| 15-21 | Severe |

### DAST-10 (Drug Use)
| Score | Severity |
|-------|----------|
| 0 | No problems |
| 1-2 | Low level |
| 3-5 | Moderate |
| 6-8 | Substantial |
| 9-10 | Severe |

### AUDIT (Alcohol Use)
| Score | Risk Level |
|-------|------------|
| 0-7 | Low risk |
| 8-15 | Risky/hazardous |
| 16-19 | High risk/harmful |
| 20+ | Possible dependence |

---

## Recommendations

1. **Create scoring macros** for each assessment type:
   ```jinja
   {% macro score_phq9(prefix) %}
     (CASE {{ prefix }}_q1 WHEN 'Not at all' THEN 0 ... END) + ...
   {% endmacro %}
   ```

2. **Build intermediate model** that calculates all scores

3. **Add risk level classification** columns

4. **Create aggregate metrics** for risk distribution

5. **Consider longitudinal tracking** - score changes over sessions

---

## Checklist

- [ ] PHQ-9 scoring model created
- [ ] GAD-7 scoring model created
- [ ] DAST-10 scoring model created
- [ ] AUDIT scoring model created
- [ ] C-SSRS risk classification created
- [ ] Combined risk level model created
- [ ] `pct_clients_severe_risk` metric implemented
- [ ] `pct_clients_moderate_risk` metric implemented
- [ ] `pct_clients_low_risk` metric implemented
- [ ] Unit tests for scoring logic
- [ ] Validated against clinical thresholds
