# ABOUTME: Analysis of mwTool Outcomes indicator comparing spec to implementation
# ABOUTME: Includes test queries and findings from database validation

# Indicator 09: Outcome of Baseline mwTool

## Specification Summary

| Field | Value |
|-------|-------|
| **Domain** | Screening |
| **Module** | mwTool |
| **Indicator** | Outcome of baseline mwTool (4 possible outcomes) |
| **Data Type** | Count |
| **Frequency** | Monthly |
| **Description** | Outcome for clients who have completed baseline mwTool |
| **Disaggregation** | By user |

### Four Possible Outcomes
1. Severe mental disorders
2. Suicidality
3. Substance use
4. Common mental disorders

---

## Current Implementation

### Related Metrics in Catalog

Several metrics track mwTool-derived eligibility:

```yaml
- id: clients_eligible_for_ipc
  description: "Clients eligible for IPC (Integrated Primary Care)"
  # Based on no_mental_problem = false

- id: clients_sbirt_mi_eligible
  description: "Clients eligible for SBIRT/MI (alcohol or drug problems)"

- id: clients_eligible_spi
  description: "Clients eligible for SPI (Suicide Prevention Intervention)"

- id: clients_eligible_referral
  description: "Clients eligible for referral (severe mental health)"
```

### Source Data: `qr_mw_tool` View

**Available Outcome Columns:**
- `mental_wellness_tool_no_mental_problem`
- `mental_wellness_tool_common_mental_health`
- `mental_wellness_tool_severe_mental_health`
- `mental_wellness_tool_suicide_risk`
- `mental_wellness_tool_alcohol_problem`
- `mental_wellness_tool_drug_problem`

---

## Database Test Results

### Query: mwTool Outcome Distribution
```sql
SELECT
  mental_wellness_tool_no_mental_problem,
  mental_wellness_tool_common_mental_health,
  mental_wellness_tool_severe_mental_health,
  mental_wellness_tool_suicide_risk,
  mental_wellness_tool_alcohol_problem,
  mental_wellness_tool_drug_problem,
  count(*) as cnt
FROM engage_analytics_engage_analytics_mart.qr_mw_tool
GROUP BY 1,2,3,4,5,6;
```

**Results:**
| no_mental | common_mh | severe_mh | suicide | alcohol | drug | cnt |
|-----------|-----------|-----------|---------|---------|------|-----|
| NULL | NULL | NULL | NULL | NULL | NULL | 59 |

**CRITICAL ISSUE**: All 59 mwTool responses have NULL values for all outcome columns!

### Query: Check Raw Data for Extraction Issue
```sql
SELECT qr_id, subject_patient_id
FROM engage_analytics_engage_analytics_mart.qr_mw_tool
LIMIT 5;
```

The records exist but outcome columns aren't populated.

### Query: Clients Eligible for IPC (workaround)
```sql
SELECT count(distinct subject_patient_id)
FROM engage_analytics.clients_eligible_for_ipc
WHERE period_date = current_date;
```

**Result:** 50 clients eligible

---

## Analysis

### Implementation Status: **PARTIALLY IMPLEMENTED - DATA ISSUE**

### Critical Issues Found:

1. **NULL Outcome Columns**: The `qr_mw_tool` view has all outcome boolean columns as NULL. This suggests:
   - The column extraction macro isn't finding these fields
   - The linkId paths in the questionnaire may have changed
   - The FHIR structure differs from expected

2. **Workaround in Place**: The `clients_eligible_for_ipc.sql` model extracts the `no-mental-problem` value directly from JSON, bypassing the broken view columns:
   ```sql
   (jsonb_path_query_first(
     qr.items::jsonb,
     '$.**.item[*] ? (@.linkId == "no-mental-problem").answer[0].valueBoolean'
   ))::boolean as no_mental_problem
   ```

3. **Partial Coverage**: Related metrics exist but don't capture the 4 specific outcomes as individual counts.

### What Works:
- `clients_eligible_for_ipc` - correctly identifies clients with mental health issues
- `clients_sbirt_mi_eligible` - should identify substance use cases
- `clients_eligible_spi` - should identify suicide risk cases
- `clients_eligible_referral` - should identify severe cases

### What's Missing:
- Direct counts of each of the 4 mwTool outcomes
- Breakdown showing: "X clients scored for common MH, Y for severe MH, Z for suicide risk, W for substance use"

---

## Recommendations

1. **Fix qr_mw_tool view** - Debug why outcome columns are NULL:
   - Check the questionnaire response JSON structure
   - Verify linkIds match what's in the data
   - Update the macro or manual extraction

2. **Add explicit outcome counts metric**:
   ```yaml
   - id: mwtool_outcome_common_mh
     expression: "count(distinct case when common_mental_health = true then subject_patient_id end)"

   - id: mwtool_outcome_severe_mh
     expression: "count(distinct case when severe_mental_health = true then subject_patient_id end)"

   - id: mwtool_outcome_suicide_risk
     expression: "count(distinct case when suicide_risk = true then subject_patient_id end)"

   - id: mwtool_outcome_substance_use
     expression: "count(distinct case when alcohol_problem = true or drug_problem = true then subject_patient_id end)"
   ```

3. **Create summary metric** showing distribution across all 4 categories

---

## Checklist

- [x] SQL model exists (`qr_mw_tool` view)
- [ ] Outcome columns populated correctly (BLOCKED - all NULL)
- [x] Related eligibility metrics exist
- [ ] Direct outcome count metrics implemented
- [ ] Unit tests written
- [ ] Disaggregation by user implemented
