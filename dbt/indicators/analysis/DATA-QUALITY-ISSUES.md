# ABOUTME: Critical data quality issues blocking indicator implementation
# ABOUTME: Action items for fixing data problems before metrics can work correctly

# Data Quality Issues

## Critical Issues

### 1. NULL Organization IDs Everywhere

**Impact:** SEVERE - Breaks all organization-level disaggregation

**Findings:**

| Table | Total Records | NULL org_id | % NULL |
|-------|---------------|-------------|--------|
| practitioners | 71 | 66 | 93% |
| patient | 400 | 400 | 100% |
| encounters | 1082 | 1082 | 100% |

**Affected Indicators:**
- All indicators requiring "by clinic" or "by organization" disaggregation
- Metrics grouped by `organization_id` show all data under NULL

**Investigation Queries:**
```sql
-- Check if organization data exists in source
SELECT count(*) FROM engage_analytics_engage_analytics_mart.organizations;
-- Result: 13 organizations exist

-- Check practitioners with orgs
SELECT organization_id, count(*)
FROM engage_analytics_engage_analytics_mart.practitioners
GROUP BY 1;
-- Result: 66 NULL, 1-2 each for 4 orgs
```

**Root Cause Hypothesis:**
- Organization assignment may happen through PractitionerRole, not direct field
- Patient organization comes from linked practitioner
- Join logic may be failing

**Action Items:**
- [ ] Review `patient.sql` model - check how `practitioner_organization_id` is populated
- [ ] Review `encounters.sql` model - same issue
- [ ] Check if organization should come from CareTeam or PractitionerRole
- [ ] Verify source FHIR data has organization references

---

### 2. mwTool Outcome Columns All NULL

**Impact:** HIGH - Blocks mwTool outcome distribution metrics

**Findings:**
```sql
SELECT
  mental_wellness_tool_no_mental_problem,
  mental_wellness_tool_common_mental_health,
  mental_wellness_tool_severe_mental_health,
  mental_wellness_tool_suicide_risk,
  mental_wellness_tool_alcohol_problem,
  mental_wellness_tool_drug_problem,
  count(*)
FROM engage_analytics_engage_analytics_mart.qr_mw_tool
GROUP BY 1,2,3,4,5,6;
```

**Result:** All 59 records have NULL for all 6 outcome columns

**Workaround Exists:**
The `clients_eligible_for_ipc.sql` model successfully extracts `no-mental-problem` using:
```sql
(jsonb_path_query_first(
  qr.items::jsonb,
  '$.**.item[*] ? (@.linkId == "no-mental-problem").answer[0].valueBoolean'
))::boolean as no_mental_problem
```

**Root Cause Hypothesis:**
- The `qr_mw_tool` view uses a macro to extract columns
- The linkIds for these fields may not match what's in the JSON
- The questionnaire structure may have changed

**Action Items:**
- [ ] Check actual linkIds in raw QuestionnaireResponse JSON
- [ ] Compare to what the extraction macro expects
- [ ] Update the `qr_mw_tool.sql` model or fix the macro
- [ ] Apply similar fix to other broken outcome fields

---

### 3. No Recent Activity Data (Stale Data)

**Impact:** MEDIUM - Current metrics show 0% active users

**Findings:**
```sql
SELECT max(meta_lastupdated)
FROM airbyte_internal.questionnaire_response;
-- Most recent QR submission: likely months old
```

All `active_30d` calculations return 0 because no QuestionnaireResponse records exist within the 30-day window.

**Note:** This may be expected if:
- This is a test/development database
- Data sync hasn't run recently
- The production system has newer data

**Action Items:**
- [ ] Verify this is expected (test data vs production)
- [ ] If production, check Airbyte sync status
- [ ] Consider adding data freshness monitoring

---

## Moderate Issues

### 4. Missing Assessment Total Scores

**Impact:** MEDIUM - Requires building scoring logic

**Findings:**
PHQ-9, GAD-7, and other assessment tables have individual question columns but no calculated total score.

**Required Work:**
- Build scoring models that sum question responses
- Map text values to numeric scores
- Apply clinical severity thresholds

**Action Items:**
- [ ] Create `int_phq9_scores.sql` with scoring logic
- [ ] Create `int_gad7_scores.sql` with scoring logic
- [ ] Add severity classification

---

### 5. Very Low Follow-up Rate

**Impact:** LOW (data issue, not technical issue)

**Findings:**
- 42 clients completed IPC Session 4
- Only 1 client has a 1-month follow-up record
- 2.4% follow-up rate

**This may indicate:**
- Follow-up process not being followed
- Follow-up data not being entered
- Or simply a new process not yet adopted

**Action Items:**
- [ ] Discuss with program team whether this is expected
- [ ] Consider adding alerts for missing follow-ups

---

## Data Quality Monitoring Recommendations

1. **Add data freshness checks:**
   ```sql
   SELECT
     'questionnaire_response' as table_name,
     max(meta_lastupdated) as latest_record,
     current_date - max(meta_lastupdated)::date as days_stale
   FROM stg_questionnaire_response;
   ```

2. **Add NULL rate monitoring:**
   ```sql
   SELECT
     'practitioners.organization_id' as field,
     count(*) filter (where organization_id is null)::float / count(*) as null_rate
   FROM practitioners;
   ```

3. **Add completeness checks for critical fields:**
   - Patient has practitioner_organization_id
   - Encounters have practitioner_organization_id
   - mwTool has outcome booleans populated

---

## Priority Fix Order

1. **Organization ID assignment** - Unblocks all org-level reporting
2. **mwTool outcome extraction** - Unblocks screening metrics
3. **Assessment scoring** - Enables risk level metrics
4. **Data freshness** - Verify sync is working
