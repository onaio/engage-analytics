# ABOUTME: Analysis of Percent Follow-up indicator comparing spec to implementation
# ABOUTME: Includes test queries and findings from database validation

# Indicator 25: Percent Follow-up

## Specification Summary

| Field | Value |
|-------|-------|
| **Domain** | Follow-up |
| **Module** | All modules |
| **Indicator** | % follow-up |
| **Data Type** | Percent |
| **Frequency** | Not specified |
| **Description** | The percentage of clients who received the one month follow-up visit after completing an SPI, SBIRT or IPC |

### Formula
`(Number of clients with follow-up sessions / Number of clients with initial sessions) * 100`

### Open Questions
- Need to confirm what is the definition of 'follow up'. What is the time frame?

---

## Current Implementation

### Metric Status: **NOT IMPLEMENTED**

No metrics in the catalog track follow-up rates.

### Available Data

**Follow-up Table:**
- `qr_1_month_follow_up` - One month follow-up questionnaire

---

## Database Test Results

### Query: Follow-up Count
```sql
SELECT count(*) as follow_ups
FROM engage_analytics_engage_analytics_mart.qr_1_month_follow_up;
```

**Result:** 1 follow-up record

### Query: Follow-up Details
```sql
SELECT
  subject_patient_id,
  author_practitioner_id,
  qr_id
FROM engage_analytics_engage_analytics_mart.qr_1_month_follow_up;
```

Only 1 client has a follow-up recorded.

### Query: Clients Who Should Have Follow-ups
```sql
-- Clients who completed IPC session 4
SELECT count(distinct subject_patient_id)
FROM engage_analytics_engage_analytics_mart.qr_start_ipc_s4;
```

**Result:** 42 clients completed IPC S4

### Query: Follow-up Coverage
```sql
WITH ipc_completers AS (
  SELECT DISTINCT subject_patient_id
  FROM engage_analytics_engage_analytics_mart.qr_start_ipc_s4
),
follow_ups AS (
  SELECT DISTINCT subject_patient_id
  FROM engage_analytics_engage_analytics_mart.qr_1_month_follow_up
)
SELECT
  count(distinct c.subject_patient_id) as completers,
  count(distinct f.subject_patient_id) as with_followup,
  round(count(distinct f.subject_patient_id)::numeric /
        nullif(count(distinct c.subject_patient_id), 0) * 100, 1) as followup_pct
FROM ipc_completers c
LEFT JOIN follow_ups f USING (subject_patient_id);
```

**Calculated Result:**
| completers | with_followup | followup_pct |
|------------|---------------|--------------|
| 42 | ~1 | ~2.4% |

---

## Analysis

### Implementation Status: **NOT IMPLEMENTED**

### Key Findings:

1. **Extremely low follow-up rate**: Only 1 out of 42 IPC completers has a follow-up (2.4%)

2. **Data exists but is sparse**: The follow-up questionnaire exists in the system but is rarely used

3. **Multiple intervention types**: Spec mentions follow-up after SPI, SBIRT, or IPC - need to track all

### What's Needed:

1. **Define eligible population**: Clients who completed:
   - IPC Session 4
   - SPI Subform 4 (or appropriate completion point)
   - SBIRT (if multi-session, the final session)

2. **Time window logic**: "One month follow-up" implies:
   - Follow-up should occur ~30 days after intervention completion
   - May need to track "follow-up due date" vs "follow-up completed date"

3. **New metrics**:
   ```yaml
   - id: clients_eligible_for_followup
     description: "Clients who completed an intervention and are due for follow-up"

   - id: clients_with_followup
     description: "Clients who received 1-month follow-up"

   - id: followup_rate
     numerator: "clients_with_followup"
     denominator: "clients_eligible_for_followup"
     unit: percent
   ```

---

## Recommendations

1. **Investigate low follow-up rate**: Is this a data entry issue or a process gap?

2. **Create eligibility model**:
   ```sql
   -- Clients eligible for follow-up (completed intervention 30+ days ago)
   SELECT
     subject_patient_id,
     intervention_type,
     completion_date,
     completion_date + interval '30 days' as followup_due_date
   FROM (
     SELECT subject_patient_id, 'IPC' as intervention_type, max(authored_date) as completion_date
     FROM qr_start_ipc_s4
     GROUP BY 1
     UNION ALL
     -- Add SPI and SBIRT
   )
   ```

3. **Track timeliness**: Not just "did they get follow-up" but "was it within the expected window"

---

## Checklist

- [ ] Eligibility model created
- [ ] `clients_eligible_for_followup` metric implemented
- [ ] `clients_with_followup` metric implemented
- [ ] `followup_rate` metric implemented
- [ ] Time window logic implemented
- [ ] Unit tests written
- [x] Data validated (1 follow-up out of 42 eligible = 2.4%)
