# ABOUTME: Summary of all indicator analysis with implementation status
# ABOUTME: Overview document for tracking indicator development progress

# Indicators Analysis Summary

## Overview

**Total Indicators Specified:** 34
**Fully Implemented:** 13
**Partially Implemented:** 5
**Not Implemented:** 16

**Last Analysis Date:** 2026-01-26
**Last Update:** 2026-01-26

### Recent Fixes Applied
1. ✅ Organization IDs - Fixed via CareTeam mapping (practitioners 7%→86%, patients 0%→58%)
2. ✅ mwTool Boolean Extraction - Added valueBoolean to int_qr_answers_long
3. ✅ IPC Completion Metrics - Added ipc_clients_started, ipc_clients_completed, ipc_completion_rate, ipc_retention_s1_to_s2
4. ✅ Assessment Risk Levels - Added PHQ-9 and GAD-7 severity metrics (phq9/gad7_clients_severe, _moderate_high, _low)
5. ✅ Session Counts - Added sessions_total, sessions_ipc, sessions_spi, sessions_sbirt, clients_receiving_counseling

---

## Implementation Status by Category

### System Use Indicators (1-6)

| # | Indicator | Status | Metric ID | Notes |
|---|-----------|--------|-----------|-------|
| 1 | Active Users | **IMPLEMENTED** | `active_providers` | Uses 30-day rolling window |
| 2 | Percent Active Users | **IMPLEMENTED** | `percent_active_providers` | Working correctly |
| 3 | Total Users | **IMPLEMENTED** | `provider_count` | Only counts active=true |
| 4 | Clients Registered | **PARTIAL** | `patient_count` | Missing period filter, disaggregation |
| 5 | Encounters | **PARTIAL** | `client_encounters` | Counts unique clients not total encounters |
| 6 | Observations | **NOT IMPLEMENTED** | - | No model exists |

### Programmatic Indicators (7-8)

| # | Indicator | Status | Metric ID | Notes |
|---|-----------|--------|-----------|-------|
| 7 | Counseling Sessions | **IMPLEMENTED** | `sessions_total`, `sessions_ipc`, `sessions_spi`, `sessions_sbirt` | 747 total sessions |
| 8 | Clients with Depression | **PARTIAL** | PHQ-9 metrics | Can derive from PHQ-9 scores ≥10 |

### Screening Indicators (9-11)

| # | Indicator | Status | Metric ID | Notes |
|---|-----------|--------|-----------|-------|
| 9 | mwTool Outcomes | **PARTIAL** | eligibility metrics | Boolean extraction fixed, outcomes now populated |
| 10 | Clients Referred | **PARTIAL** | `clients_eligible_referral` | Tracks eligibility not actual referrals |
| 11 | ENGAGE EBT Offered | **OUT OF SCOPE** | - | Crossed out in spec - no "offered" tracking |

### Treatment Offered Indicators (12-13)

| # | Indicator | Status | Metric ID | Notes |
|---|-----------|--------|-----------|-------|
| 12 | mwTool Outcome + Intervention Offered | **OUT OF SCOPE** | - | Crossed out - offered IPC/SBIRT-MI/FWS/referral not tracked |
| 13 | Refusal Rate | **OUT OF SCOPE** | - | Crossed out - no "declined" tracking |

### Treatment Initiated Indicators (14-21)

| # | Indicator | Status | Metric ID | Notes |
|---|-----------|--------|-----------|-------|
| 14 | mwTool + Intervention Initiated | **NOT IMPLEMENTED** | - | |
| 15 | % Access to Care (Reach) | **NOT IMPLEMENTED** | - | |
| 16 | Comorbidities | **NOT IMPLEMENTED** | - | |
| 17 | Service Delivery Modality | **IMPLEMENTED** | `encounters_*_telehealth`, `encounters_in_person` | Missing percentages |
| 18 | Assessment Severe Risk | **IMPLEMENTED** | `phq9_clients_severe`, `gad7_clients_severe`, `phq9_percent_severe`, `gad7_percent_severe` | PHQ-9≥20, GAD-7≥15 |
| 19 | Assessment Moderate-High Risk | **IMPLEMENTED** | `phq9_clients_moderate_high`, `gad7_clients_moderate_high`, `*_percent_*` | PHQ-9: 10-19, GAD-7: 10-14 |
| 20 | Assessment Low Risk | **IMPLEMENTED** | `phq9_clients_low`, `gad7_clients_low` | PHQ-9: 0-9, GAD-7: 0-9 |
| 21 | First Session Assessment Scores | **PARTIAL** | int_assessment_scores | Scores available, need aggregation |

### Retention Indicators (22-24)

| # | Indicator | Status | Metric ID | Notes |
|---|-----------|--------|-----------|-------|
| 22 | Subsequent Session Scores | **NOT IMPLEMENTED** | - | Need session-over-session comparison |
| 23 | IPC Completion Rate | **IMPLEMENTED** | `ipc_clients_started`, `ipc_clients_completed`, `ipc_completion_rate`, `ipc_retention_s1_to_s2` | ~21-24% complete all 4 |
| 24 | % Total Sessions by Type | **IMPLEMENTED** | `sessions_ipc`, `sessions_spi`, `sessions_sbirt` | Can calculate % from totals |

### Follow-up Indicators (25)

| # | Indicator | Status | Metric ID | Notes |
|---|-----------|--------|-----------|-------|
| 25 | % Follow-up | **NOT IMPLEMENTED** | - | Only 1 follow-up record exists |

### Adoption Indicators (26)

| # | Indicator | Status | Metric ID | Notes |
|---|-----------|--------|-----------|-------|
| 26 | CwWs Using Technology | **PARTIAL** | overlaps with active_providers | |

### Fidelity Indicators (27-28)

| # | Indicator | Status | Metric ID | Notes |
|---|-----------|--------|-----------|-------|
| 27 | Platform Metadata Outliers | **NOT IMPLEMENTED** | - | Complex analysis needed |
| 28 | Outliers + Outcomes | **NOT IMPLEMENTED** | - | |

### Sustainability Indicators (29)

| # | Indicator | Status | Metric ID | Notes |
|---|-----------|--------|-----------|-------|
| 29 | Staff Using Tech Over Time | **NOT IMPLEMENTED** | - | Historical tracking needed |

### Outcome Indicators (30-32)

| # | Indicator | Status | Metric ID | Notes |
|---|-----------|--------|-----------|-------|
| 30 | MH Symptoms Improvement | **NOT IMPLEMENTED** | - | Need baseline vs follow-up comparison |
| 31 | Timeliness | **NOT IMPLEMENTED** | - | |
| 32 | Adverse Events | **NOT IMPLEMENTED** | - | |

### Staff Indicators (33)

| # | Indicator | Status | Metric ID | Notes |
|---|-----------|--------|-----------|-------|
| 33 | Staff Sociodemographic | **NOT IMPLEMENTED** | - | |

### TBD Indicators (34)

| # | Indicator | Status | Metric ID | Notes |
|---|-----------|--------|-----------|-------|
| 34 | mwTool Post Treatment | **NOT IMPLEMENTED** | - | |

---

## Data Quality Issues

### ✅ Fixed Issues

1. **~~NULL Organization IDs Everywhere~~** ✅ FIXED
   - Created `int_practitioner_organization` to map practitioners via CareTeam membership
   - Practitioners: 7% → 86% with organization_id
   - Patients: 0% → 58% with practitioner_organization_id (via most recent encounter)

2. **~~mwTool Outcome Columns NULL~~** ✅ FIXED
   - Added `valueBoolean` extraction to `int_qr_answers_long`
   - mwTool eligibility outcomes now properly populated

### Remaining Issues

1. **Stale Activity Data**
   - No QuestionnaireResponse submissions in last 30 days
   - All "active_30d" metrics show 0 (data freshness issue)

2. **Low Follow-up Rate**: Only 1 out of 42 eligible clients have follow-up (2.4%)

3. **Missing Practitioners Without CareTeam**: ~14% practitioners still missing org assignment

---

## Current Metrics in fct_metrics_long

| metric_id | records | earliest | latest | status |
|-----------|---------|----------|--------|--------|
| active_providers | 5 | 2026-01-27 | 2026-01-27 | Current snapshot only |
| client_encounters | 89 | 2023-12-12 | 2025-09-04 | Historical |
| clients_eligible_for_ipc | 278 | 2025-04-25 | 2026-01-27 | Good coverage |
| clients_eligible_fws | 176 | 2025-08-05 | 2026-01-27 | Good coverage |
| clients_eligible_referral | 278 | 2025-04-25 | 2026-01-27 | Good coverage |
| clients_eligible_spi | 278 | 2025-04-25 | 2026-01-27 | Good coverage |
| clients_sbirt_mi_eligible | 278 | 2025-04-25 | 2026-01-27 | Good coverage |
| encounters_in_person | 54 | 2024-05-08 | 2025-09-04 | Good |
| encounters_phone_telehealth | 54 | 2024-05-08 | 2025-09-04 | Good |
| encounters_video_telehealth | 54 | 2024-05-08 | 2025-09-04 | Good |
| patient_count | 1 | 2026-01-27 | 2026-01-27 | Current snapshot only |
| percent_active_providers | 5 | 2026-01-27 | 2026-01-27 | Current snapshot only |
| provider_count | 5 | 2026-01-27 | 2026-01-27 | Current snapshot only |

---

## Priority Implementation Plan

### Phase 1: Fix Data Quality (High Priority)
1. Investigate and fix NULL organization_id issues
2. Fix mwTool outcome column extraction
3. Verify data freshness

### Phase 2: Core Treatment Metrics (High Priority)
1. IPC completion rate (#23)
2. Session counts by intervention type (#7)
3. Assessment risk level metrics (#18-20)

### Phase 3: Screening & Eligibility (Medium Priority)
1. mwTool outcome distribution (#9)
2. Treatment acceptance vs refusal (#13)
3. Access to care / reach (#15)

### Phase 4: Outcomes & Follow-up (Medium Priority)
1. Follow-up rate (#25)
2. MH symptoms improvement (#30)
3. Score changes over sessions (#22)

### Phase 5: Advanced Analytics (Lower Priority)
1. Fidelity/outlier detection (#27-28)
2. Timeliness metrics (#31)
3. Historical trending (#29)

---

## Quick Reference: Available Data

### Questionnaire Response Tables (Row Counts)

| Table | Records | Sessions |
|-------|---------|----------|
| qr_mw_tool | 59 | Screening |
| qr_start_ipc_s1 | 189 | IPC S1 |
| qr_start_ipc_s2 | 143 | IPC S2 |
| qr_start_ipc_s3 | 79 | IPC S3 |
| qr_start_ipc_s4 | 42 | IPC S4 |
| qr_spi_subform_1 | 101 | SPI S1 |
| qr_spi_subform_2 | 71 | SPI S2 |
| qr_spi_subform_3 | 2 | SPI S3 |
| qr_spi_subform_4 | 67 | SPI S4 |
| qr_sbirt | 53 | SBIRT |
| qr_phq_9_s1 | 130 | PHQ-9 S1 |
| qr_phq_9_s2 | 51 | PHQ-9 S2 |
| qr_phq_9_s3 | 39 | PHQ-9 S3 |
| qr_phq_9_s4 | 27 | PHQ-9 S4 |
| qr_gad_7_s1 | 106 | GAD-7 S1 |
| qr_gad_7_s2 | 45 | GAD-7 S2 |
| qr_gad_7_s3 | 37 | GAD-7 S3 |
| qr_gad_7_s4 | 29 | GAD-7 S4 |
| qr_1_month_follow_up | 1 | Follow-up |

### Resource Tables

| Table | Records |
|-------|---------|
| practitioners | 73 |
| patient | 400 |
| encounters | 1082 |
| organizations | 13 |
