# ABOUTME: Engage Analytics Indicators Reference
# ABOUTME: Detailed specification for all metrics/indicators with implementation notes

# Engage Analytics Indicators

This document provides detailed specifications for each indicator in the Engage Analytics system. Each section represents a single indicator with all its metadata, calculation logic, and implementation notes.

---

## Table of Contents

1. [System Use Indicators](#system-use-indicators)
   - [Active Users](#1-active-users)
   - [Percent of Active Users](#2-percent-of-active-users)
   - [Total Users](#3-total-users)
   - [Clients Registered](#4-clients-registered)
   - [Encounters](#5-encounters)
   - [Observations](#6-observations)
2. [Programmatic Indicators](#programmatic-indicators)
   - [Number of Counseling Sessions](#7-number-of-counseling-sessions)
   - [Number of Clients with Depression](#8-number-of-clients-with-depression)
3. [Screening Indicators](#screening-indicators)
   - [Outcome of Baseline mwTool](#9-outcome-of-baseline-mwtool)
   - [Clients Referred for Higher Level of Care](#10-clients-referred-for-higher-level-of-care)
   - [ENGAGE EBT Offered](#11-engage-ebt-offered)
4. [Treatment Offered Indicators](#treatment-offered-indicators)
   - [mwTool Outcome and Intervention Offered](#12-mwtool-outcome-and-intervention-offered)
   - [Refusal Rate](#13-refusal-rate)
5. [Treatment Initiated Indicators](#treatment-initiated-indicators)
   - [mwTool Outcome and Intervention Initiated](#14-mwtool-outcome-and-intervention-initiated)
   - [Percent Access to Care (Reach)](#15-percent-access-to-care-reach)
   - [Comorbidities](#16-comorbidities)
   - [Service Delivery Modality](#17-service-delivery-modality)
   - [Assessment Outcome - Severe Risk](#18-assessment-outcome---severe-risk)
   - [Assessment Outcome - Moderate-High Risk](#19-assessment-outcome---moderate-high-risk)
   - [Assessment Outcome - Low Risk](#20-assessment-outcome---low-risk)
   - [First Session Assessment Scores](#21-first-session-assessment-scores)
6. [Retention Indicators](#retention-indicators)
   - [Subsequent Session Assessment Scores](#22-subsequent-session-assessment-scores)
   - [Sessions by Intervention Type / IPC Completion](#23-sessions-by-intervention-type--ipc-completion)
   - [Percent of Total Sessions by Intervention Type](#24-percent-of-total-sessions-by-intervention-type)
7. [Follow-up Indicators](#follow-up-indicators)
   - [Percent Follow-up](#25-percent-follow-up)
8. [Adoption Indicators](#adoption-indicators)
   - [CwWs Using Technology](#26-cwws-using-technology)
9. [Fidelity Indicators](#fidelity-indicators)
   - [Platform Metadata Outliers](#27-platform-metadata-outliers)
   - [Outliers and Client Symptom Outcomes](#28-outliers-and-client-symptom-outcomes)
10. [Sustainability Indicators](#sustainability-indicators)
    - [Staff Using Technology Across Time](#29-staff-using-technology-across-time)
11. [Outcome Indicators](#outcome-indicators)
    - [MH Symptoms Improvement](#30-mh-symptoms-improvement)
    - [Timeliness](#31-timeliness)
    - [Adverse Events](#32-adverse-events)
12. [Staff Indicators](#staff-indicators)
    - [Staff Sociodemographic Information](#33-staff-sociodemographic-information)
13. [TBD Indicators](#tbd-indicators)
    - [mwTool Post Treatment Follow-ups](#34-mwtool-post-treatment-follow-ups)

---

# System Use Indicators

## 1. Active Users

| Field | Value |
|-------|-------|
| **Domain** | System Use |
| **Module** | All |
| **Indicator** | Active users |
| **Data Type** | Count |
| **Frequency** | Monthly |

### Description
Health workers who submitted at least one report in the period.

### FHIR Implementation Notes
Encounter represents an activity of the Practitioner (user). Check `Encounter.period` for the date and time. Check which Encounter belongs to which Practitioner in `Encounter.participant` with `Encounter.participant.type` equals to `ADM` code.

### Calculation
| Component | Definition |
|-----------|------------|
| **Numerator** | # of active users |
| **Denominator** | N/A |
| **Formula** | Count the number of users that have submitted at least 1 record during the reporting period. This can also be set to a certain threshold (e.g., 30 visits or some predefined target). |

### Disaggregation
Not specified

### Visualization
Bar chart or line chart time series

### Implementation Status
- [ ] SQL model created
- [ ] Unit tests written
- [ ] Validated against sample data

---

## 2. Percent of Active Users

| Field | Value |
|-------|-------|
| **Domain** | System Use |
| **Module** | All |
| **Indicator** | Percent of active users |
| **Data Type** | Percent |
| **Frequency** | Monthly |

### Description
Percentage of users who used the system in the past month.

### FHIR Implementation Notes
Encounter represents an activity of the Practitioner (user). Check `Encounter.period` for the date and time. Check which Encounter belongs to which Practitioner in `Encounter.participant` with `Encounter.participant.type` equals to `ADM` code.

### Calculation
| Component | Definition |
|-----------|------------|
| **Numerator** | # active users |
| **Denominator** | # users |
| **Formula** | # active users / # total users |

### Disaggregation
Not specified

### Visualization
Not specified

### Implementation Status
- [ ] SQL model created
- [ ] Unit tests written
- [ ] Validated against sample data

---

## 3. Total Users

| Field | Value |
|-------|-------|
| **Domain** | System Use |
| **Module** | All |
| **Indicator** | Total Users |
| **Data Type** | Count |
| **Frequency** | Not specified |

### Description
Total number of users registered in the system.

### FHIR Implementation Notes
Practitioner resource.

### Calculation
| Component | Definition |
|-----------|------------|
| **Numerator** | # total users |
| **Denominator** | N/A |
| **Formula** | # registered users in the system |

### Disaggregation
Not specified

### Visualization
Not specified

### Open Questions
- [ ] To clarify what is the rule for a user to no longer be part of the denominator past a specific date

### Implementation Status
- [ ] SQL model created
- [ ] Unit tests written
- [ ] Validated against sample data

---

## 4. Clients Registered

| Field | Value |
|-------|-------|
| **Domain** | System Use |
| **Module** | All |
| **Indicator** | Clients registered |
| **Data Type** | Count |
| **Frequency** | Monthly |

### Description
Number of Clients registered.

### FHIR Implementation Notes
Client (Patient) resource.

### Calculation
| Component | Definition |
|-----------|------------|
| **Numerator** | # Clients registered in the period |
| **Denominator** | N/A |
| **Formula** | # Clients registered in the period |

### Disaggregation
- By user
- By clinic
- Location (e.g., outside of clinic or in clinic)

### Visualization
Not specified

### Implementation Status
- [ ] SQL model created
- [ ] Unit tests written
- [ ] Validated against sample data

---

## 5. Encounters

| Field | Value |
|-------|-------|
| **Domain** | System Use |
| **Module** | All |
| **Indicator** | Encounters |
| **Data Type** | Count |
| **Frequency** | Monthly |

### Description
Number of Client encounters.

### FHIR Implementation Notes
Encounter tied to how many times the client has been encountered by the Practitioner. Check which client related to this encounter in `Encounter.subject`. Total number of client encounters during the previous month.

### Calculation
| Component | Definition |
|-----------|------------|
| **Numerator** | # of client encounters |
| **Denominator** | N/A |
| **Formula** | # of client encounters |

### Disaggregation
- By user
- By clinic
- By encounter type

### Visualization
Not specified

### Implementation Status
- [ ] SQL model created
- [ ] Unit tests written
- [ ] Validated against sample data

---

## 6. Observations

| Field | Value |
|-------|-------|
| **Domain** | System Use |
| **Module** | All |
| **Indicator** | Observations |
| **Data Type** | Count |
| **Frequency** | Monthly |

### Description
This relates more to specific things we may want to be recording (e.g., people with depression).

### FHIR Implementation Notes
Observation tied to the specific things that are recorded. Check `Observation.code` to see what the specific thing is.

### Calculation
| Component | Definition |
|-----------|------------|
| **Numerator** | # of observations |
| **Denominator** | N/A |
| **Formula** | Not specified |

### Disaggregation
- By user
- By clinic
- By encounter type

### Visualization
Not specified

### Implementation Status
- [ ] SQL model created
- [ ] Unit tests written
- [ ] Validated against sample data

---

# Programmatic Indicators

## 7. Number of Counseling Sessions

| Field | Value |
|-------|-------|
| **Domain** | Programmatic |
| **Module** | All |
| **Indicator** | Number of counseling sessions |
| **Data Type** | Count |
| **Frequency** | Not specified |

### Description
Number of counseling sessions.

### FHIR Implementation Notes
Encounter tied to each session/form submission. Check which client related to this encounter in `Encounter.subject`.

### Calculation
| Component | Definition |
|-----------|------------|
| **Numerator** | # of sessions / modules |
| **Denominator** | N/A |
| **Formula** | Not specified |

### Disaggregation
- By user
- By clinic
- By client
- By module type

### Visualization
Not specified

### Open Questions
- [ ] This has to be intervention specific because IPC is 4 sessions and the others are 1 session

### Implementation Status
- [ ] SQL model created
- [ ] Unit tests written
- [ ] Validated against sample data

---

## 8. Number of Clients with Depression

| Field | Value |
|-------|-------|
| **Domain** | Programmatic |
| **Module** | IPC1 |
| **Indicator** | Number of Clients with depression |
| **Data Type** | Count |
| **Frequency** | Monthly |

### Description
Number of Clients reporting depression.

### FHIR Implementation Notes
Observation tied to client depression. Check `Observation.code` to see the depression indicator.

### Calculation
| Component | Definition |
|-----------|------------|
| **Numerator** | # Clients reporting depression |
| **Denominator** | N/A |
| **Formula** | Not specified |

### Disaggregation
- By user
- By clinic
- By location

### Visualization
Not specified

### Open Questions
- [ ] Need to clarify when a user starts and ends to have depression

### Implementation Status
- [ ] SQL model created
- [ ] Unit tests written
- [ ] Validated against sample data

---

# Screening Indicators

## 9. Outcome of Baseline mwTool

| Field | Value |
|-------|-------|
| **Domain** | Screening |
| **Implementation Outcome** | Screening |
| **Module** | mwTool |
| **Indicator** | Outcome of baseline mwTool (4 possible outcomes) |
| **Data Type** | Count |
| **Frequency** | Monthly |

### Description
Outcome for clients who have completed baseline mwTool.

### FHIR Implementation Notes
TBD

### Calculation
| Component | Definition |
|-----------|------------|
| **Numerator** | # of clients with each of the 4 possible mwTool outcomes (severe mental disorders; suicidality; substance use; and common mental disorders) |
| **Denominator** | N/A |
| **Formula** | Not specified |

### Disaggregation
By user

### Visualization
Not specified

### Implementation Status
- [ ] SQL model created
- [ ] Unit tests written
- [ ] Validated against sample data

---

## 10. Clients Referred for Higher Level of Care

| Field | Value |
|-------|-------|
| **Domain** | Screening |
| **Implementation Outcome** | Screening |
| **Module** | mwTool & SPI |
| **Indicator** | Clients referred for a higher level of care post-mwTool administration |
| **Data Type** | Count |
| **Frequency** | Monthly |

### Description
Number of clients referred to a higher level of care post mwTool administration. The number of clients who are referred to a higher level of care based on their scores from the mwTool and the C-SSRS in the SPI module.

### FHIR Implementation Notes
TBD

### Calculation
| Component | Definition |
|-----------|------------|
| **Numerator** | - # of clients screened for severe mental disorders on the mwTool<br>- # of clients with severe risk result on the C-SSRS in SPI module<br>- Number of clients referred to higher-level care |
| **Denominator** | N/A - Total number of clients screened |
| **Formula** | Number of clients referred / Total number of clients screened |

### Disaggregation
By user

### Visualization
Not specified

### Open Questions
- [ ] There's a gap between the indicator name and the definitions. Is this a proxy indicator?

### Implementation Status
- [ ] SQL model created
- [ ] Unit tests written
- [ ] Validated against sample data

---

## 11. ENGAGE EBT Offered

| Field | Value |
|-------|-------|
| **Domain** | Screening |
| **Implementation Outcome** | Screening |
| **Module** | mwTool |
| **Indicator** | ENGAGE EBT offered/positive outcome on mwTool |
| **Data Type** | Percent |
| **Frequency** | Monthly |

### Description
The number of clients who were offered evidence-based treatments (EBTs) such as IPC, SPI, or SBIRT after screening positive for common mental disorders, suicidality, or substance use on the mwTool.

### FHIR Implementation Notes
TBD

### Calculation
| Component | Definition |
|-----------|------------|
| **Numerator** | # of clients offered IPC, SPI, or SBIRT |
| **Denominator** | # of clients screened positive for common mental disorders, suicidality, or substance use on the mwTool |
| **Formula** | (# of clients offered IPC, SPI, or SBIRT / # of clients screened positive) * 100 |

### Disaggregation
Not specified

### Visualization
Not specified

### Open Questions
- [ ] What does "# of clients offered" mean? May be answered by new flows in the app

### Implementation Status
- [ ] SQL model created
- [ ] Unit tests written
- [ ] Validated against sample data

---

# Treatment Offered Indicators

## 12. mwTool Outcome and Intervention Offered

| Field | Value |
|-------|-------|
| **Domain** | Treatment Offered |
| **Implementation Outcome** | Treatment offered |
| **Module** | mwTool related workflow |
| **Indicator** | Outcome of baseline mwTool (4 possible outcomes) and the intervention (IPC, SPI, SBIRT) offered |
| **Data Type** | Not specified |
| **Frequency** | Not specified |

### Description
Looking at whether the intervention matches the results of the mwTool.

### FHIR Implementation Notes
Not specified

### Calculation
| Component | Definition |
|-----------|------------|
| **Numerator** | Not specified |
| **Denominator** | Not specified |
| **Formula** | Not specified |

### Disaggregation
Not specified

### Visualization
Not specified

### Implementation Status
- [ ] SQL model created
- [ ] Unit tests written
- [ ] Validated against sample data

---

## 13. Refusal Rate

| Field | Value |
|-------|-------|
| **Domain** | Treatment Offered |
| **Implementation Outcome** | Treatment offered |
| **Module** | mwTool |
| **Indicator** | Refusal rate |
| **Data Type** | Percent |
| **Frequency** | Not specified |

### Description
Declined/offered services.

### FHIR Implementation Notes
TBD

### Calculation
| Component | Definition |
|-----------|------------|
| **Numerator** | # of clients that declined |
| **Denominator** | Services offered (SPI, IPC, SBIRT) |
| **Formula** | Declined / services offered |

### Disaggregation
Not specified

### Visualization
Not specified

### Implementation Status
- [ ] SQL model created
- [ ] Unit tests written
- [ ] Validated against sample data

---

# Treatment Initiated Indicators

## 14. mwTool Outcome and Intervention Initiated

| Field | Value |
|-------|-------|
| **Domain** | Treatment Initiated |
| **Implementation Outcome** | Treatment initiated |
| **Module** | Not specified |
| **Indicator** | Outcome of baseline mwTool (4 possible outcomes) and the intervention (IPC, SPI, SBIRT) initiated |
| **Data Type** | Not specified |
| **Frequency** | Not specified |

### Description
Have at least one ENGAGE session done.

### FHIR Implementation Notes
Not specified

### Calculation
| Component | Definition |
|-----------|------------|
| **Numerator** | Not specified |
| **Denominator** | Not specified |
| **Formula** | Not specified |

### Disaggregation
Not specified

### Visualization
Not specified

### Implementation Status
- [ ] SQL model created
- [ ] Unit tests written
- [ ] Validated against sample data

---

## 15. Percent Access to Care (Reach)

| Field | Value |
|-------|-------|
| **Domain** | Treatment Initiated |
| **Implementation Outcome** | Treatment initiated |
| **Module** | mwTool |
| **Indicator** | % access to care (Reach) |
| **Data Type** | Percent |
| **Frequency** | Monthly |

### Description
Started treatment (Y/N)/screened positive for baseline mwTool. The percentage of clients who have started treatment following a positive screening result on the baseline mwTool.

### FHIR Implementation Notes
TBD

### Calculation
| Component | Definition |
|-----------|------------|
| **Numerator** | Clients who started treatment (yes or no) - Number of clients who attended their first formal treatment session |
| **Denominator** | Clients screened positive for baseline mwTool - Number of clients who screened positive on the baseline mwTool |
| **Formula** | (Number of clients who started treatment / Number of clients screened positive) * 100 |

### Disaggregation
Not specified

### Visualization
Not specified

### Open Questions
- [ ] What is the definition of starting treatment? When is a user counted as having 'started treatment'? There is a lag between screening and treatment start, so the timing of the metric is relevant

### Implementation Status
- [ ] SQL model created
- [ ] Unit tests written
- [ ] Validated against sample data

---

## 16. Comorbidities

| Field | Value |
|-------|-------|
| **Domain** | Treatment Initiated |
| **Implementation Outcome** | Treatment initiated |
| **Module** | mwTool |
| **Indicator** | Comorbidities |
| **Data Type** | Count & Percent |
| **Frequency** | Not specified |

### Description
Number of clients that scored positive on the mwTool in multiple categories.

### FHIR Implementation Notes
TBD

### Calculation
| Component | Definition |
|-----------|------------|
| **Numerator** | Number of clients that scored positive on the mwTool in multiple categories |
| **Denominator** | Client that scored positive on the mwTool |
| **Formula** | Not specified |

### Disaggregation
Not specified

### Visualization
Not specified

### Implementation Status
- [ ] SQL model created
- [ ] Unit tests written
- [ ] Validated against sample data

---

## 17. Service Delivery Modality

| Field | Value |
|-------|-------|
| **Domain** | Treatment Initiated |
| **Implementation Outcome** | Treatment initiated |
| **Module** | Each module's session |
| **Indicator** | Service delivery modality |
| **Data Type** | Count & Percent |
| **Frequency** | Not specified |

### Description
Service delivery modality: phone telehealth, video telehealth, in-person, or hybrid.

### FHIR Implementation Notes
Encounter tied to the Service delivery modality (phone telehealth, video telehealth, in-person). Check the modality in `Encounter.type`. Check which client related to this encounter in `Encounter.subject`. Check which form that results to this encounter in `Encounter.reasonCode`.

### Calculation
| Component | Definition |
|-----------|------------|
| **Numerator** | Number of sessions of each modality in a month |
| **Denominator** | Number of total sessions in a month |
| **Formula** | Not specified |

### Disaggregation
Not specified

### Visualization
Not specified

### Implementation Status
- [ ] SQL model created
- [ ] Unit tests written
- [ ] Validated against sample data

---

## 18. Assessment Outcome - Severe Risk

| Field | Value |
|-------|-------|
| **Domain** | Treatment Initiated |
| **Implementation Outcome** | Treatment initiated |
| **Module** | All modules |
| **Indicator** | Assessment outcome of severe risk |
| **Data Type** | Percent |
| **Frequency** | Not specified |

### Description
Assessment outcome from all questionnaires => severe risk.

### FHIR Implementation Notes
Observation tied to the risk level (severe, high, moderate, low). Check the risk level meaning in `Observation.interpretation`. Check the risk level numeric in `Observation.valueQuantity`. Check which form that results to the risk level in `Observation.code`. Check which client that has this risk level in `Observation.subject`.

### Calculation
| Component | Definition |
|-----------|------------|
| **Numerator** | # of clients scoring severe in any one of the following assessments: mwTool, PHQ-9, GAD-7, DAST, C-SSRS, and AUDIT |
| **Denominator** | # of clients |
| **Formula** | Not specified |

### Disaggregation
Not specified

### Visualization
Not specified

### Implementation Status
- [ ] SQL model created
- [ ] Unit tests written
- [ ] Validated against sample data

---

## 19. Assessment Outcome - Moderate-High Risk

| Field | Value |
|-------|-------|
| **Domain** | Treatment Initiated |
| **Implementation Outcome** | Treatment initiated |
| **Module** | All modules |
| **Indicator** | Assessment outcome of moderate-high risk |
| **Data Type** | Percent |
| **Frequency** | Not specified |

### Description
Assessment outcome from all questionnaires => moderate-high risk.

### FHIR Implementation Notes
Same as Assessment Outcome - Severe Risk.

### Calculation
| Component | Definition |
|-----------|------------|
| **Numerator** | # of clients scoring moderate (but not severe) in any one of the following assessments: mwTool, PHQ-9, GAD-7, DAST, C-SSRS, and AUDIT |
| **Denominator** | # of clients |
| **Formula** | Not specified |

### Disaggregation
Not specified

### Visualization
Not specified

### Implementation Status
- [ ] SQL model created
- [ ] Unit tests written
- [ ] Validated against sample data

---

## 20. Assessment Outcome - Low Risk

| Field | Value |
|-------|-------|
| **Domain** | Treatment Initiated |
| **Implementation Outcome** | Treatment initiated |
| **Module** | All modules |
| **Indicator** | Assessment outcome of low risk |
| **Data Type** | Percent |
| **Frequency** | Not specified |

### Description
Assessment outcome from all questionnaires => low risk.

### FHIR Implementation Notes
Same as Assessment Outcome - Severe Risk.

### Calculation
| Component | Definition |
|-----------|------------|
| **Numerator** | # of clients scoring low (but not moderate or severe) in any one of the following assessments: mwTool, PHQ-9, GAD-7, DAST, C-SSRS, and AUDIT |
| **Denominator** | # of clients |
| **Formula** | Not specified |

### Disaggregation
Not specified

### Visualization
Not specified

### Implementation Status
- [ ] SQL model created
- [ ] Unit tests written
- [ ] Validated against sample data

---

## 21. First Session Assessment Scores

| Field | Value |
|-------|-------|
| **Domain** | Treatment Initiated |
| **Implementation Outcome** | Treatment initiated |
| **Module** | All modules |
| **Indicator** | PHQ-9, GAD-7, PC-PTSD, Mood Rating Scale, DAST-10, AUDIT, and C-SSRS scores (first session) among those who initiate treatment |
| **Data Type** | Percent |
| **Frequency** | Monthly |

### Description
The assessment scores from PHQ-9, GAD-7, PC-PTSD, Mood Rating Scale, DAST-10, AUDIT, and C-SSRS during the first session among clients who have initiated treatment or accepted treatment.

### FHIR Implementation Notes
Not specified

### Calculation
| Component | Definition |
|-----------|------------|
| **Numerator** | Assessment scores in the first session - Number of clients with recorded assessment scores in their first treatment session |
| **Denominator** | Clients who started treatment (yes or no) - Number of clients who have accepted treatment and attended their first session |
| **Formula** | (Number of clients with recorded assessment scores in their first session / Number of clients who accepted treatment and attended their first session) * 100 |

### Disaggregation
Not specified

### Visualization
Not specified

### Open Questions
- [ ] Confirm the denominator: Who are all the ones that begin treatment?

### Implementation Status
- [ ] SQL model created
- [ ] Unit tests written
- [ ] Validated against sample data

---

# Retention Indicators

## 22. Subsequent Session Assessment Scores

| Field | Value |
|-------|-------|
| **Domain** | Retention |
| **Implementation Outcome** | Retention |
| **Module** | All modules |
| **Indicator** | PHQ-9, GAD-7, PC-PTSD, Mood Rating Scale, DAST-10, AUDIT, and C-SSRS scores (for all subsequent sessions after the first session) |
| **Data Type** | Percent |
| **Frequency** | Not specified |

### Description
Assessment scores for subsequent sessions among those who initiate treatment/individuals who accept treatment.

### FHIR Implementation Notes
Observation tied to the total score of each form. Check the total score in `Observation.value`. Check which form that the total score is related to from `Observation.derivedFrom`. Check which client that this total score is related to in `Observation.subject`.

### Calculation
| Component | Definition |
|-----------|------------|
| **Numerator** | Assessment scores for each session |
| **Denominator** | Clients who started treatment (yes or no) |
| **Formula** | Not specified |

### Disaggregation
Not specified

### Visualization
Not specified

### Implementation Status
- [ ] SQL model created
- [ ] Unit tests written
- [ ] Validated against sample data

---

## 23. Sessions by Intervention Type / IPC Completion

| Field | Value |
|-------|-------|
| **Domain** | Retention |
| **Implementation Outcome** | Retention |
| **Module** | All modules |
| **Indicator** | Number of sessions by intervention type (IPC, SPI, SBIRT) / % of People Who Finish All Sessions for IPC |
| **Data Type** | Count |
| **Frequency** | Monthly |

### Description
The percentage of clients who complete all required sessions of IPC.

### FHIR Implementation Notes
Encounter tied to each session. Check which client related to this encounter in `Encounter.subject`. Check which session/form that results to this encounter in `Encounter.reasonCode`.

### Calculation
| Component | Definition |
|-----------|------------|
| **Numerator** | Number of sessions for each intervention / Number of clients who completed all sessions for the intervention |
| **Denominator** | Number of clients who start the intervention |
| **Formula** | (Number of clients who complete all sessions / Number of clients who start the intervention) * 100 |

### Disaggregation
Not specified

### Visualization
Not specified

### Open Questions
- [ ] What is the correct definition? Inconsistency between indicator name and comment

### Implementation Status
- [ ] SQL model created
- [ ] Unit tests written
- [ ] Validated against sample data

---

## 24. Percent of Total Sessions by Intervention Type

| Field | Value |
|-------|-------|
| **Domain** | Retention |
| **Implementation Outcome** | Retention |
| **Module** | All modules |
| **Indicator** | % of total sessions by intervention type |
| **Data Type** | Percent |
| **Frequency** | Not specified |

### Description
IPC = 4 sessions & SBIRT = 4 sessions.

### FHIR Implementation Notes
Not specified

### Calculation
| Component | Definition |
|-----------|------------|
| **Numerator** | How many sessions done for IPC and SBIRT |
| **Denominator** | 4 |
| **Formula** | Not specified |

### Disaggregation
Not specified

### Visualization
Not specified

### Implementation Status
- [ ] SQL model created
- [ ] Unit tests written
- [ ] Validated against sample data

---

# Follow-up Indicators

## 25. Percent Follow-up

| Field | Value |
|-------|-------|
| **Domain** | Follow-up |
| **Implementation Outcome** | Follow-up |
| **Module** | All modules |
| **Indicator** | % follow-up |
| **Data Type** | Percent |
| **Frequency** | Not specified |

### Description
The percentage of clients who received the one month follow-up visit after completing an SPI, SBIRT or IPC.

### FHIR Implementation Notes
TODO

### Calculation
| Component | Definition |
|-----------|------------|
| **Numerator** | Number of clients who received the one month follow-up session |
| **Denominator** | Number of clients who attended at least one intervention sessions |
| **Formula** | (Number of clients with follow-up sessions / Number of clients with initial sessions) * 100 |

### Disaggregation
Not specified

### Visualization
Not specified

### Open Questions
- [ ] Need to confirm what is the definition of 'follow up'. What is the time frame?

### Implementation Status
- [ ] SQL model created
- [ ] Unit tests written
- [ ] Validated against sample data

---

# Adoption Indicators

## 26. CwWs Using Technology

| Field | Value |
|-------|-------|
| **Domain** | Adoption |
| **Implementation Outcome** | Adoption |
| **Module** | All modules |
| **Indicator** | CwWs using the technology |
| **Data Type** | Count & Percent |
| **Frequency** | Not specified |

### Description
The number of Community Wellness Workers (CwWs) actively using the technology platform to manage and deliver interventions.

### FHIR Implementation Notes
Encounter represents an activity of the Practitioner (user). Check `Encounter.period` for the date and time. Check which Encounter belongs to which Practitioner in `Encounter.participant` with `Encounter.participant.type` equals to `ADM` code.

### Calculation
| Component | Definition |
|-----------|------------|
| **Numerator** | Number of CwWs registered and have at least one client / Number of CwWs who were active in the last month's use of the platform |
| **Denominator** | Total number of registered CwWs |
| **Formula** | (Number of active CwWs / Total number of registered CwWs) * 100 |

### Disaggregation
Not specified

### Visualization
Not specified

### Implementation Status
- [ ] SQL model created
- [ ] Unit tests written
- [ ] Validated against sample data

---

# Fidelity Indicators

## 27. Platform Metadata Outliers

| Field | Value |
|-------|-------|
| **Domain** | Fidelity |
| **Implementation Outcome** | Fidelity |
| **Module** | All modules |
| **Indicator** | Meta data from the platform to search for outliers |
| **Data Type** | Time duration or Count |
| **Frequency** | Not specified |

### Description
The process of analyzing metadata from the platform to identify outliers, such as unusual time ranges per session, time spent per screen, frequency of questions skipped, frequency of info icon box clicks, and frequency of screen skipping by CwWs.

### FHIR Implementation Notes
**% of questions skipped out of total questions; which questions are skipped:**
QuestionnaireResponse tied to a form that was filled and submitted by the Practitioner. Check all items that are non `display` and `group` type, if such item has no answer, that item is skipped.

**% gave formulation in session 1 vs. session 2 (IPC):**
QuestionnaireResponse tied to a form that was filled and submitted by the Practitioner. Check specific items [TODO]

**% of problem areas (IPC); types of substance (SBIRT):**
TBD - Ideally with charts

### Calculation
| Component | Definition |
|-----------|------------|
| **Numerator** | Not specified |
| **Denominator** | Not specified |
| **Formula** | Not specified |

### Disaggregation
Not specified

### Visualization
Not specified

### Implementation Status
- [ ] SQL model created
- [ ] Unit tests written
- [ ] Validated against sample data

---

## 28. Outliers and Client Symptom Outcomes

| Field | Value |
|-------|-------|
| **Domain** | Fidelity |
| **Implementation Outcome** | Fidelity |
| **Module** | All modules |
| **Indicator** | Meta data from the platform; outliers and how they correlate with client symptom outcome |
| **Data Type** | Not specified |
| **Frequency** | Not specified |

### Description
Not specified

### FHIR Implementation Notes
TBD

### Calculation
| Component | Definition |
|-----------|------------|
| **Numerator** | Not specified |
| **Denominator** | Not specified |
| **Formula** | Not specified |

### Disaggregation
Not specified

### Visualization
Not specified

### Implementation Status
- [ ] SQL model created
- [ ] Unit tests written
- [ ] Validated against sample data

---

# Sustainability Indicators

## 29. Staff Using Technology Across Time

| Field | Value |
|-------|-------|
| **Domain** | Sustainability |
| **Implementation Outcome** | Sustainability |
| **Module** | All modules |
| **Indicator** | Staff using technology across time |
| **Data Type** | Percent |
| **Frequency** | Not specified |

### Description
% of clinic staff using the technology across time (or 6 months).

### FHIR Implementation Notes
Encounter represents an activity of the Practitioner (user). Check `Encounter.period` for the date and time. Check which Encounter belongs to which Practitioner in `Encounter.participant` with `Encounter.participant.type` equals to `ADM` code.

### Calculation
| Component | Definition |
|-----------|------------|
| **Numerator** | Staff registered and have at least one client |
| **Denominator** | Staff registered |
| **Formula** | Not specified |

### Disaggregation
Not specified

### Visualization
Not specified

### Implementation Status
- [ ] SQL model created
- [ ] Unit tests written
- [ ] Validated against sample data

---

# Outcome Indicators

## 30. MH Symptoms Improvement

| Field | Value |
|-------|-------|
| **Domain** | Outcome |
| **Implementation Outcome** | MH symptoms improvement (by 50%) |
| **Module** | All modules |
| **Indicator** | Outcomes by disorder type: PHQ-9, GAD-7, PC-PTSD, Mood Rating Scale, DAST-10, AUDIT, and C-SSRS |
| **Data Type** | Not specified |
| **Frequency** | Not specified |

### Description
Not specified

### FHIR Implementation Notes
TODO

### Calculation
| Component | Definition |
|-----------|------------|
| **Numerator** | Not specified |
| **Denominator** | Not specified |
| **Formula** | Not specified |

### Disaggregation
Not specified

### Visualization
Not specified

### Implementation Status
- [ ] SQL model created
- [ ] Unit tests written
- [ ] Validated against sample data

---

## 31. Timeliness

| Field | Value |
|-------|-------|
| **Domain** | Outcome |
| **Implementation Outcome** | Timeliness |
| **Module** | All modules |
| **Indicator** | Timeliness of the triage and short term treatment sessions (IPC and MI: 4-6 weeks window) |
| **Data Type** | Not specified |
| **Frequency** | Not specified |

### Description
Not specified

### FHIR Implementation Notes
TODO

### Calculation
| Component | Definition |
|-----------|------------|
| **Numerator** | Not specified |
| **Denominator** | Not specified |
| **Formula** | Not specified |

### Disaggregation
Not specified

### Visualization
Not specified

### Implementation Status
- [ ] SQL model created
- [ ] Unit tests written
- [ ] Validated against sample data

---

## 32. Adverse Events

| Field | Value |
|-------|-------|
| **Domain** | Outcome |
| **Implementation Outcome** | Service-level outcomes |
| **Module** | All modules |
| **Indicator** | Adverse events |
| **Data Type** | Count |
| **Frequency** | Not specified |

### Description
Adverse events among clients and staff.

### FHIR Implementation Notes
TBD

### Calculation
| Component | Definition |
|-----------|------------|
| **Numerator** | Not specified |
| **Denominator** | Not specified |
| **Formula** | Not specified |

### Disaggregation
Not specified

### Visualization
Not specified

### Implementation Status
- [ ] SQL model created
- [ ] Unit tests written
- [ ] Validated against sample data

---

# Staff Indicators

## 33. Staff Sociodemographic Information

| Field | Value |
|-------|-------|
| **Domain** | Staff Knowledge, Attitudes, and Beliefs (Inner Context) |
| **Implementation Outcome** | Staff Knowledge, Attitudes, and Beliefs (Inner Context) |
| **Module** | Account registration |
| **Indicator** | Staff basic/sociodemographic information |
| **Data Type** | Count |
| **Frequency** | Not specified |

### Description
Staff gender, age, race/ethnicity, position, job tenure, education.

### FHIR Implementation Notes
TBD

### Calculation
| Component | Definition |
|-----------|------------|
| **Numerator** | Staff gender, age, race/ethnicity, position, job tenure, education |
| **Denominator** | Not specified |
| **Formula** | Not specified |

### Disaggregation
Not specified

### Visualization
Not specified

### Implementation Status
- [ ] SQL model created
- [ ] Unit tests written
- [ ] Validated against sample data

---

# TBD Indicators

## 34. mwTool Post Treatment Follow-ups

| Field | Value |
|-------|-------|
| **Domain** | TBD |
| **Module** | Not specified |
| **Indicator** | mwTool post treatment and at 3- and 6-month follow-ups |
| **Data Type** | Not specified |
| **Frequency** | Not specified |

### Description
Not specified

### FHIR Implementation Notes
TBD

### Calculation
| Component | Definition |
|-----------|------------|
| **Numerator** | Not specified |
| **Denominator** | Not specified |
| **Formula** | Not specified |

### Disaggregation
Not specified

### Visualization
Not specified

### Implementation Status
- [ ] SQL model created
- [ ] Unit tests written
- [ ] Validated against sample data

---

# Summary: Currently Implemented vs. Specified

## Implemented in `dbt/macros/metrics.sql`

| metric_id | Maps to Indicator |
|-----------|-------------------|
| `provider_count` | Total Users (#3) |
| `patient_count` | Clients Registered (#4) |
| `client_encounters` | Encounters (#5) |
| `active_providers` | Active Users (#1) |
| `percent_active_providers` | Percent of Active Users (#2) |
| `clients_eligible_for_ipc` | Related to ENGAGE EBT Offered (#11) |
| `clients_accepted_ipc` | Related to Treatment Initiated |
| `clients_sbirt_mi_eligible` | Related to mwTool outcomes (#9) |
| `clients_eligible_spi` | Related to mwTool outcomes (#9) |
| `clients_eligible_referral` | Clients Referred (#10) |
| `clients_eligible_fws` | Related to Financial Wellness |
| `encounters_in_person` | Service Delivery Modality (#17) |
| `encounters_video_telehealth` | Service Delivery Modality (#17) |
| `encounters_phone_telehealth` | Service Delivery Modality (#17) |

## Not Yet Implemented

- Number of Counseling Sessions (#7)
- Number of Clients with Depression (#8)
- Refusal Rate (#13)
- Percent Access to Care (#15)
- Comorbidities (#16)
- Assessment Outcome indicators (#18, #19, #20)
- First Session Assessment Scores (#21)
- Retention indicators (#22, #23, #24)
- Follow-up indicator (#25)
- CwWs Using Technology (#26)
- Fidelity indicators (#27, #28)
- Sustainability indicator (#29)
- Outcome indicators (#30, #31, #32)
- Staff indicators (#33)
- TBD indicators (#34)
