-- ABOUTME: Calculates PHQ-9 and GAD-7 total scores with severity classification
-- ABOUTME: Enables risk level metrics (indicators #18-20) from assessment questionnaires

{{ config(materialized='view') }}

-- PHQ-9 scores from session 1
-- Queries int_qr_answers_long directly by linkId to avoid dependency on
-- dynamically generated column names in wide views (questionnaire ID: Questionnaire/54)
with phq9_s1_raw as (
    select
        a.qr_id,
        a.subject_patient_id,
        t.practitioner_organization_id as organization_id,
        max(case when a.linkid = 'f1.3.1'  then a.answer_value_text end) as q1_raw,
        max(case when a.linkid = 'f1.4.1'  then a.answer_value_text end) as q2_raw,
        max(case when a.linkid = 'f1.5.1'  then a.answer_value_text end) as q3_raw,
        max(case when a.linkid = 'f1.6.1'  then a.answer_value_text end) as q4_raw,
        max(case when a.linkid = 'f1.7.1'  then a.answer_value_text end) as q5_raw,
        max(case when a.linkid = 'f1.8.1'  then a.answer_value_text end) as q6_raw,
        max(case when a.linkid = 'f1.9.1'  then a.answer_value_text end) as q7_raw,
        max(case when a.linkid = 'f1.10.1' then a.answer_value_text end) as q8_raw,
        max(case when a.linkid = 'f1.11.1' then a.answer_value_text end) as q9_raw
    from {{ ref('int_qr_answers_long') }} a
    left join {{ ref('int_qr_tags') }} t on t.resource_id = a.qr_id
    where a.questionnaire_id = 'Questionnaire/54'
        and a.subject_patient_id is not null
    group by a.qr_id, a.subject_patient_id, t.practitioner_organization_id
),

phq9_s1 as (
    select
        subject_patient_id,
        organization_id,
        qr_id,
        'session_1' as session,
        -- Extract numeric scores from text answers (handles "0 - Not at all" and "Not at all" formats)
        case when q1_raw like '0 -%' or q1_raw = 'Not at all' then 0
             when q1_raw like '1 -%' or q1_raw = 'Several days' then 1
             when q1_raw like '2 -%' or q1_raw = 'More than half the days' then 2
             when q1_raw like '3 -%' or q1_raw = 'Nearly every day' then 3
             else null end as q1_interest,
        case when q2_raw like '0 -%' or q2_raw = 'Not at all' then 0
             when q2_raw like '1 -%' or q2_raw = 'Several days' then 1
             when q2_raw like '2 -%' or q2_raw = 'More than half the days' then 2
             when q2_raw like '3 -%' or q2_raw = 'Nearly every day' then 3
             else null end as q2_depression,
        case when q3_raw like '0 -%' or q3_raw = 'Not at all' then 0
             when q3_raw like '1 -%' or q3_raw = 'Several days' then 1
             when q3_raw like '2 -%' or q3_raw = 'More than half the days' then 2
             when q3_raw like '3 -%' or q3_raw = 'Nearly every day' then 3
             else null end as q3_sleep,
        case when q4_raw like '0 -%' or q4_raw = 'Not at all' then 0
             when q4_raw like '1 -%' or q4_raw = 'Several days' then 1
             when q4_raw like '2 -%' or q4_raw = 'More than half the days' then 2
             when q4_raw like '3 -%' or q4_raw = 'Nearly every day' then 3
             else null end as q4_fatigue,
        case when q5_raw like '0 -%' or q5_raw = 'Not at all' then 0
             when q5_raw like '1 -%' or q5_raw = 'Several days' then 1
             when q5_raw like '2 -%' or q5_raw = 'More than half the days' then 2
             when q5_raw like '3 -%' or q5_raw = 'Nearly every day' then 3
             else null end as q5_appetite,
        case when q6_raw like '0 -%' or q6_raw = 'Not at all' then 0
             when q6_raw like '1 -%' or q6_raw = 'Several days' then 1
             when q6_raw like '2 -%' or q6_raw = 'More than half the days' then 2
             when q6_raw like '3 -%' or q6_raw = 'Nearly every day' then 3
             else null end as q6_selfworth,
        case when q7_raw like '0 -%' or q7_raw = 'Not at all' then 0
             when q7_raw like '1 -%' or q7_raw = 'Several days' then 1
             when q7_raw like '2 -%' or q7_raw = 'More than half the days' then 2
             when q7_raw like '3 -%' or q7_raw = 'Nearly every day' then 3
             else null end as q7_concentration,
        case when q8_raw like '0 -%' or q8_raw = 'Not at all' then 0
             when q8_raw like '1 -%' or q8_raw = 'Several days' then 1
             when q8_raw like '2 -%' or q8_raw = 'More than half the days' then 2
             when q8_raw like '3 -%' or q8_raw = 'Nearly every day' then 3
             else null end as q8_movement,
        case when q9_raw like '0 -%' or q9_raw = 'Not at all' then 0
             when q9_raw like '1 -%' or q9_raw = 'Several days' then 1
             when q9_raw like '2 -%' or q9_raw = 'More than half the days' then 2
             when q9_raw like '3 -%' or q9_raw = 'Nearly every day' then 3
             else null end as q9_selfharm
    from phq9_s1_raw
),

phq9_scored as (
    select
        subject_patient_id,
        organization_id,
        qr_id,
        session,
        'PHQ-9' as assessment_type,
        (coalesce(q1_interest, 0) + coalesce(q2_depression, 0) + coalesce(q3_sleep, 0) +
         coalesce(q4_fatigue, 0) + coalesce(q5_appetite, 0) + coalesce(q6_selfworth, 0) +
         coalesce(q7_concentration, 0) + coalesce(q8_movement, 0) + coalesce(q9_selfharm, 0)) as total_score,
        -- PHQ-9 Severity: 0-4 Minimal, 5-9 Mild, 10-14 Moderate, 15-19 Moderately Severe, 20-27 Severe
        case
            when (coalesce(q1_interest, 0) + coalesce(q2_depression, 0) + coalesce(q3_sleep, 0) +
                  coalesce(q4_fatigue, 0) + coalesce(q5_appetite, 0) + coalesce(q6_selfworth, 0) +
                  coalesce(q7_concentration, 0) + coalesce(q8_movement, 0) + coalesce(q9_selfharm, 0)) >= 20 then 'Severe'
            when (coalesce(q1_interest, 0) + coalesce(q2_depression, 0) + coalesce(q3_sleep, 0) +
                  coalesce(q4_fatigue, 0) + coalesce(q5_appetite, 0) + coalesce(q6_selfworth, 0) +
                  coalesce(q7_concentration, 0) + coalesce(q8_movement, 0) + coalesce(q9_selfharm, 0)) >= 15 then 'Moderately Severe'
            when (coalesce(q1_interest, 0) + coalesce(q2_depression, 0) + coalesce(q3_sleep, 0) +
                  coalesce(q4_fatigue, 0) + coalesce(q5_appetite, 0) + coalesce(q6_selfworth, 0) +
                  coalesce(q7_concentration, 0) + coalesce(q8_movement, 0) + coalesce(q9_selfharm, 0)) >= 10 then 'Moderate'
            when (coalesce(q1_interest, 0) + coalesce(q2_depression, 0) + coalesce(q3_sleep, 0) +
                  coalesce(q4_fatigue, 0) + coalesce(q5_appetite, 0) + coalesce(q6_selfworth, 0) +
                  coalesce(q7_concentration, 0) + coalesce(q8_movement, 0) + coalesce(q9_selfharm, 0)) >= 5 then 'Mild'
            else 'Minimal'
        end as severity
    from phq9_s1
),

-- GAD-7 scores from session 1
-- (questionnaire ID: Questionnaire/202)
gad7_s1_raw as (
    select
        a.qr_id,
        a.subject_patient_id,
        t.practitioner_organization_id as organization_id,
        max(case when a.linkid = 'f1.15.1' then a.answer_value_text end) as q1_raw,
        max(case when a.linkid = 'f1.16.1' then a.answer_value_text end) as q2_raw,
        max(case when a.linkid = 'f1.17.1' then a.answer_value_text end) as q3_raw,
        max(case when a.linkid = 'f1.18.1' then a.answer_value_text end) as q4_raw,
        max(case when a.linkid = 'f1.19.1' then a.answer_value_text end) as q5_raw,
        max(case when a.linkid = 'f1.20.1' then a.answer_value_text end) as q6_raw,
        max(case when a.linkid = 'f1.21.1' then a.answer_value_text end) as q7_raw
    from {{ ref('int_qr_answers_long') }} a
    left join {{ ref('int_qr_tags') }} t on t.resource_id = a.qr_id
    where a.questionnaire_id = 'Questionnaire/202'
        and a.subject_patient_id is not null
    group by a.qr_id, a.subject_patient_id, t.practitioner_organization_id
),

gad7_s1 as (
    select
        subject_patient_id,
        organization_id,
        qr_id,
        'session_1' as session,
        case when q1_raw like '0 -%' or q1_raw = 'Not at all' then 0
             when q1_raw like '1 -%' or q1_raw = 'Several days' then 1
             when q1_raw like '2 -%' or q1_raw = 'More than half the days' then 2
             when q1_raw like '3 -%' or q1_raw = 'Nearly every day' then 3
             else null end as q1_nervous,
        case when q2_raw like '0 -%' or q2_raw = 'Not at all' then 0
             when q2_raw like '1 -%' or q2_raw = 'Several days' then 1
             when q2_raw like '2 -%' or q2_raw = 'More than half the days' then 2
             when q2_raw like '3 -%' or q2_raw = 'Nearly every day' then 3
             else null end as q2_worrying,
        case when q3_raw like '0 -%' or q3_raw = 'Not at all' then 0
             when q3_raw like '1 -%' or q3_raw = 'Several days' then 1
             when q3_raw like '2 -%' or q3_raw = 'More than half the days' then 2
             when q3_raw like '3 -%' or q3_raw = 'Nearly every day' then 3
             else null end as q3_worry_excess,
        case when q4_raw like '0 -%' or q4_raw = 'Not at all' then 0
             when q4_raw like '1 -%' or q4_raw = 'Several days' then 1
             when q4_raw like '2 -%' or q4_raw = 'More than half the days' then 2
             when q4_raw like '3 -%' or q4_raw = 'Nearly every day' then 3
             else null end as q4_relaxing,
        case when q5_raw like '0 -%' or q5_raw = 'Not at all' then 0
             when q5_raw like '1 -%' or q5_raw = 'Several days' then 1
             when q5_raw like '2 -%' or q5_raw = 'More than half the days' then 2
             when q5_raw like '3 -%' or q5_raw = 'Nearly every day' then 3
             else null end as q5_restless,
        case when q6_raw like '0 -%' or q6_raw = 'Not at all' then 0
             when q6_raw like '1 -%' or q6_raw = 'Several days' then 1
             when q6_raw like '2 -%' or q6_raw = 'More than half the days' then 2
             when q6_raw like '3 -%' or q6_raw = 'Nearly every day' then 3
             else null end as q6_irritable,
        case when q7_raw like '0 -%' or q7_raw = 'Not at all' then 0
             when q7_raw like '1 -%' or q7_raw = 'Several days' then 1
             when q7_raw like '2 -%' or q7_raw = 'More than half the days' then 2
             when q7_raw like '3 -%' or q7_raw = 'Nearly every day' then 3
             else null end as q7_afraid
    from gad7_s1_raw
),

gad7_scored as (
    select
        subject_patient_id,
        organization_id,
        qr_id,
        session,
        'GAD-7' as assessment_type,
        (coalesce(q1_nervous, 0) + coalesce(q2_worrying, 0) + coalesce(q3_worry_excess, 0) +
         coalesce(q4_relaxing, 0) + coalesce(q5_restless, 0) + coalesce(q6_irritable, 0) +
         coalesce(q7_afraid, 0)) as total_score,
        -- GAD-7 Severity: 0-4 Minimal, 5-9 Mild, 10-14 Moderate, 15-21 Severe
        case
            when (coalesce(q1_nervous, 0) + coalesce(q2_worrying, 0) + coalesce(q3_worry_excess, 0) +
                  coalesce(q4_relaxing, 0) + coalesce(q5_restless, 0) + coalesce(q6_irritable, 0) +
                  coalesce(q7_afraid, 0)) >= 15 then 'Severe'
            when (coalesce(q1_nervous, 0) + coalesce(q2_worrying, 0) + coalesce(q3_worry_excess, 0) +
                  coalesce(q4_relaxing, 0) + coalesce(q5_restless, 0) + coalesce(q6_irritable, 0) +
                  coalesce(q7_afraid, 0)) >= 10 then 'Moderate'
            when (coalesce(q1_nervous, 0) + coalesce(q2_worrying, 0) + coalesce(q3_worry_excess, 0) +
                  coalesce(q4_relaxing, 0) + coalesce(q5_restless, 0) + coalesce(q6_irritable, 0) +
                  coalesce(q7_afraid, 0)) >= 5 then 'Mild'
            else 'Minimal'
        end as severity
    from gad7_s1
),

-- Combine all assessments
combined as (
    select * from phq9_scored
    union all
    select * from gad7_scored
)

select
    subject_patient_id,
    organization_id,
    qr_id,
    session,
    assessment_type,
    total_score,
    severity,
    -- Map to indicator risk levels
    case
        when severity = 'Severe' then 'severe'
        when severity in ('Moderately Severe', 'Moderate') then 'moderate_high'
        when severity in ('Mild', 'Minimal') then 'low'
    end as risk_level
from combined
