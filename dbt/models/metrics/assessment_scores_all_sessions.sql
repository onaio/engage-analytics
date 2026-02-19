-- ABOUTME: Calculates PHQ-9 and GAD-7 scores across all 4 sessions
-- ABOUTME: Enables tracking symptom change over treatment (indicator #22)

{{ config(materialized='view') }}

-- Queries int_qr_answers_long directly by linkId to avoid dependency on
-- dynamically generated column names in wide views.
-- PHQ-9 linkIds f1.3.1-f1.11.1 are consistent across all session questionnaires.
-- Questionnaire IDs: s1=Questionnaire/54, s2=Questionnaire/57,
--                    s3=Questionnaire/61, s4=Questionnaire/211

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

-- PHQ-9 Session 1
phq9_s1 as (
    select
        subject_patient_id,
        organization_id,
        qr_id,
        1 as session_number,
        'PHQ-9' as assessment_type,
        (coalesce(case when q1_raw like '0 -%' or q1_raw = 'Not at all' then 0 when q1_raw like '1 -%' or q1_raw = 'Several days' then 1 when q1_raw like '2 -%' or q1_raw = 'More than half the days' then 2 when q1_raw like '3 -%' or q1_raw = 'Nearly every day' then 3 else null end, 0) +
         coalesce(case when q2_raw like '0 -%' or q2_raw = 'Not at all' then 0 when q2_raw like '1 -%' or q2_raw = 'Several days' then 1 when q2_raw like '2 -%' or q2_raw = 'More than half the days' then 2 when q2_raw like '3 -%' or q2_raw = 'Nearly every day' then 3 else null end, 0) +
         coalesce(case when q3_raw like '0 -%' or q3_raw = 'Not at all' then 0 when q3_raw like '1 -%' or q3_raw = 'Several days' then 1 when q3_raw like '2 -%' or q3_raw = 'More than half the days' then 2 when q3_raw like '3 -%' or q3_raw = 'Nearly every day' then 3 else null end, 0) +
         coalesce(case when q4_raw like '0 -%' or q4_raw = 'Not at all' then 0 when q4_raw like '1 -%' or q4_raw = 'Several days' then 1 when q4_raw like '2 -%' or q4_raw = 'More than half the days' then 2 when q4_raw like '3 -%' or q4_raw = 'Nearly every day' then 3 else null end, 0) +
         coalesce(case when q5_raw like '0 -%' or q5_raw = 'Not at all' then 0 when q5_raw like '1 -%' or q5_raw = 'Several days' then 1 when q5_raw like '2 -%' or q5_raw = 'More than half the days' then 2 when q5_raw like '3 -%' or q5_raw = 'Nearly every day' then 3 else null end, 0) +
         coalesce(case when q6_raw like '0 -%' or q6_raw = 'Not at all' then 0 when q6_raw like '1 -%' or q6_raw = 'Several days' then 1 when q6_raw like '2 -%' or q6_raw = 'More than half the days' then 2 when q6_raw like '3 -%' or q6_raw = 'Nearly every day' then 3 else null end, 0) +
         coalesce(case when q7_raw like '0 -%' or q7_raw = 'Not at all' then 0 when q7_raw like '1 -%' or q7_raw = 'Several days' then 1 when q7_raw like '2 -%' or q7_raw = 'More than half the days' then 2 when q7_raw like '3 -%' or q7_raw = 'Nearly every day' then 3 else null end, 0) +
         coalesce(case when q8_raw like '0 -%' or q8_raw = 'Not at all' then 0 when q8_raw like '1 -%' or q8_raw = 'Several days' then 1 when q8_raw like '2 -%' or q8_raw = 'More than half the days' then 2 when q8_raw like '3 -%' or q8_raw = 'Nearly every day' then 3 else null end, 0) +
         coalesce(case when q9_raw like '0 -%' or q9_raw = 'Not at all' then 0 when q9_raw like '1 -%' or q9_raw = 'Several days' then 1 when q9_raw like '2 -%' or q9_raw = 'More than half the days' then 2 when q9_raw like '3 -%' or q9_raw = 'Nearly every day' then 3 else null end, 0)) as total_score
    from phq9_s1_raw
),

phq9_s2_raw as (
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
    where a.questionnaire_id = 'Questionnaire/57'
        and a.subject_patient_id is not null
    group by a.qr_id, a.subject_patient_id, t.practitioner_organization_id
),

-- PHQ-9 Session 2
phq9_s2 as (
    select
        subject_patient_id,
        organization_id,
        qr_id,
        2 as session_number,
        'PHQ-9' as assessment_type,
        (coalesce(case when q1_raw like '0 -%' or q1_raw = 'Not at all' then 0 when q1_raw like '1 -%' or q1_raw = 'Several days' then 1 when q1_raw like '2 -%' or q1_raw = 'More than half the days' then 2 when q1_raw like '3 -%' or q1_raw = 'Nearly every day' then 3 else null end, 0) +
         coalesce(case when q2_raw like '0 -%' or q2_raw = 'Not at all' then 0 when q2_raw like '1 -%' or q2_raw = 'Several days' then 1 when q2_raw like '2 -%' or q2_raw = 'More than half the days' then 2 when q2_raw like '3 -%' or q2_raw = 'Nearly every day' then 3 else null end, 0) +
         coalesce(case when q3_raw like '0 -%' or q3_raw = 'Not at all' then 0 when q3_raw like '1 -%' or q3_raw = 'Several days' then 1 when q3_raw like '2 -%' or q3_raw = 'More than half the days' then 2 when q3_raw like '3 -%' or q3_raw = 'Nearly every day' then 3 else null end, 0) +
         coalesce(case when q4_raw like '0 -%' or q4_raw = 'Not at all' then 0 when q4_raw like '1 -%' or q4_raw = 'Several days' then 1 when q4_raw like '2 -%' or q4_raw = 'More than half the days' then 2 when q4_raw like '3 -%' or q4_raw = 'Nearly every day' then 3 else null end, 0) +
         coalesce(case when q5_raw like '0 -%' or q5_raw = 'Not at all' then 0 when q5_raw like '1 -%' or q5_raw = 'Several days' then 1 when q5_raw like '2 -%' or q5_raw = 'More than half the days' then 2 when q5_raw like '3 -%' or q5_raw = 'Nearly every day' then 3 else null end, 0) +
         coalesce(case when q6_raw like '0 -%' or q6_raw = 'Not at all' then 0 when q6_raw like '1 -%' or q6_raw = 'Several days' then 1 when q6_raw like '2 -%' or q6_raw = 'More than half the days' then 2 when q6_raw like '3 -%' or q6_raw = 'Nearly every day' then 3 else null end, 0) +
         coalesce(case when q7_raw like '0 -%' or q7_raw = 'Not at all' then 0 when q7_raw like '1 -%' or q7_raw = 'Several days' then 1 when q7_raw like '2 -%' or q7_raw = 'More than half the days' then 2 when q7_raw like '3 -%' or q7_raw = 'Nearly every day' then 3 else null end, 0) +
         coalesce(case when q8_raw like '0 -%' or q8_raw = 'Not at all' then 0 when q8_raw like '1 -%' or q8_raw = 'Several days' then 1 when q8_raw like '2 -%' or q8_raw = 'More than half the days' then 2 when q8_raw like '3 -%' or q8_raw = 'Nearly every day' then 3 else null end, 0) +
         coalesce(case when q9_raw like '0 -%' or q9_raw = 'Not at all' then 0 when q9_raw like '1 -%' or q9_raw = 'Several days' then 1 when q9_raw like '2 -%' or q9_raw = 'More than half the days' then 2 when q9_raw like '3 -%' or q9_raw = 'Nearly every day' then 3 else null end, 0)) as total_score
    from phq9_s2_raw
),

phq9_s3_raw as (
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
    where a.questionnaire_id = 'Questionnaire/61'
        and a.subject_patient_id is not null
    group by a.qr_id, a.subject_patient_id, t.practitioner_organization_id
),

-- PHQ-9 Session 3
phq9_s3 as (
    select
        subject_patient_id,
        organization_id,
        qr_id,
        3 as session_number,
        'PHQ-9' as assessment_type,
        (coalesce(case when q1_raw like '0 -%' or q1_raw = 'Not at all' then 0 when q1_raw like '1 -%' or q1_raw = 'Several days' then 1 when q1_raw like '2 -%' or q1_raw = 'More than half the days' then 2 when q1_raw like '3 -%' or q1_raw = 'Nearly every day' then 3 else null end, 0) +
         coalesce(case when q2_raw like '0 -%' or q2_raw = 'Not at all' then 0 when q2_raw like '1 -%' or q2_raw = 'Several days' then 1 when q2_raw like '2 -%' or q2_raw = 'More than half the days' then 2 when q2_raw like '3 -%' or q2_raw = 'Nearly every day' then 3 else null end, 0) +
         coalesce(case when q3_raw like '0 -%' or q3_raw = 'Not at all' then 0 when q3_raw like '1 -%' or q3_raw = 'Several days' then 1 when q3_raw like '2 -%' or q3_raw = 'More than half the days' then 2 when q3_raw like '3 -%' or q3_raw = 'Nearly every day' then 3 else null end, 0) +
         coalesce(case when q4_raw like '0 -%' or q4_raw = 'Not at all' then 0 when q4_raw like '1 -%' or q4_raw = 'Several days' then 1 when q4_raw like '2 -%' or q4_raw = 'More than half the days' then 2 when q4_raw like '3 -%' or q4_raw = 'Nearly every day' then 3 else null end, 0) +
         coalesce(case when q5_raw like '0 -%' or q5_raw = 'Not at all' then 0 when q5_raw like '1 -%' or q5_raw = 'Several days' then 1 when q5_raw like '2 -%' or q5_raw = 'More than half the days' then 2 when q5_raw like '3 -%' or q5_raw = 'Nearly every day' then 3 else null end, 0) +
         coalesce(case when q6_raw like '0 -%' or q6_raw = 'Not at all' then 0 when q6_raw like '1 -%' or q6_raw = 'Several days' then 1 when q6_raw like '2 -%' or q6_raw = 'More than half the days' then 2 when q6_raw like '3 -%' or q6_raw = 'Nearly every day' then 3 else null end, 0) +
         coalesce(case when q7_raw like '0 -%' or q7_raw = 'Not at all' then 0 when q7_raw like '1 -%' or q7_raw = 'Several days' then 1 when q7_raw like '2 -%' or q7_raw = 'More than half the days' then 2 when q7_raw like '3 -%' or q7_raw = 'Nearly every day' then 3 else null end, 0) +
         coalesce(case when q8_raw like '0 -%' or q8_raw = 'Not at all' then 0 when q8_raw like '1 -%' or q8_raw = 'Several days' then 1 when q8_raw like '2 -%' or q8_raw = 'More than half the days' then 2 when q8_raw like '3 -%' or q8_raw = 'Nearly every day' then 3 else null end, 0) +
         coalesce(case when q9_raw like '0 -%' or q9_raw = 'Not at all' then 0 when q9_raw like '1 -%' or q9_raw = 'Several days' then 1 when q9_raw like '2 -%' or q9_raw = 'More than half the days' then 2 when q9_raw like '3 -%' or q9_raw = 'Nearly every day' then 3 else null end, 0)) as total_score
    from phq9_s3_raw
),

phq9_s4_raw as (
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
    where a.questionnaire_id = 'Questionnaire/211'
        and a.subject_patient_id is not null
    group by a.qr_id, a.subject_patient_id, t.practitioner_organization_id
),

-- PHQ-9 Session 4
phq9_s4 as (
    select
        subject_patient_id,
        organization_id,
        qr_id,
        4 as session_number,
        'PHQ-9' as assessment_type,
        (coalesce(case when q1_raw like '0 -%' or q1_raw = 'Not at all' then 0 when q1_raw like '1 -%' or q1_raw = 'Several days' then 1 when q1_raw like '2 -%' or q1_raw = 'More than half the days' then 2 when q1_raw like '3 -%' or q1_raw = 'Nearly every day' then 3 else null end, 0) +
         coalesce(case when q2_raw like '0 -%' or q2_raw = 'Not at all' then 0 when q2_raw like '1 -%' or q2_raw = 'Several days' then 1 when q2_raw like '2 -%' or q2_raw = 'More than half the days' then 2 when q2_raw like '3 -%' or q2_raw = 'Nearly every day' then 3 else null end, 0) +
         coalesce(case when q3_raw like '0 -%' or q3_raw = 'Not at all' then 0 when q3_raw like '1 -%' or q3_raw = 'Several days' then 1 when q3_raw like '2 -%' or q3_raw = 'More than half the days' then 2 when q3_raw like '3 -%' or q3_raw = 'Nearly every day' then 3 else null end, 0) +
         coalesce(case when q4_raw like '0 -%' or q4_raw = 'Not at all' then 0 when q4_raw like '1 -%' or q4_raw = 'Several days' then 1 when q4_raw like '2 -%' or q4_raw = 'More than half the days' then 2 when q4_raw like '3 -%' or q4_raw = 'Nearly every day' then 3 else null end, 0) +
         coalesce(case when q5_raw like '0 -%' or q5_raw = 'Not at all' then 0 when q5_raw like '1 -%' or q5_raw = 'Several days' then 1 when q5_raw like '2 -%' or q5_raw = 'More than half the days' then 2 when q5_raw like '3 -%' or q5_raw = 'Nearly every day' then 3 else null end, 0) +
         coalesce(case when q6_raw like '0 -%' or q6_raw = 'Not at all' then 0 when q6_raw like '1 -%' or q6_raw = 'Several days' then 1 when q6_raw like '2 -%' or q6_raw = 'More than half the days' then 2 when q6_raw like '3 -%' or q6_raw = 'Nearly every day' then 3 else null end, 0) +
         coalesce(case when q7_raw like '0 -%' or q7_raw = 'Not at all' then 0 when q7_raw like '1 -%' or q7_raw = 'Several days' then 1 when q7_raw like '2 -%' or q7_raw = 'More than half the days' then 2 when q7_raw like '3 -%' or q7_raw = 'Nearly every day' then 3 else null end, 0) +
         coalesce(case when q8_raw like '0 -%' or q8_raw = 'Not at all' then 0 when q8_raw like '1 -%' or q8_raw = 'Several days' then 1 when q8_raw like '2 -%' or q8_raw = 'More than half the days' then 2 when q8_raw like '3 -%' or q8_raw = 'Nearly every day' then 3 else null end, 0) +
         coalesce(case when q9_raw like '0 -%' or q9_raw = 'Not at all' then 0 when q9_raw like '1 -%' or q9_raw = 'Several days' then 1 when q9_raw like '2 -%' or q9_raw = 'More than half the days' then 2 when q9_raw like '3 -%' or q9_raw = 'Nearly every day' then 3 else null end, 0)) as total_score
    from phq9_s4_raw
),

-- Combine all PHQ-9 sessions
all_phq9 as (
    select * from phq9_s1
    union all select * from phq9_s2
    union all select * from phq9_s3
    union all select * from phq9_s4
),

-- Get first and last scores per patient for PHQ-9
phq9_change as (
    select
        subject_patient_id,
        organization_id,
        min(case when session_number = 1 then total_score end) as first_score,
        max(session_number) as last_session,
        max(case when session_number = (select max(session_number) from all_phq9 a2 where a2.subject_patient_id = all_phq9.subject_patient_id) then total_score end) as last_score
    from all_phq9
    group by subject_patient_id, organization_id
    having count(distinct session_number) > 1
)

select
    subject_patient_id,
    organization_id,
    'PHQ-9' as assessment_type,
    first_score,
    last_session,
    last_score,
    first_score - last_score as score_improvement,
    case when first_score > 0 then round(((first_score - last_score)::numeric / first_score) * 100, 1) else 0 end as percent_improvement,
    last_score < first_score as improved,
    first_score >= 10 and last_score < 10 as achieved_remission
from phq9_change
where first_score is not null and last_score is not null
