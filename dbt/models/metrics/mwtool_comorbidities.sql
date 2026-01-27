-- ABOUTME: Clients with comorbidities (multiple positive mwTool results)
-- ABOUTME: Supports indicator #22 (Comorbidities)

{{ config(materialized='view') }}

-- Extract all mwTool flags per client
with mwtool_flags as (
    select
        qr.subject_patient_id,
        p.practitioner_organization_id as organization_id,
        -- Extract boolean flags
        (jsonb_path_query_first(qr.items::jsonb, 
            '$.**.item[*] ? (@.linkId == "no-mental-problem").answer[0].valueBoolean'))::boolean as no_mental_problem,
        (jsonb_path_query_first(qr.items::jsonb, 
            '$.**.item[*] ? (@.linkId == "alcohol-problem").answer[0].valueBoolean'))::boolean as alcohol_problem,
        (jsonb_path_query_first(qr.items::jsonb, 
            '$.**.item[*] ? (@.linkId == "drug-problem").answer[0].valueBoolean'))::boolean as drug_problem,
        (jsonb_path_query_first(qr.items::jsonb, 
            '$.**.item[*] ? (@.linkId == "suicide-risk").answer[0].valueBoolean'))::boolean as suicide_risk
    from {{ ref('stg_questionnaire_response') }} qr
    left join {{ ref('patient') }} p on p.id = qr.subject_patient_id
    where qr.questionnaire_id = 'Questionnaire/1613532'
      and qr.subject_patient_id is not null
),

-- Aggregate to client level and count positive flags
client_summary as (
    select
        subject_patient_id,
        organization_id,
        -- A client has common mental health if no_mental_problem is false
        bool_or(no_mental_problem = false) as has_common_mh,
        bool_or(alcohol_problem = true) as has_alcohol_problem,
        bool_or(drug_problem = true) as has_drug_problem,
        bool_or(suicide_risk = true) as has_suicide_risk
    from mwtool_flags
    group by 1, 2
),

-- Calculate positive count
with_counts as (
    select
        subject_patient_id,
        organization_id,
        has_common_mh,
        has_alcohol_problem,
        has_drug_problem,
        has_suicide_risk,
        (case when has_common_mh then 1 else 0 end +
         case when has_alcohol_problem then 1 else 0 end +
         case when has_drug_problem then 1 else 0 end +
         case when has_suicide_risk then 1 else 0 end) as positive_category_count
    from client_summary
)

select
    subject_patient_id,
    organization_id,
    has_common_mh,
    has_alcohol_problem,
    has_drug_problem,
    has_suicide_risk,
    positive_category_count,
    positive_category_count > 1 as has_comorbidity
from with_counts
where positive_category_count >= 1  -- At least one positive result
