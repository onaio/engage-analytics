-- ABOUTME: Calculates PHQ-9 and GAD-7 total scores with severity classification
-- ABOUTME: Enables risk level metrics (indicators #18-20) from assessment questionnaires

{{ config(materialized='view') }}

-- PHQ-9 scores from session 1
with phq9_s1 as (
    select
        subject_patient_id,
        practitioner_organization_id as organization_id,
        qr_id,
        'session_1' as session,
        -- Extract numeric scores from text answers (handles "0 - Not at all" and "Not at all" formats)
        case when common_mental_health_symptoms_little_interest_or_pleasure_in__2 like '0 -%' or common_mental_health_symptoms_little_interest_or_pleasure_in__2 = 'Not at all' then 0
             when common_mental_health_symptoms_little_interest_or_pleasure_in__2 like '1 -%' or common_mental_health_symptoms_little_interest_or_pleasure_in__2 = 'Several days' then 1
             when common_mental_health_symptoms_little_interest_or_pleasure_in__2 like '2 -%' or common_mental_health_symptoms_little_interest_or_pleasure_in__2 = 'More than half the days' then 2
             when common_mental_health_symptoms_little_interest_or_pleasure_in__2 like '3 -%' or common_mental_health_symptoms_little_interest_or_pleasure_in__2 = 'Nearly every day' then 3
             else null end as q1_interest,
        case when common_mental_health_symptoms_feeling_down_depressed_or_hopel_2 like '0 -%' or common_mental_health_symptoms_feeling_down_depressed_or_hopel_2 = 'Not at all' then 0
             when common_mental_health_symptoms_feeling_down_depressed_or_hopel_2 like '1 -%' or common_mental_health_symptoms_feeling_down_depressed_or_hopel_2 = 'Several days' then 1
             when common_mental_health_symptoms_feeling_down_depressed_or_hopel_2 like '2 -%' or common_mental_health_symptoms_feeling_down_depressed_or_hopel_2 = 'More than half the days' then 2
             when common_mental_health_symptoms_feeling_down_depressed_or_hopel_2 like '3 -%' or common_mental_health_symptoms_feeling_down_depressed_or_hopel_2 = 'Nearly every day' then 3
             else null end as q2_depression,
        case when common_mental_health_symptoms_trouble_falling_or_staying_asle_2 like '0 -%' or common_mental_health_symptoms_trouble_falling_or_staying_asle_2 = 'Not at all' then 0
             when common_mental_health_symptoms_trouble_falling_or_staying_asle_2 like '1 -%' or common_mental_health_symptoms_trouble_falling_or_staying_asle_2 = 'Several days' then 1
             when common_mental_health_symptoms_trouble_falling_or_staying_asle_2 like '2 -%' or common_mental_health_symptoms_trouble_falling_or_staying_asle_2 = 'More than half the days' then 2
             when common_mental_health_symptoms_trouble_falling_or_staying_asle_2 like '3 -%' or common_mental_health_symptoms_trouble_falling_or_staying_asle_2 = 'Nearly every day' then 3
             else null end as q3_sleep,
        case when common_mental_health_symptoms_feeling_tired_or_having_little__2 like '0 -%' or common_mental_health_symptoms_feeling_tired_or_having_little__2 = 'Not at all' then 0
             when common_mental_health_symptoms_feeling_tired_or_having_little__2 like '1 -%' or common_mental_health_symptoms_feeling_tired_or_having_little__2 = 'Several days' then 1
             when common_mental_health_symptoms_feeling_tired_or_having_little__2 like '2 -%' or common_mental_health_symptoms_feeling_tired_or_having_little__2 = 'More than half the days' then 2
             when common_mental_health_symptoms_feeling_tired_or_having_little__2 like '3 -%' or common_mental_health_symptoms_feeling_tired_or_having_little__2 = 'Nearly every day' then 3
             else null end as q4_fatigue,
        case when common_mental_health_symptoms_poor_appetite_or_overeating_2 like '0 -%' or common_mental_health_symptoms_poor_appetite_or_overeating_2 = 'Not at all' then 0
             when common_mental_health_symptoms_poor_appetite_or_overeating_2 like '1 -%' or common_mental_health_symptoms_poor_appetite_or_overeating_2 = 'Several days' then 1
             when common_mental_health_symptoms_poor_appetite_or_overeating_2 like '2 -%' or common_mental_health_symptoms_poor_appetite_or_overeating_2 = 'More than half the days' then 2
             when common_mental_health_symptoms_poor_appetite_or_overeating_2 like '3 -%' or common_mental_health_symptoms_poor_appetite_or_overeating_2 = 'Nearly every day' then 3
             else null end as q5_appetite,
        case when common_mental_health_symptoms_feeling_bad_about_yourself_or_t_2 like '0 -%' or common_mental_health_symptoms_feeling_bad_about_yourself_or_t_2 = 'Not at all' then 0
             when common_mental_health_symptoms_feeling_bad_about_yourself_or_t_2 like '1 -%' or common_mental_health_symptoms_feeling_bad_about_yourself_or_t_2 = 'Several days' then 1
             when common_mental_health_symptoms_feeling_bad_about_yourself_or_t_2 like '2 -%' or common_mental_health_symptoms_feeling_bad_about_yourself_or_t_2 = 'More than half the days' then 2
             when common_mental_health_symptoms_feeling_bad_about_yourself_or_t_2 like '3 -%' or common_mental_health_symptoms_feeling_bad_about_yourself_or_t_2 = 'Nearly every day' then 3
             else null end as q6_selfworth,
        case when common_mental_health_symptoms_trouble_concentrating_on_things_2 like '0 -%' or common_mental_health_symptoms_trouble_concentrating_on_things_2 = 'Not at all' then 0
             when common_mental_health_symptoms_trouble_concentrating_on_things_2 like '1 -%' or common_mental_health_symptoms_trouble_concentrating_on_things_2 = 'Several days' then 1
             when common_mental_health_symptoms_trouble_concentrating_on_things_2 like '2 -%' or common_mental_health_symptoms_trouble_concentrating_on_things_2 = 'More than half the days' then 2
             when common_mental_health_symptoms_trouble_concentrating_on_things_2 like '3 -%' or common_mental_health_symptoms_trouble_concentrating_on_things_2 = 'Nearly every day' then 3
             else null end as q7_concentration,
        case when common_mental_health_symptoms_moving_or_speaking_so_slowly_th_2 like '0 -%' or common_mental_health_symptoms_moving_or_speaking_so_slowly_th_2 = 'Not at all' then 0
             when common_mental_health_symptoms_moving_or_speaking_so_slowly_th_2 like '1 -%' or common_mental_health_symptoms_moving_or_speaking_so_slowly_th_2 = 'Several days' then 1
             when common_mental_health_symptoms_moving_or_speaking_so_slowly_th_2 like '2 -%' or common_mental_health_symptoms_moving_or_speaking_so_slowly_th_2 = 'More than half the days' then 2
             when common_mental_health_symptoms_moving_or_speaking_so_slowly_th_2 like '3 -%' or common_mental_health_symptoms_moving_or_speaking_so_slowly_th_2 = 'Nearly every day' then 3
             else null end as q8_movement,
        case when common_mental_health_symptoms_thoughts_that_you_would_be_bett_2 like '0 -%' or common_mental_health_symptoms_thoughts_that_you_would_be_bett_2 = 'Not at all' then 0
             when common_mental_health_symptoms_thoughts_that_you_would_be_bett_2 like '1 -%' or common_mental_health_symptoms_thoughts_that_you_would_be_bett_2 = 'Several days' then 1
             when common_mental_health_symptoms_thoughts_that_you_would_be_bett_2 like '2 -%' or common_mental_health_symptoms_thoughts_that_you_would_be_bett_2 = 'More than half the days' then 2
             when common_mental_health_symptoms_thoughts_that_you_would_be_bett_2 like '3 -%' or common_mental_health_symptoms_thoughts_that_you_would_be_bett_2 = 'Nearly every day' then 3
             else null end as q9_selfharm
    from {{ ref('qr_phq_9_s1') }}
    where subject_patient_id is not null
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
gad7_s1 as (
    select
        subject_patient_id,
        practitioner_organization_id as organization_id,
        qr_id,
        'session_1' as session,
        case when common_mental_health_symptoms_feeling_nervous_anxious_or_on_e_2 like '0 -%' or common_mental_health_symptoms_feeling_nervous_anxious_or_on_e_2 = 'Not at all' then 0
             when common_mental_health_symptoms_feeling_nervous_anxious_or_on_e_2 like '1 -%' or common_mental_health_symptoms_feeling_nervous_anxious_or_on_e_2 = 'Several days' then 1
             when common_mental_health_symptoms_feeling_nervous_anxious_or_on_e_2 like '2 -%' or common_mental_health_symptoms_feeling_nervous_anxious_or_on_e_2 = 'More than half the days' then 2
             when common_mental_health_symptoms_feeling_nervous_anxious_or_on_e_2 like '3 -%' or common_mental_health_symptoms_feeling_nervous_anxious_or_on_e_2 = 'Nearly every day' then 3
             else null end as q1_nervous,
        case when common_mental_health_symptoms_not_being_able_to_stop_or_contr_2 like '0 -%' or common_mental_health_symptoms_not_being_able_to_stop_or_contr_2 = 'Not at all' then 0
             when common_mental_health_symptoms_not_being_able_to_stop_or_contr_2 like '1 -%' or common_mental_health_symptoms_not_being_able_to_stop_or_contr_2 = 'Several days' then 1
             when common_mental_health_symptoms_not_being_able_to_stop_or_contr_2 like '2 -%' or common_mental_health_symptoms_not_being_able_to_stop_or_contr_2 = 'More than half the days' then 2
             when common_mental_health_symptoms_not_being_able_to_stop_or_contr_2 like '3 -%' or common_mental_health_symptoms_not_being_able_to_stop_or_contr_2 = 'Nearly every day' then 3
             else null end as q2_worrying,
        case when common_mental_health_symptoms_worrying_too_much_about_differe_2 like '0 -%' or common_mental_health_symptoms_worrying_too_much_about_differe_2 = 'Not at all' then 0
             when common_mental_health_symptoms_worrying_too_much_about_differe_2 like '1 -%' or common_mental_health_symptoms_worrying_too_much_about_differe_2 = 'Several days' then 1
             when common_mental_health_symptoms_worrying_too_much_about_differe_2 like '2 -%' or common_mental_health_symptoms_worrying_too_much_about_differe_2 = 'More than half the days' then 2
             when common_mental_health_symptoms_worrying_too_much_about_differe_2 like '3 -%' or common_mental_health_symptoms_worrying_too_much_about_differe_2 = 'Nearly every day' then 3
             else null end as q3_worry_excess,
        case when common_mental_health_symptoms_trouble_relaxing_2 like '0 -%' or common_mental_health_symptoms_trouble_relaxing_2 = 'Not at all' then 0
             when common_mental_health_symptoms_trouble_relaxing_2 like '1 -%' or common_mental_health_symptoms_trouble_relaxing_2 = 'Several days' then 1
             when common_mental_health_symptoms_trouble_relaxing_2 like '2 -%' or common_mental_health_symptoms_trouble_relaxing_2 = 'More than half the days' then 2
             when common_mental_health_symptoms_trouble_relaxing_2 like '3 -%' or common_mental_health_symptoms_trouble_relaxing_2 = 'Nearly every day' then 3
             else null end as q4_relaxing,
        case when common_mental_health_symptoms_being_so_restless_that_it_s_har_2 like '0 -%' or common_mental_health_symptoms_being_so_restless_that_it_s_har_2 = 'Not at all' then 0
             when common_mental_health_symptoms_being_so_restless_that_it_s_har_2 like '1 -%' or common_mental_health_symptoms_being_so_restless_that_it_s_har_2 = 'Several days' then 1
             when common_mental_health_symptoms_being_so_restless_that_it_s_har_2 like '2 -%' or common_mental_health_symptoms_being_so_restless_that_it_s_har_2 = 'More than half the days' then 2
             when common_mental_health_symptoms_being_so_restless_that_it_s_har_2 like '3 -%' or common_mental_health_symptoms_being_so_restless_that_it_s_har_2 = 'Nearly every day' then 3
             else null end as q5_restless,
        case when common_mental_health_symptoms_becoming_easily_annoyed_or_irri_2 like '0 -%' or common_mental_health_symptoms_becoming_easily_annoyed_or_irri_2 = 'Not at all' then 0
             when common_mental_health_symptoms_becoming_easily_annoyed_or_irri_2 like '1 -%' or common_mental_health_symptoms_becoming_easily_annoyed_or_irri_2 = 'Several days' then 1
             when common_mental_health_symptoms_becoming_easily_annoyed_or_irri_2 like '2 -%' or common_mental_health_symptoms_becoming_easily_annoyed_or_irri_2 = 'More than half the days' then 2
             when common_mental_health_symptoms_becoming_easily_annoyed_or_irri_2 like '3 -%' or common_mental_health_symptoms_becoming_easily_annoyed_or_irri_2 = 'Nearly every day' then 3
             else null end as q6_irritable,
        case when common_mental_health_symptoms_feeling_afraid_as_if_something__2 like '0 -%' or common_mental_health_symptoms_feeling_afraid_as_if_something__2 = 'Not at all' then 0
             when common_mental_health_symptoms_feeling_afraid_as_if_something__2 like '1 -%' or common_mental_health_symptoms_feeling_afraid_as_if_something__2 = 'Several days' then 1
             when common_mental_health_symptoms_feeling_afraid_as_if_something__2 like '2 -%' or common_mental_health_symptoms_feeling_afraid_as_if_something__2 = 'More than half the days' then 2
             when common_mental_health_symptoms_feeling_afraid_as_if_something__2 like '3 -%' or common_mental_health_symptoms_feeling_afraid_as_if_something__2 = 'Nearly every day' then 3
             else null end as q7_afraid
    from {{ ref('qr_gad_7_s1') }}
    where subject_patient_id is not null
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
