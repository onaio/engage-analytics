{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_spi_subform_4 with PII fields masked based on questionnaire_metadata.anon flag
-- Auto-generated to mask fields marked as anon=TRUE in questionnaire_metadata

with metadata_pii as (
    select distinct
        question_alias
    from {{ ref('questionnaire_metadata') }}
    where questionnaire_id in ('Questionnaire/104394')
    and anon = 'TRUE'
),

source_data as (
    select * from {{ ref('qr_spi_subform_4') }}
)

select 
    MD5(COALESCE(qr_id, '')::text) as qr_id_hash,
    questionnaire_id,
    MD5(COALESCE(subject_patient_id, '')::text) as subject_patient_id_hash,
    encounter_id,
    author_practitioner_id,
    practitioner_location_id,
    practitioner_organization_id,
    practitioner_id,
    practitioner_careteam_id,
    application_version,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_encounter_id_of_spi_sub_4')
        THEN 'REDACTED'
        ELSE spi_encounter_id_of_spi_sub_4::text
    END as spi_encounter_id_of_spi_sub_4,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_taking_pills_i_will_protect_myself_from_this_risk_')
        THEN 'REDACTED'
        ELSE spi_taking_pills_i_will_protect_myself_from_this_risk_::text
    END as spi_taking_pills_i_will_protect_myself_from_this_risk_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_in_a_few_days_i_would_like_to_meet_for_a_followup_')
        THEN 'REDACTED'
        ELSE spi_in_a_few_days_i_would_like_to_meet_for_a_followup_::text
    END as spi_in_a_few_days_i_would_like_to_meet_for_a_followup_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_person_1')
        THEN 'REDACTED'
        ELSE spi_person_1::text
    END as spi_person_1,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_when_you_feel_a_suicidal_crisis_arising_go_through')
        THEN 'REDACTED'
        ELSE spi_when_you_feel_a_suicidal_crisis_arising_go_through::text
    END as spi_when_you_feel_a_suicidal_crisis_arising_go_through,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_i_would_like_to_make_a_plan_that_will_help_you_man')
        THEN 'REDACTED'
        ELSE spi_i_would_like_to_make_a_plan_that_will_help_you_man::text
    END as spi_i_would_like_to_make_a_plan_that_will_help_you_man,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_cce2de12cf6244ff84e89eb4d864d048')
        THEN 'REDACTED'
        ELSE spi_cce2de12cf6244ff84e89eb4d864d048::text
    END as spi_cce2de12cf6244ff84e89eb4d864d048,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_using_a_firearm_i_will_protect_myself_from_this_ri')
        THEN 'REDACTED'
        ELSE spi_using_a_firearm_i_will_protect_myself_from_this_ri::text
    END as spi_using_a_firearm_i_will_protect_myself_from_this_ri,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_40fbe8d043804efcbb0426fc7d3714eb')
        THEN 'REDACTED'
        ELSE spi_40fbe8d043804efcbb0426fc7d3714eb::text
    END as spi_40fbe8d043804efcbb0426fc7d3714eb,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_the_methods_i_have_thought_about_using_include')
        THEN 'REDACTED'
        ELSE spi_the_methods_i_have_thought_about_using_include::text
    END as spi_the_methods_i_have_thought_about_using_include,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_person_1_11')
        THEN 'REDACTED'
        ELSE spi_person_1_11::text
    END as spi_person_1_11,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_90cedcbfbf114dc3973342e6900a09fc')
        THEN 'REDACTED'
        ELSE spi_90cedcbfbf114dc3973342e6900a09fc::text
    END as spi_90cedcbfbf114dc3973342e6900a09fc,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_92ab6e65f9bd47188af6f731425d3031')
        THEN 'REDACTED'
        ELSE spi_92ab6e65f9bd47188af6f731425d3031::text
    END as spi_92ab6e65f9bd47188af6f731425d3031,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_9d203bac75fd4f31bed6073502a977f3')
        THEN 'REDACTED'
        ELSE spi_9d203bac75fd4f31bed6073502a977f3::text
    END as spi_9d203bac75fd4f31bed6073502a977f3,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_additional_hotline_numbers_you_can_contact')
        THEN 'REDACTED'
        ELSE spi_additional_hotline_numbers_you_can_contact::text
    END as spi_additional_hotline_numbers_you_can_contact,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_taking_pills_i_will_protect_myself_from_this_risk__16')
        THEN 'REDACTED'
        ELSE spi_taking_pills_i_will_protect_myself_from_this_risk__16::text
    END as spi_taking_pills_i_will_protect_myself_from_this_risk__16,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_12d89dfd5e434f008446b3af23323969')
        THEN 'REDACTED'
        ELSE spi_12d89dfd5e434f008446b3af23323969::text
    END as spi_12d89dfd5e434f008446b3af23323969,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_39de8b745cbc4225b27bc97e72f08af3')
        THEN 'REDACTED'
        ELSE spi_39de8b745cbc4225b27bc97e72f08af3::text
    END as spi_39de8b745cbc4225b27bc97e72f08af3,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_437d9ea2adb5471a828ba0285a282217')
        THEN 'REDACTED'
        ELSE spi_437d9ea2adb5471a828ba0285a282217::text
    END as spi_437d9ea2adb5471a828ba0285a282217,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_509b140eb1954696bc5ff2fc2d2f77dd')
        THEN 'REDACTED'
        ELSE spi_509b140eb1954696bc5ff2fc2d2f77dd::text
    END as spi_509b140eb1954696bc5ff2fc2d2f77dd,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_53a575e3aa59408da0dd8df237b68bf7')
        THEN 'REDACTED'
        ELSE spi_53a575e3aa59408da0dd8df237b68bf7::text
    END as spi_53a575e3aa59408da0dd8df237b68bf7,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_a6d066a2028649b0b68834696e7fb256')
        THEN 'REDACTED'
        ELSE spi_a6d066a2028649b0b68834696e7fb256::text
    END as spi_a6d066a2028649b0b68834696e7fb256,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_ce56c71d9ea44c8e9e9ad1da93bac1d4')
        THEN 'REDACTED'
        ELSE spi_ce56c71d9ea44c8e9e9ad1da93bac1d4::text
    END as spi_ce56c71d9ea44c8e9e9ad1da93bac1d4,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_going_in_front_of_a_moving_vehicle_i_will_protect_')
        THEN 'REDACTED'
        ELSE spi_going_in_front_of_a_moving_vehicle_i_will_protect_::text
    END as spi_going_in_front_of_a_moving_vehicle_i_will_protect_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_person_1_25')
        THEN 'REDACTED'
        ELSE spi_person_1_25::text
    END as spi_person_1_25,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_person_1_26')
        THEN 'REDACTED'
        ELSE spi_person_1_26::text
    END as spi_person_1_26,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_beb4ad5f2fd84b80aa93aa9ff1896dd0')
        THEN 'REDACTED'
        ELSE spi_beb4ad5f2fd84b80aa93aa9ff1896dd0::text
    END as spi_beb4ad5f2fd84b80aa93aa9ff1896dd0,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_c396a3af6f0d4a8ea42b4f47bbce5f46')
        THEN 'REDACTED'
        ELSE spi_c396a3af6f0d4a8ea42b4f47bbce5f46::text
    END as spi_c396a3af6f0d4a8ea42b4f47bbce5f46,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_e45b778dee8c4ee981f48927006160f9')
        THEN 'REDACTED'
        ELSE spi_e45b778dee8c4ee981f48927006160f9::text
    END as spi_e45b778dee8c4ee981f48927006160f9,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_your_local_hotline_number')
        THEN 'REDACTED'
        ELSE spi_your_local_hotline_number::text
    END as spi_your_local_hotline_number,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_101d2b7f2b68407daa4930cb4dadc888')
        THEN 'REDACTED'
        ELSE spi_101d2b7f2b68407daa4930cb4dadc888::text
    END as spi_101d2b7f2b68407daa4930cb4dadc888,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_14ec8995eee9416f83ad9f44631ac9b0')
        THEN 'REDACTED'
        ELSE spi_14ec8995eee9416f83ad9f44631ac9b0::text
    END as spi_14ec8995eee9416f83ad9f44631ac9b0,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_18d8ee56ffd847119074126ef2cdc564')
        THEN 'REDACTED'
        ELSE spi_18d8ee56ffd847119074126ef2cdc564::text
    END as spi_18d8ee56ffd847119074126ef2cdc564,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_cutting_i_will_protect_myself_from_this_risk_by')
        THEN 'REDACTED'
        ELSE spi_cutting_i_will_protect_myself_from_this_risk_by::text
    END as spi_cutting_i_will_protect_myself_from_this_risk_by,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_5324bdcebaaa45bf935aeb2f65bd7124')
        THEN 'REDACTED'
        ELSE spi_5324bdcebaaa45bf935aeb2f65bd7124::text
    END as spi_5324bdcebaaa45bf935aeb2f65bd7124,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_68fa6f2e84ec4dbc900e10ede297c488')
        THEN 'REDACTED'
        ELSE spi_68fa6f2e84ec4dbc900e10ede297c488::text
    END as spi_68fa6f2e84ec4dbc900e10ede297c488,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_8891cd7bad1c4030afe46752cd273cf4')
        THEN 'REDACTED'
        ELSE spi_8891cd7bad1c4030afe46752cd273cf4::text
    END as spi_8891cd7bad1c4030afe46752cd273cf4,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_using_a_firearm_i_will_protect_myself_from_this_ri_38')
        THEN 'REDACTED'
        ELSE spi_using_a_firearm_i_will_protect_myself_from_this_ri_38::text
    END as spi_using_a_firearm_i_will_protect_myself_from_this_ri_38,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_taskid_spi_navigator')
        THEN 'REDACTED'
        ELSE spi_taskid_spi_navigator::text
    END as spi_taskid_spi_navigator,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_c631e361027c4e4db89448785a3e6dd0')
        THEN 'REDACTED'
        ELSE spi_c631e361027c4e4db89448785a3e6dd0::text
    END as spi_c631e361027c4e4db89448785a3e6dd0,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_taskid_my_safety_plan_pdf')
        THEN 'REDACTED'
        ELSE spi_taskid_my_safety_plan_pdf::text
    END as spi_taskid_my_safety_plan_pdf,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_a1c56f60b8234c15b99da315d2ca1871')
        THEN 'REDACTED'
        ELSE spi_a1c56f60b8234c15b99da315d2ca1871::text
    END as spi_a1c56f60b8234c15b99da315d2ca1871,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_ae4fa455f173435abb1b4df1d1316bd9')
        THEN 'REDACTED'
        ELSE spi_ae4fa455f173435abb1b4df1d1316bd9::text
    END as spi_ae4fa455f173435abb1b4df1d1316bd9,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_bc981d8a7f22450babab3acca810ccad')
        THEN 'REDACTED'
        ELSE spi_bc981d8a7f22450babab3acca810ccad::text
    END as spi_bc981d8a7f22450babab3acca810ccad,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_c6c266cc1c8541c097ce9ab491be45db')
        THEN 'REDACTED'
        ELSE spi_c6c266cc1c8541c097ce9ab491be45db::text
    END as spi_c6c266cc1c8541c097ce9ab491be45db,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_person_2')
        THEN 'REDACTED'
        ELSE spi_person_2::text
    END as spi_person_2,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_7d8621a4dcc84f4892103fb7d0dcc3c7')
        THEN 'REDACTED'
        ELSE spi_7d8621a4dcc84f4892103fb7d0dcc3c7::text
    END as spi_7d8621a4dcc84f4892103fb7d0dcc3c7,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_83661ebe71cf4eab88e3bfecbfe35164')
        THEN 'REDACTED'
        ELSE spi_83661ebe71cf4eab88e3bfecbfe35164::text
    END as spi_83661ebe71cf4eab88e3bfecbfe35164,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_be88b02764b64c3cbf5936a94dc79b12')
        THEN 'REDACTED'
        ELSE spi_be88b02764b64c3cbf5936a94dc79b12::text
    END as spi_be88b02764b64c3cbf5936a94dc79b12,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_c3b4cf6e385d40b793804de2f81463de')
        THEN 'REDACTED'
        ELSE spi_c3b4cf6e385d40b793804de2f81463de::text
    END as spi_c3b4cf6e385d40b793804de2f81463de,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_hanging_i_will_protect_myself_from_this_risk_by')
        THEN 'REDACTED'
        ELSE spi_hanging_i_will_protect_myself_from_this_risk_by::text
    END as spi_hanging_i_will_protect_myself_from_this_risk_by,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_are_you_willing_to_share_your_plan_with_this_perso')
        THEN 'REDACTED'
        ELSE spi_are_you_willing_to_share_your_plan_with_this_perso::text
    END as spi_are_you_willing_to_share_your_plan_with_this_perso,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_going_in_front_of_a_moving_vehicle_i_will_protect__53')
        THEN 'REDACTED'
        ELSE spi_going_in_front_of_a_moving_vehicle_i_will_protect__53::text
    END as spi_going_in_front_of_a_moving_vehicle_i_will_protect__53,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_ff115c2695b94e0d87ed80817d438fc7')
        THEN 'REDACTED'
        ELSE spi_ff115c2695b94e0d87ed80817d438fc7::text
    END as spi_ff115c2695b94e0d87ed80817d438fc7,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_656a57f8f6f04fc5b34771dd8d38f596')
        THEN 'REDACTED'
        ELSE spi_656a57f8f6f04fc5b34771dd8d38f596::text
    END as spi_656a57f8f6f04fc5b34771dd8d38f596,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_f749685963bb4b2f8f7596e44e893bf7')
        THEN 'REDACTED'
        ELSE spi_f749685963bb4b2f8f7596e44e893bf7::text
    END as spi_f749685963bb4b2f8f7596e44e893bf7,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_f8861b1c781f4f12b4c8f8110f0c2cc0')
        THEN 'REDACTED'
        ELSE spi_f8861b1c781f4f12b4c8f8110f0c2cc0::text
    END as spi_f8861b1c781f4f12b4c8f8110f0c2cc0,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_15e5007d028f46448fb60346730c3101')
        THEN 'REDACTED'
        ELSE spi_15e5007d028f46448fb60346730c3101::text
    END as spi_15e5007d028f46448fb60346730c3101,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_jumping_i_will_protect_myself_from_this_risk_by')
        THEN 'REDACTED'
        ELSE spi_jumping_i_will_protect_myself_from_this_risk_by::text
    END as spi_jumping_i_will_protect_myself_from_this_risk_by,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_are_you_willing_to_share_your_plan_with_this_perso_60')
        THEN 'REDACTED'
        ELSE spi_are_you_willing_to_share_your_plan_with_this_perso_60::text
    END as spi_are_you_willing_to_share_your_plan_with_this_perso_60,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_2b2e05e90c11490cbbff278ba90d7b4e')
        THEN 'REDACTED'
        ELSE spi_2b2e05e90c11490cbbff278ba90d7b4e::text
    END as spi_2b2e05e90c11490cbbff278ba90d7b4e,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_3e388595a32f46238c9f3c67bad04070')
        THEN 'REDACTED'
        ELSE spi_3e388595a32f46238c9f3c67bad04070::text
    END as spi_3e388595a32f46238c9f3c67bad04070,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_62b0eb1746e54c50a133087cc190a123')
        THEN 'REDACTED'
        ELSE spi_62b0eb1746e54c50a133087cc190a123::text
    END as spi_62b0eb1746e54c50a133087cc190a123,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_cutting_i_will_protect_myself_from_this_risk_by_64')
        THEN 'REDACTED'
        ELSE spi_cutting_i_will_protect_myself_from_this_risk_by_64::text
    END as spi_cutting_i_will_protect_myself_from_this_risk_by_64,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_705df2d9af924f4fb51317e01d07b609')
        THEN 'REDACTED'
        ELSE spi_705df2d9af924f4fb51317e01d07b609::text
    END as spi_705df2d9af924f4fb51317e01d07b609,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_7845c436490441fa95532c401935623b')
        THEN 'REDACTED'
        ELSE spi_7845c436490441fa95532c401935623b::text
    END as spi_7845c436490441fa95532c401935623b,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_person_2_67')
        THEN 'REDACTED'
        ELSE spi_person_2_67::text
    END as spi_person_2_67,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_person_2_68')
        THEN 'REDACTED'
        ELSE spi_person_2_68::text
    END as spi_person_2_68,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_c802f463c8f54056bc458ab8d64a1966')
        THEN 'REDACTED'
        ELSE spi_c802f463c8f54056bc458ab8d64a1966::text
    END as spi_c802f463c8f54056bc458ab8d64a1966,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_hanging_i_will_protect_myself_from_this_risk_by_70')
        THEN 'REDACTED'
        ELSE spi_hanging_i_will_protect_myself_from_this_risk_by_70::text
    END as spi_hanging_i_will_protect_myself_from_this_risk_by_70,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_2a6c5feb8034418b8644142a5e31df63')
        THEN 'REDACTED'
        ELSE spi_2a6c5feb8034418b8644142a5e31df63::text
    END as spi_2a6c5feb8034418b8644142a5e31df63,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_3785a2648bd544abbf088aa33735c0eb')
        THEN 'REDACTED'
        ELSE spi_3785a2648bd544abbf088aa33735c0eb::text
    END as spi_3785a2648bd544abbf088aa33735c0eb,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_person_2_73')
        THEN 'REDACTED'
        ELSE spi_person_2_73::text
    END as spi_person_2_73,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_603d1492316f497f967b5c4b1ab983ea')
        THEN 'REDACTED'
        ELSE spi_603d1492316f497f967b5c4b1ab983ea::text
    END as spi_603d1492316f497f967b5c4b1ab983ea,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_9f3a622ad39b49b084e70bcf4f832a4f')
        THEN 'REDACTED'
        ELSE spi_9f3a622ad39b49b084e70bcf4f832a4f::text
    END as spi_9f3a622ad39b49b084e70bcf4f832a4f,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_e9c42647eefb42bd9080f83630729606')
        THEN 'REDACTED'
        ELSE spi_e9c42647eefb42bd9080f83630729606::text
    END as spi_e9c42647eefb42bd9080f83630729606,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_drowning_i_will_protect_myself_from_this_risk_by')
        THEN 'REDACTED'
        ELSE spi_drowning_i_will_protect_myself_from_this_risk_by::text
    END as spi_drowning_i_will_protect_myself_from_this_risk_by,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_ee5d4acfecf04e628924cff17a33679d')
        THEN 'REDACTED'
        ELSE spi_ee5d4acfecf04e628924cff17a33679d::text
    END as spi_ee5d4acfecf04e628924cff17a33679d,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_fe2f09118ddc412ba3e6f216620d147e')
        THEN 'REDACTED'
        ELSE spi_fe2f09118ddc412ba3e6f216620d147e::text
    END as spi_fe2f09118ddc412ba3e6f216620d147e,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_370c849b0e0042acbe9d3b4c719ac31a')
        THEN 'REDACTED'
        ELSE spi_370c849b0e0042acbe9d3b4c719ac31a::text
    END as spi_370c849b0e0042acbe9d3b4c719ac31a,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_person_3')
        THEN 'REDACTED'
        ELSE spi_person_3::text
    END as spi_person_3,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_08b5e17c5e144b19ac939e479cd2a2bb')
        THEN 'REDACTED'
        ELSE spi_08b5e17c5e144b19ac939e479cd2a2bb::text
    END as spi_08b5e17c5e144b19ac939e479cd2a2bb,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_13321778b5d44b018eda610cf0007c86')
        THEN 'REDACTED'
        ELSE spi_13321778b5d44b018eda610cf0007c86::text
    END as spi_13321778b5d44b018eda610cf0007c86,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_jumping_i_will_protect_myself_from_this_risk_by_84')
        THEN 'REDACTED'
        ELSE spi_jumping_i_will_protect_myself_from_this_risk_by_84::text
    END as spi_jumping_i_will_protect_myself_from_this_risk_by_84,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_84018cd89134429c8dfefd6a16e4bd17')
        THEN 'REDACTED'
        ELSE spi_84018cd89134429c8dfefd6a16e4bd17::text
    END as spi_84018cd89134429c8dfefd6a16e4bd17,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_other_ways_i_will_make_my_environment_safer')
        THEN 'REDACTED'
        ELSE spi_other_ways_i_will_make_my_environment_safer::text
    END as spi_other_ways_i_will_make_my_environment_safer,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_fdd7c3c3f2c343bd8a806b48aa2b9604')
        THEN 'REDACTED'
        ELSE spi_fdd7c3c3f2c343bd8a806b48aa2b9604::text
    END as spi_fdd7c3c3f2c343bd8a806b48aa2b9604,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_8a9424b0c8934371b5c9bc0008be14b4')
        THEN 'REDACTED'
        ELSE spi_8a9424b0c8934371b5c9bc0008be14b4::text
    END as spi_8a9424b0c8934371b5c9bc0008be14b4,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_a5d96f90cd914a5b8862c4e3f88694af')
        THEN 'REDACTED'
        ELSE spi_a5d96f90cd914a5b8862c4e3f88694af::text
    END as spi_a5d96f90cd914a5b8862c4e3f88694af,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_person_3_90')
        THEN 'REDACTED'
        ELSE spi_person_3_90::text
    END as spi_person_3_90,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_28b2e31aee4d4e998974b616145311eb')
        THEN 'REDACTED'
        ELSE spi_28b2e31aee4d4e998974b616145311eb::text
    END as spi_28b2e31aee4d4e998974b616145311eb,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_drowning_i_will_protect_myself_from_this_risk_by_92')
        THEN 'REDACTED'
        ELSE spi_drowning_i_will_protect_myself_from_this_risk_by_92::text
    END as spi_drowning_i_will_protect_myself_from_this_risk_by_92,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_7e38a614b0a248eab7c7ca2bdc1f00db')
        THEN 'REDACTED'
        ELSE spi_7e38a614b0a248eab7c7ca2bdc1f00db::text
    END as spi_7e38a614b0a248eab7c7ca2bdc1f00db,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_c1e99f8bb1ab46d1b205ecbbb4240fdf')
        THEN 'REDACTED'
        ELSE spi_c1e99f8bb1ab46d1b205ecbbb4240fdf::text
    END as spi_c1e99f8bb1ab46d1b205ecbbb4240fdf,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_are_you_willing_to_share_your_plan_with_this_perso_95')
        THEN 'REDACTED'
        ELSE spi_are_you_willing_to_share_your_plan_with_this_perso_95::text
    END as spi_are_you_willing_to_share_your_plan_with_this_perso_95,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_d4ebe78407d043128a46634c0e338a54')
        THEN 'REDACTED'
        ELSE spi_d4ebe78407d043128a46634c0e338a54::text
    END as spi_d4ebe78407d043128a46634c0e338a54,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_53def3a1a7e5432695f22561c26ecb47')
        THEN 'REDACTED'
        ELSE spi_53def3a1a7e5432695f22561c26ecb47::text
    END as spi_53def3a1a7e5432695f22561c26ecb47,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_2b96f1d93450423da277060fcfdadc6b')
        THEN 'REDACTED'
        ELSE spi_2b96f1d93450423da277060fcfdadc6b::text
    END as spi_2b96f1d93450423da277060fcfdadc6b,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_person_4')
        THEN 'REDACTED'
        ELSE spi_person_4::text
    END as spi_person_4,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_b36fdd5fccac457ca8eaf9497204ef1d')
        THEN 'REDACTED'
        ELSE spi_b36fdd5fccac457ca8eaf9497204ef1d::text
    END as spi_b36fdd5fccac457ca8eaf9497204ef1d,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_are_you_willing_to_share_your_plan_with_this_perso_101')
        THEN 'REDACTED'
        ELSE spi_are_you_willing_to_share_your_plan_with_this_perso_101::text
    END as spi_are_you_willing_to_share_your_plan_with_this_perso_101,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_other_ways_i_will_make_my_environment_safer_102')
        THEN 'REDACTED'
        ELSE spi_other_ways_i_will_make_my_environment_safer_102::text
    END as spi_other_ways_i_will_make_my_environment_safer_102,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_person_3_103')
        THEN 'REDACTED'
        ELSE spi_person_3_103::text
    END as spi_person_3_103,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_f952feaf7abc4f6fa3bcd10a43ea62fa')
        THEN 'REDACTED'
        ELSE spi_f952feaf7abc4f6fa3bcd10a43ea62fa::text
    END as spi_f952feaf7abc4f6fa3bcd10a43ea62fa,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_bd7644ee28f14f43a65417fd5034d1f6')
        THEN 'REDACTED'
        ELSE spi_bd7644ee28f14f43a65417fd5034d1f6::text
    END as spi_bd7644ee28f14f43a65417fd5034d1f6,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_72e5c1b77ce949bd834bd98b73ba7090')
        THEN 'REDACTED'
        ELSE spi_72e5c1b77ce949bd834bd98b73ba7090::text
    END as spi_72e5c1b77ce949bd834bd98b73ba7090,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_person_4_107')
        THEN 'REDACTED'
        ELSE spi_person_4_107::text
    END as spi_person_4_107,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_d82c6ae90516451391fa664686779d6a')
        THEN 'REDACTED'
        ELSE spi_d82c6ae90516451391fa664686779d6a::text
    END as spi_d82c6ae90516451391fa664686779d6a,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_person_3_109')
        THEN 'REDACTED'
        ELSE spi_person_3_109::text
    END as spi_person_3_109,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_fc5c673b41d4435286bb043c73ed9807')
        THEN 'REDACTED'
        ELSE spi_fc5c673b41d4435286bb043c73ed9807::text
    END as spi_fc5c673b41d4435286bb043c73ed9807,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_0397084acd514b3a809d970a2406ea49')
        THEN 'REDACTED'
        ELSE spi_0397084acd514b3a809d970a2406ea49::text
    END as spi_0397084acd514b3a809d970a2406ea49,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_6a273a9882ca4d36ade0ce98f7899972')
        THEN 'REDACTED'
        ELSE spi_6a273a9882ca4d36ade0ce98f7899972::text
    END as spi_6a273a9882ca4d36ade0ce98f7899972,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_11aadd880cf948b2939d2b3e28191c5f')
        THEN 'REDACTED'
        ELSE spi_11aadd880cf948b2939d2b3e28191c5f::text
    END as spi_11aadd880cf948b2939d2b3e28191c5f,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_57e87170761c4b57a09cd50cab02860c')
        THEN 'REDACTED'
        ELSE spi_57e87170761c4b57a09cd50cab02860c::text
    END as spi_57e87170761c4b57a09cd50cab02860c,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_965b7874dd244215acd48d241a262638')
        THEN 'REDACTED'
        ELSE spi_965b7874dd244215acd48d241a262638::text
    END as spi_965b7874dd244215acd48d241a262638,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_bd62b64972094d329bbbdc2b2823aadb')
        THEN 'REDACTED'
        ELSE spi_bd62b64972094d329bbbdc2b2823aadb::text
    END as spi_bd62b64972094d329bbbdc2b2823aadb,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_07c2e8dcb48b4bc7b21bc196cce32709')
        THEN 'REDACTED'
        ELSE spi_07c2e8dcb48b4bc7b21bc196cce32709::text
    END as spi_07c2e8dcb48b4bc7b21bc196cce32709,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_person_5')
        THEN 'REDACTED'
        ELSE spi_person_5::text
    END as spi_person_5,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_ae94969792ae476a96ccf8b0adc4221f')
        THEN 'REDACTED'
        ELSE spi_ae94969792ae476a96ccf8b0adc4221f::text
    END as spi_ae94969792ae476a96ccf8b0adc4221f,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_2c7b6bd736f6495b9f41eb492c07c7b2')
        THEN 'REDACTED'
        ELSE spi_2c7b6bd736f6495b9f41eb492c07c7b2::text
    END as spi_2c7b6bd736f6495b9f41eb492c07c7b2,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_are_you_willing_to_share_your_plan_with_this_perso_121')
        THEN 'REDACTED'
        ELSE spi_are_you_willing_to_share_your_plan_with_this_perso_121::text
    END as spi_are_you_willing_to_share_your_plan_with_this_perso_121,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_fefdc90ffe1d4bfbaf9394df8c482897')
        THEN 'REDACTED'
        ELSE spi_fefdc90ffe1d4bfbaf9394df8c482897::text
    END as spi_fefdc90ffe1d4bfbaf9394df8c482897,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_1fc8f8e7202248d2b41d4e8ad22af344')
        THEN 'REDACTED'
        ELSE spi_1fc8f8e7202248d2b41d4e8ad22af344::text
    END as spi_1fc8f8e7202248d2b41d4e8ad22af344,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_8c1ac4fe1d7644368f34d315c6c17b9c')
        THEN 'REDACTED'
        ELSE spi_8c1ac4fe1d7644368f34d315c6c17b9c::text
    END as spi_8c1ac4fe1d7644368f34d315c6c17b9c,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_person_5_125')
        THEN 'REDACTED'
        ELSE spi_person_5_125::text
    END as spi_person_5_125,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_df53c37827fe483da630f7b171f72980')
        THEN 'REDACTED'
        ELSE spi_df53c37827fe483da630f7b171f72980::text
    END as spi_df53c37827fe483da630f7b171f72980,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_e552afd32816446e9ff5e898bc813660')
        THEN 'REDACTED'
        ELSE spi_e552afd32816446e9ff5e898bc813660::text
    END as spi_e552afd32816446e9ff5e898bc813660,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_are_you_willing_to_share_your_plan_with_this_perso_128')
        THEN 'REDACTED'
        ELSE spi_are_you_willing_to_share_your_plan_with_this_perso_128::text
    END as spi_are_you_willing_to_share_your_plan_with_this_perso_128,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_person_4_129')
        THEN 'REDACTED'
        ELSE spi_person_4_129::text
    END as spi_person_4_129,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_44f024678ee049f08da75e7ed827125a')
        THEN 'REDACTED'
        ELSE spi_44f024678ee049f08da75e7ed827125a::text
    END as spi_44f024678ee049f08da75e7ed827125a,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_82d6b50b0bb240f694b17a4c58df43f9')
        THEN 'REDACTED'
        ELSE spi_82d6b50b0bb240f694b17a4c58df43f9::text
    END as spi_82d6b50b0bb240f694b17a4c58df43f9,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_892337bdd7fd46a7bf7208e87c9f412c')
        THEN 'REDACTED'
        ELSE spi_892337bdd7fd46a7bf7208e87c9f412c::text
    END as spi_892337bdd7fd46a7bf7208e87c9f412c,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_e4a1be29fe8b442391bc6b4f5d730e6b')
        THEN 'REDACTED'
        ELSE spi_e4a1be29fe8b442391bc6b4f5d730e6b::text
    END as spi_e4a1be29fe8b442391bc6b4f5d730e6b,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_9f46e0ff05324105be360cfa9d0770f2')
        THEN 'REDACTED'
        ELSE spi_9f46e0ff05324105be360cfa9d0770f2::text
    END as spi_9f46e0ff05324105be360cfa9d0770f2,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_86da79f044cb417dab20c7ecc6f8f7c8')
        THEN 'REDACTED'
        ELSE spi_86da79f044cb417dab20c7ecc6f8f7c8::text
    END as spi_86da79f044cb417dab20c7ecc6f8f7c8,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_ce0fdd416b0941049998a04c5dd9a5b7')
        THEN 'REDACTED'
        ELSE spi_ce0fdd416b0941049998a04c5dd9a5b7::text
    END as spi_ce0fdd416b0941049998a04c5dd9a5b7,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_3314932c64e44192b583eb31e87da1d0')
        THEN 'REDACTED'
        ELSE spi_3314932c64e44192b583eb31e87da1d0::text
    END as spi_3314932c64e44192b583eb31e87da1d0,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_5e2ab0f5f5d94502befeaba770f0e206')
        THEN 'REDACTED'
        ELSE spi_5e2ab0f5f5d94502befeaba770f0e206::text
    END as spi_5e2ab0f5f5d94502befeaba770f0e206,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_796f544054f84d69b961a5b7e6a63240')
        THEN 'REDACTED'
        ELSE spi_796f544054f84d69b961a5b7e6a63240::text
    END as spi_796f544054f84d69b961a5b7e6a63240,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_9f531e6126cf4223865339c24d6dd478')
        THEN 'REDACTED'
        ELSE spi_9f531e6126cf4223865339c24d6dd478::text
    END as spi_9f531e6126cf4223865339c24d6dd478,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_are_you_willing_to_share_your_plan_with_this_perso_141')
        THEN 'REDACTED'
        ELSE spi_are_you_willing_to_share_your_plan_with_this_perso_141::text
    END as spi_are_you_willing_to_share_your_plan_with_this_perso_141,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_4d334b16ea16429994ed32e84ef97021')
        THEN 'REDACTED'
        ELSE spi_4d334b16ea16429994ed32e84ef97021::text
    END as spi_4d334b16ea16429994ed32e84ef97021,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_895fad45c3b547c59edf5f9b9e793fbf')
        THEN 'REDACTED'
        ELSE spi_895fad45c3b547c59edf5f9b9e793fbf::text
    END as spi_895fad45c3b547c59edf5f9b9e793fbf,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_92da95cc50dd4bf0acca4465c07d44cc')
        THEN 'REDACTED'
        ELSE spi_92da95cc50dd4bf0acca4465c07d44cc::text
    END as spi_92da95cc50dd4bf0acca4465c07d44cc,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_person_5_145')
        THEN 'REDACTED'
        ELSE spi_person_5_145::text
    END as spi_person_5_145,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_f1fef9d72ab94ead9c24c4dc9c7ed4cc')
        THEN 'REDACTED'
        ELSE spi_f1fef9d72ab94ead9c24c4dc9c7ed4cc::text
    END as spi_f1fef9d72ab94ead9c24c4dc9c7ed4cc,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_40d6a87f94984cfdb2334610e6de08c2')
        THEN 'REDACTED'
        ELSE spi_40d6a87f94984cfdb2334610e6de08c2::text
    END as spi_40d6a87f94984cfdb2334610e6de08c2,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_ac3206ddb60b4cb1a5593a770a685668')
        THEN 'REDACTED'
        ELSE spi_ac3206ddb60b4cb1a5593a770a685668::text
    END as spi_ac3206ddb60b4cb1a5593a770a685668,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_f895ed4f50044da49d59c21293674b21')
        THEN 'REDACTED'
        ELSE spi_f895ed4f50044da49d59c21293674b21::text
    END as spi_f895ed4f50044da49d59c21293674b21,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_faefef23d2e64777b6288f83921e8fd8')
        THEN 'REDACTED'
        ELSE spi_faefef23d2e64777b6288f83921e8fd8::text
    END as spi_faefef23d2e64777b6288f83921e8fd8,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_62a0463889284aabb4901a33739b7a5c')
        THEN 'REDACTED'
        ELSE spi_62a0463889284aabb4901a33739b7a5c::text
    END as spi_62a0463889284aabb4901a33739b7a5c,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_bfe595d6325f492ab3b6cc5541e18c3f')
        THEN 'REDACTED'
        ELSE spi_bfe595d6325f492ab3b6cc5541e18c3f::text
    END as spi_bfe595d6325f492ab3b6cc5541e18c3f,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_40d8ef6402f042ae947877651bd26dfa')
        THEN 'REDACTED'
        ELSE spi_40d8ef6402f042ae947877651bd26dfa::text
    END as spi_40d8ef6402f042ae947877651bd26dfa,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_249f5ce8ecc04e4396756b01ca1502a3')
        THEN 'REDACTED'
        ELSE spi_249f5ce8ecc04e4396756b01ca1502a3::text
    END as spi_249f5ce8ecc04e4396756b01ca1502a3,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_9b1b11e8608f4ea9911b88aae467ac27')
        THEN 'REDACTED'
        ELSE spi_9b1b11e8608f4ea9911b88aae467ac27::text
    END as spi_9b1b11e8608f4ea9911b88aae467ac27,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_dae7f411758d48009842fc0038a6b4a4')
        THEN 'REDACTED'
        ELSE spi_dae7f411758d48009842fc0038a6b4a4::text
    END as spi_dae7f411758d48009842fc0038a6b4a4,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_0c308fef8ce54217a8d48e6ff00c8e14')
        THEN 'REDACTED'
        ELSE spi_0c308fef8ce54217a8d48e6ff00c8e14::text
    END as spi_0c308fef8ce54217a8d48e6ff00c8e14,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_54b75d551c434552acbfd9754831add8')
        THEN 'REDACTED'
        ELSE spi_54b75d551c434552acbfd9754831add8::text
    END as spi_54b75d551c434552acbfd9754831add8,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_9faa2b7818424326bf7f9975bf04dc2c')
        THEN 'REDACTED'
        ELSE spi_9faa2b7818424326bf7f9975bf04dc2c::text
    END as spi_9faa2b7818424326bf7f9975bf04dc2c,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_a320b7874cba457f9c6067ff76c7020f')
        THEN 'REDACTED'
        ELSE spi_a320b7874cba457f9c6067ff76c7020f::text
    END as spi_a320b7874cba457f9c6067ff76c7020f,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_706fc36fefef4bf9827e7dc100cc92bc')
        THEN 'REDACTED'
        ELSE spi_706fc36fefef4bf9827e7dc100cc92bc::text
    END as spi_706fc36fefef4bf9827e7dc100cc92bc,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_a57b5b3412e54ec882157f316267323a')
        THEN 'REDACTED'
        ELSE spi_a57b5b3412e54ec882157f316267323a::text
    END as spi_a57b5b3412e54ec882157f316267323a,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_6bbdb2cfe7f6443a8f4e841e16bb9b75')
        THEN 'REDACTED'
        ELSE spi_6bbdb2cfe7f6443a8f4e841e16bb9b75::text
    END as spi_6bbdb2cfe7f6443a8f4e841e16bb9b75,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_d617b1e1ea224863a878627f6b81963e')
        THEN 'REDACTED'
        ELSE spi_d617b1e1ea224863a878627f6b81963e::text
    END as spi_d617b1e1ea224863a878627f6b81963e,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_2cedd4dd15e549518674ea4d4eeb7cfc')
        THEN 'REDACTED'
        ELSE spi_2cedd4dd15e549518674ea4d4eeb7cfc::text
    END as spi_2cedd4dd15e549518674ea4d4eeb7cfc,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_c0655b118f2147ed8ede95498c2e8200')
        THEN 'REDACTED'
        ELSE spi_c0655b118f2147ed8ede95498c2e8200::text
    END as spi_c0655b118f2147ed8ede95498c2e8200,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_1895c0a7b0904f499b0c08dca9a7b225')
        THEN 'REDACTED'
        ELSE spi_1895c0a7b0904f499b0c08dca9a7b225::text
    END as spi_1895c0a7b0904f499b0c08dca9a7b225,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_f311f3ca5537413488285e537bb26a9c')
        THEN 'REDACTED'
        ELSE spi_f311f3ca5537413488285e537bb26a9c::text
    END as spi_f311f3ca5537413488285e537bb26a9c,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_1a00c7b7e80d4e908ed4efd8f6d96268')
        THEN 'REDACTED'
        ELSE spi_1a00c7b7e80d4e908ed4efd8f6d96268::text
    END as spi_1a00c7b7e80d4e908ed4efd8f6d96268,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_527c0166cc354cc3a9d97eeb0a54635e')
        THEN 'REDACTED'
        ELSE spi_527c0166cc354cc3a9d97eeb0a54635e::text
    END as spi_527c0166cc354cc3a9d97eeb0a54635e,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_41cce38aca5c4e24904a25b1027336ed')
        THEN 'REDACTED'
        ELSE spi_41cce38aca5c4e24904a25b1027336ed::text
    END as spi_41cce38aca5c4e24904a25b1027336ed,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_e8726638cfdb40aeb72cf1d594f64de5')
        THEN 'REDACTED'
        ELSE spi_e8726638cfdb40aeb72cf1d594f64de5::text
    END as spi_e8726638cfdb40aeb72cf1d594f64de5,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_6ac23c5d0035489585f233c2fbba8f3f')
        THEN 'REDACTED'
        ELSE spi_6ac23c5d0035489585f233c2fbba8f3f::text
    END as spi_6ac23c5d0035489585f233c2fbba8f3f,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_e7ba8485515441b8ac1835410a83cd99')
        THEN 'REDACTED'
        ELSE spi_e7ba8485515441b8ac1835410a83cd99::text
    END as spi_e7ba8485515441b8ac1835410a83cd99,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_06f9231a999b445982606fbbcdfc5d81')
        THEN 'REDACTED'
        ELSE spi_06f9231a999b445982606fbbcdfc5d81::text
    END as spi_06f9231a999b445982606fbbcdfc5d81,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_d8615ac796b246a2b3a5b5b83945b444')
        THEN 'REDACTED'
        ELSE spi_d8615ac796b246a2b3a5b5b83945b444::text
    END as spi_d8615ac796b246a2b3a5b5b83945b444,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_74d0f0dfe7264c4084ceefece882818b')
        THEN 'REDACTED'
        ELSE spi_74d0f0dfe7264c4084ceefece882818b::text
    END as spi_74d0f0dfe7264c4084ceefece882818b,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_1d0ad82509de4018a680cd0e6bf0983d')
        THEN 'REDACTED'
        ELSE spi_1d0ad82509de4018a680cd0e6bf0983d::text
    END as spi_1d0ad82509de4018a680cd0e6bf0983d,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_d5194db005464bd196d8d56457930c03')
        THEN 'REDACTED'
        ELSE spi_d5194db005464bd196d8d56457930c03::text
    END as spi_d5194db005464bd196d8d56457930c03,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_9e11ce9790e842f2ba188c4f0720c131')
        THEN 'REDACTED'
        ELSE spi_9e11ce9790e842f2ba188c4f0720c131::text
    END as spi_9e11ce9790e842f2ba188c4f0720c131,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_cb75bba90bd84dceba2fd1b21a97c2a0')
        THEN 'REDACTED'
        ELSE spi_cb75bba90bd84dceba2fd1b21a97c2a0::text
    END as spi_cb75bba90bd84dceba2fd1b21a97c2a0,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_2f44652fdf2d476e933d75b6ed55f5f2')
        THEN 'REDACTED'
        ELSE spi_2f44652fdf2d476e933d75b6ed55f5f2::text
    END as spi_2f44652fdf2d476e933d75b6ed55f5f2,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_7b51625b73b9472d9c71299beb26694f')
        THEN 'REDACTED'
        ELSE spi_7b51625b73b9472d9c71299beb26694f::text
    END as spi_7b51625b73b9472d9c71299beb26694f,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_13e2f93482ed4b23af6e95523dc3746a')
        THEN 'REDACTED'
        ELSE spi_13e2f93482ed4b23af6e95523dc3746a::text
    END as spi_13e2f93482ed4b23af6e95523dc3746a,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_ec54e447c9814dd2aaa4659f8e494248')
        THEN 'REDACTED'
        ELSE spi_ec54e447c9814dd2aaa4659f8e494248::text
    END as spi_ec54e447c9814dd2aaa4659f8e494248,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_cf16ca6e8b1b467db807fc5d50339f71')
        THEN 'REDACTED'
        ELSE spi_cf16ca6e8b1b467db807fc5d50339f71::text
    END as spi_cf16ca6e8b1b467db807fc5d50339f71,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_ce18eee3aa6444238db150fe7f00850d')
        THEN 'REDACTED'
        ELSE spi_ce18eee3aa6444238db150fe7f00850d::text
    END as spi_ce18eee3aa6444238db150fe7f00850d,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_5a79866df1fb4bd8b4c4f18c982bef3c')
        THEN 'REDACTED'
        ELSE spi_5a79866df1fb4bd8b4c4f18c982bef3c::text
    END as spi_5a79866df1fb4bd8b4c4f18c982bef3c,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_45772f3fc610408b942b359769945cf4')
        THEN 'REDACTED'
        ELSE spi_45772f3fc610408b942b359769945cf4::text
    END as spi_45772f3fc610408b942b359769945cf4,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_cebffdf48fd14829884c41cc42bc0111')
        THEN 'REDACTED'
        ELSE spi_cebffdf48fd14829884c41cc42bc0111::text
    END as spi_cebffdf48fd14829884c41cc42bc0111,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_7ca03155c9ec45e18cd05b86fa444ad0')
        THEN 'REDACTED'
        ELSE spi_7ca03155c9ec45e18cd05b86fa444ad0::text
    END as spi_7ca03155c9ec45e18cd05b86fa444ad0,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_46c2861b70a94fdf94eae1a01cae3149')
        THEN 'REDACTED'
        ELSE spi_46c2861b70a94fdf94eae1a01cae3149::text
    END as spi_46c2861b70a94fdf94eae1a01cae3149,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_8a5b1c7de3ef444385f1fa5213d89f8d')
        THEN 'REDACTED'
        ELSE spi_8a5b1c7de3ef444385f1fa5213d89f8d::text
    END as spi_8a5b1c7de3ef444385f1fa5213d89f8d,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'date_of_birth')
        THEN 'REDACTED'
        ELSE date_of_birth::text
    END as date_of_birth,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'age')
        THEN 'REDACTED'
        ELSE age::text
    END as age,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'birth_month')
        THEN 'REDACTED'
        ELSE birth_month::text
    END as birth_month,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'age_years')
        THEN 'REDACTED'
        ELSE age_years::text
    END as age_years,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'paracetamol')
        THEN 'REDACTED'
        ELSE paracetamol::text
    END as paracetamol,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'tiredness')
        THEN 'REDACTED'
        ELSE tiredness::text
    END as tiredness,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'watching_tv')
        THEN 'REDACTED'
        ELSE watching_tv::text
    END as watching_tv,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'wait_what')
        THEN 'REDACTED'
        ELSE wait_what::text
    END as wait_what,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'toxins_i_will_protect_myself_from_this_risk_by')
        THEN 'REDACTED'
        ELSE toxins_i_will_protect_myself_from_this_risk_by::text
    END as toxins_i_will_protect_myself_from_this_risk_by,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sleeping')
        THEN 'REDACTED'
        ELSE sleeping::text
    END as sleeping,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'what')
        THEN 'REDACTED'
        ELSE what::text
    END as what,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'what_205')
        THEN 'REDACTED'
        ELSE what_205::text
    END as what_205,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'wait')
        THEN 'REDACTED'
        ELSE wait::text
    END as wait,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'wait_what_207')
        THEN 'REDACTED'
        ELSE wait_what_207::text
    END as wait_what_207,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'tiredness_208')
        THEN 'REDACTED'
        ELSE tiredness_208::text
    END as tiredness_208,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'toxins_i_will_protect_myself_from_this_risk_by_209')
        THEN 'REDACTED'
        ELSE toxins_i_will_protect_myself_from_this_risk_by_209::text
    END as toxins_i_will_protect_myself_from_this_risk_by_209,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sleeping_210')
        THEN 'REDACTED'
        ELSE sleeping_210::text
    END as sleeping_210,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'socialize')
        THEN 'REDACTED'
        ELSE socialize::text
    END as socialize,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'yoga_playing_with_dog')
        THEN 'REDACTED'
        ELSE yoga_playing_with_dog::text
    END as yoga_playing_with_dog,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'watching_tv_213')
        THEN 'REDACTED'
        ELSE watching_tv_213::text
    END as watching_tv_213,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'video_games')
        THEN 'REDACTED'
        ELSE video_games::text
    END as video_games,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'wait_215')
        THEN 'REDACTED'
        ELSE wait_215::text
    END as wait_215,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'walking_in_the_park')
        THEN 'REDACTED'
        ELSE walking_in_the_park::text
    END as walking_in_the_park,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'none')
        THEN 'REDACTED'
        ELSE none::text
    END as none,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = '20250204')
        THEN 'REDACTED'
        ELSE "20250204"::text
    END as "20250204",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'taking_toxins')
        THEN 'REDACTED'
        ELSE taking_toxins::text
    END as taking_toxins,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'tiktok_dance_challenges')
        THEN 'REDACTED'
        ELSE tiktok_dance_challenges::text
    END as tiktok_dance_challenges,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'no-response-step-1')
        THEN 'REDACTED'
        ELSE "no-response-step-1"::text
    END as "no-response-step-1",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'no-response-step-1-review')
        THEN 'REDACTED'
        ELSE "no-response-step-1-review"::text
    END as "no-response-step-1-review",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'no-response-step-2')
        THEN 'REDACTED'
        ELSE "no-response-step-2"::text
    END as "no-response-step-2",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'no-response-step-2-review')
        THEN 'REDACTED'
        ELSE "no-response-step-2-review"::text
    END as "no-response-step-2-review",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'no-response-step-3')
        THEN 'REDACTED'
        ELSE "no-response-step-3"::text
    END as "no-response-step-3",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'no-response-step-3-review')
        THEN 'REDACTED'
        ELSE "no-response-step-3-review"::text
    END as "no-response-step-3-review",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'no-response-step-4')
        THEN 'REDACTED'
        ELSE "no-response-step-4"::text
    END as "no-response-step-4",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'no-response-step-4-review')
        THEN 'REDACTED'
        ELSE "no-response-step-4-review"::text
    END as "no-response-step-4-review",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'no-response-step-6a')
        THEN 'REDACTED'
        ELSE "no-response-step-6a"::text
    END as "no-response-step-6a",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'no-response-step-6a-review')
        THEN 'REDACTED'
        ELSE "no-response-step-6a-review"::text
    END as "no-response-step-6a-review",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'other-signs-step-1')
        THEN 'REDACTED'
        ELSE "other-signs-step-1"::text
    END as "other-signs-step-1",
        CURRENT_TIMESTAMP as anonymized_at

from source_data