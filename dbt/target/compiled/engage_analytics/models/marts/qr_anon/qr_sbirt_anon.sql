

-- Anonymized view for qr_sbirt with PII fields masked based on questionnaire_metadata.anon flag
-- Auto-generated to mask fields marked as anon=TRUE in questionnaire_metadata

with metadata_pii as (
    select distinct
        question_alias
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where questionnaire_id in ('Questionnaire/212')
    and anon = 'TRUE'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_sbirt"
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
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_on_average_how_many_days_per_week_do_you_drink_alc')
        THEN 'REDACTED'
        ELSE sbirt_on_average_how_many_days_per_week_do_you_drink_alc::text
    END as sbirt_on_average_how_many_days_per_week_do_you_drink_alc,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_in_the_past_three_months_how_often_have_you_used_t')
        THEN 'REDACTED'
        ELSE sbirt_in_the_past_three_months_how_often_have_you_used_t::text
    END as sbirt_in_the_past_three_months_how_often_have_you_used_t,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_have_you_or_someone_else_been_injured_as_a_result_')
        THEN 'REDACTED'
        ELSE sbirt_have_you_or_someone_else_been_injured_as_a_result_::text
    END as sbirt_have_you_or_someone_else_been_injured_as_a_result_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_how_often_do_you_have_six_or_more_drinks_on_one_oc')
        THEN 'REDACTED'
        ELSE sbirt_how_often_do_you_have_six_or_more_drinks_on_one_oc::text
    END as sbirt_how_often_do_you_have_six_or_more_drinks_on_one_oc,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_how_often_during_the_last_year_have_you_needed_a_f')
        THEN 'REDACTED'
        ELSE sbirt_how_often_during_the_last_year_have_you_needed_a_f::text
    END as sbirt_how_often_during_the_last_year_have_you_needed_a_f,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_have_you_ever_experienced_withdrawal_symptoms_felt')
        THEN 'REDACTED'
        ELSE sbirt_have_you_ever_experienced_withdrawal_symptoms_felt::text
    END as sbirt_have_you_ever_experienced_withdrawal_symptoms_felt,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_have_you_had_blackouts_or_flashbacks_as_a_result_o')
        THEN 'REDACTED'
        ELSE sbirt_have_you_had_blackouts_or_flashbacks_as_a_result_o::text
    END as sbirt_have_you_had_blackouts_or_flashbacks_as_a_result_o,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_in_the_past_three_months_how_often_have_you_used_t_8')
        THEN 'REDACTED'
        ELSE sbirt_in_the_past_three_months_how_often_have_you_used_t_8::text
    END as sbirt_in_the_past_three_months_how_often_have_you_used_t_8,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_how_often_during_the_last_year_have_you_had_a_feel')
        THEN 'REDACTED'
        ELSE sbirt_how_often_during_the_last_year_have_you_had_a_feel::text
    END as sbirt_how_often_during_the_last_year_have_you_had_a_feel,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_how_many_standard_drinks_containing_alcohol_do_you')
        THEN 'REDACTED'
        ELSE sbirt_how_many_standard_drinks_containing_alcohol_do_you::text
    END as sbirt_how_many_standard_drinks_containing_alcohol_do_you,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_how_often_during_the_last_year_have_you_been_unabl')
        THEN 'REDACTED'
        ELSE sbirt_how_often_during_the_last_year_have_you_been_unabl::text
    END as sbirt_how_often_during_the_last_year_have_you_been_unabl,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_how_often_do_you_have_a_drink_containing_alcohol')
        THEN 'REDACTED'
        ELSE sbirt_how_often_do_you_have_a_drink_containing_alcohol::text
    END as sbirt_how_often_do_you_have_a_drink_containing_alcohol,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_have_you_neglected_your_family_because_of_your_use')
        THEN 'REDACTED'
        ELSE sbirt_have_you_neglected_your_family_because_of_your_use::text
    END as sbirt_have_you_neglected_your_family_because_of_your_use,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_do_you_ever_feel_bad_or_guilty_about_your_drug_use')
        THEN 'REDACTED'
        ELSE sbirt_do_you_ever_feel_bad_or_guilty_about_your_drug_use::text
    END as sbirt_do_you_ever_feel_bad_or_guilty_about_your_drug_use,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_in_the_past_three_months_how_often_have_you_used_t_15')
        THEN 'REDACTED'
        ELSE sbirt_in_the_past_three_months_how_often_have_you_used_t_15::text
    END as sbirt_in_the_past_three_months_how_often_have_you_used_t_15,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_have_you_had_medical_problems_as_a_result_of_your_')
        THEN 'REDACTED'
        ELSE sbirt_have_you_had_medical_problems_as_a_result_of_your_::text
    END as sbirt_have_you_had_medical_problems_as_a_result_of_your_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_have_you_ever_used_any_drugs_by_injection_nonmedic')
        THEN 'REDACTED'
        ELSE sbirt_have_you_ever_used_any_drugs_by_injection_nonmedic::text
    END as sbirt_have_you_ever_used_any_drugs_by_injection_nonmedic,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_have_you_engaged_in_illegal_activities_in_order_to')
        THEN 'REDACTED'
        ELSE sbirt_have_you_engaged_in_illegal_activities_in_order_to::text
    END as sbirt_have_you_engaged_in_illegal_activities_in_order_to,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_in_the_past_three_months_how_often_have_you_used_t_19')
        THEN 'REDACTED'
        ELSE sbirt_in_the_past_three_months_how_often_have_you_used_t_19::text
    END as sbirt_in_the_past_three_months_how_often_have_you_used_t_19,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_in_the_past_three_months_how_often_have_you_used_t_20')
        THEN 'REDACTED'
        ELSE sbirt_in_the_past_three_months_how_often_have_you_used_t_20::text
    END as sbirt_in_the_past_three_months_how_often_have_you_used_t_20,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_how_often_during_the_last_year_have_you_failed_to_')
        THEN 'REDACTED'
        ELSE sbirt_how_often_during_the_last_year_have_you_failed_to_::text
    END as sbirt_how_often_during_the_last_year_have_you_failed_to_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_in_the_past_three_months_how_often_have_you_used_t_22')
        THEN 'REDACTED'
        ELSE sbirt_in_the_past_three_months_how_often_have_you_used_t_22::text
    END as sbirt_in_the_past_three_months_how_often_have_you_used_t_22,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_does_your_spouse_or_parents_ever_complain_about_yo')
        THEN 'REDACTED'
        ELSE sbirt_does_your_spouse_or_parents_ever_complain_about_yo::text
    END as sbirt_does_your_spouse_or_parents_ever_complain_about_yo,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_in_the_past_three_months_how_often_have_you_used_t_24')
        THEN 'REDACTED'
        ELSE sbirt_in_the_past_three_months_how_often_have_you_used_t_24::text
    END as sbirt_in_the_past_three_months_how_often_have_you_used_t_24,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_in_the_past_three_months_how_often_have_you_used_t_25')
        THEN 'REDACTED'
        ELSE sbirt_in_the_past_three_months_how_often_have_you_used_t_25::text
    END as sbirt_in_the_past_three_months_how_often_have_you_used_t_25,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_in_the_past_three_months_how_often_have_you_used_t_26')
        THEN 'REDACTED'
        ELSE sbirt_in_the_past_three_months_how_often_have_you_used_t_26::text
    END as sbirt_in_the_past_three_months_how_often_have_you_used_t_26,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_are_you_always_able_to_stop_using_drugs_when_you_w')
        THEN 'REDACTED'
        ELSE sbirt_are_you_always_able_to_stop_using_drugs_when_you_w::text
    END as sbirt_are_you_always_able_to_stop_using_drugs_when_you_w,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_how_often_during_the_last_year_have_you_found_that')
        THEN 'REDACTED'
        ELSE sbirt_how_often_during_the_last_year_have_you_found_that::text
    END as sbirt_how_often_during_the_last_year_have_you_found_that,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_have_you_used_drugs_other_than_those_required_for_')
        THEN 'REDACTED'
        ELSE sbirt_have_you_used_drugs_other_than_those_required_for_::text
    END as sbirt_have_you_used_drugs_other_than_those_required_for_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_has_a_relative_or_friend_or_a_doctor_or_another_he')
        THEN 'REDACTED'
        ELSE sbirt_has_a_relative_or_friend_or_a_doctor_or_another_he::text
    END as sbirt_has_a_relative_or_friend_or_a_doctor_or_another_he,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_do_you_use_more_than_one_drug_at_a_time')
        THEN 'REDACTED'
        ELSE sbirt_do_you_use_more_than_one_drug_at_a_time::text
    END as sbirt_do_you_use_more_than_one_drug_at_a_time,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_please_select_one_substance_you_would_like_to_disc')
        THEN 'REDACTED'
        ELSE sbirt_please_select_one_substance_you_would_like_to_disc::text
    END as sbirt_please_select_one_substance_you_would_like_to_disc,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_on_average_how_many_days_per_week_do_you_use_metha')
        THEN 'REDACTED'
        ELSE sbirt_on_average_how_many_days_per_week_do_you_use_metha::text
    END as sbirt_on_average_how_many_days_per_week_do_you_use_metha,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_i_would_like_to_offer_an_opportunity_to_start_a_co')
        THEN 'REDACTED'
        ELSE sbirt_i_would_like_to_offer_an_opportunity_to_start_a_co::text
    END as sbirt_i_would_like_to_offer_an_opportunity_to_start_a_co,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_we_could_start_by_discussing_the_pros_and_cons_of_')
        THEN 'REDACTED'
        ELSE sbirt_we_could_start_by_discussing_the_pros_and_cons_of_::text
    END as sbirt_we_could_start_by_discussing_the_pros_and_cons_of_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_on_average_how_many_days_per_week_do_you_use_other')
        THEN 'REDACTED'
        ELSE sbirt_on_average_how_many_days_per_week_do_you_use_other::text
    END as sbirt_on_average_how_many_days_per_week_do_you_use_other,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_are_you_interested_in_learning_more_about_what_alc')
        THEN 'REDACTED'
        ELSE sbirt_are_you_interested_in_learning_more_about_what_alc::text
    END as sbirt_are_you_interested_in_learning_more_about_what_alc,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_on_average_how_many_days_per_week_do_you_use_canna')
        THEN 'REDACTED'
        ELSE sbirt_on_average_how_many_days_per_week_do_you_use_canna::text
    END as sbirt_on_average_how_many_days_per_week_do_you_use_canna,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_on_average_how_many_days_per_week_do_you_use_hallu')
        THEN 'REDACTED'
        ELSE sbirt_on_average_how_many_days_per_week_do_you_use_hallu::text
    END as sbirt_on_average_how_many_days_per_week_do_you_use_hallu,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_on_average_how_many_days_per_week_do_you_use_sedat')
        THEN 'REDACTED'
        ELSE sbirt_on_average_how_many_days_per_week_do_you_use_sedat::text
    END as sbirt_on_average_how_many_days_per_week_do_you_use_sedat,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_on_average_how_many_days_per_week_do_you_use_inhal')
        THEN 'REDACTED'
        ELSE sbirt_on_average_how_many_days_per_week_do_you_use_inhal::text
    END as sbirt_on_average_how_many_days_per_week_do_you_use_inhal,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_on_average_how_many_days_per_week_do_you_use_cocai')
        THEN 'REDACTED'
        ELSE sbirt_on_average_how_many_days_per_week_do_you_use_cocai::text
    END as sbirt_on_average_how_many_days_per_week_do_you_use_cocai,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_are_you_interested_in_learning_more_about_what_the')
        THEN 'REDACTED'
        ELSE sbirt_are_you_interested_in_learning_more_about_what_the::text
    END as sbirt_are_you_interested_in_learning_more_about_what_the,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_on_a_scale_of_0_to_10_with_0_being_not_ready_at_al')
        THEN 'REDACTED'
        ELSE sbirt_on_a_scale_of_0_to_10_with_0_being_not_ready_at_al::text
    END as sbirt_on_a_scale_of_0_to_10_with_0_being_not_ready_at_al,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_on_average_how_many_days_per_week_do_you_use_presc')
        THEN 'REDACTED'
        ELSE sbirt_on_average_how_many_days_per_week_do_you_use_presc::text
    END as sbirt_on_average_how_many_days_per_week_do_you_use_presc,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_during_the_past_three_months_how_often_have_you_ha')
        THEN 'REDACTED'
        ELSE sbirt_during_the_past_three_months_how_often_have_you_ha::text
    END as sbirt_during_the_past_three_months_how_often_have_you_ha,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_during_the_past_three_months_how_often_have_you_ha_47')
        THEN 'REDACTED'
        ELSE sbirt_during_the_past_three_months_how_often_have_you_ha_47::text
    END as sbirt_during_the_past_three_months_how_often_have_you_ha_47,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_during_the_past_three_months_how_often_have_you_ha_48')
        THEN 'REDACTED'
        ELSE sbirt_during_the_past_three_months_how_often_have_you_ha_48::text
    END as sbirt_during_the_past_three_months_how_often_have_you_ha_48,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_during_the_past_three_months_how_often_have_you_ha_49')
        THEN 'REDACTED'
        ELSE sbirt_during_the_past_three_months_how_often_have_you_ha_49::text
    END as sbirt_during_the_past_three_months_how_often_have_you_ha_49,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_during_the_past_three_months_how_often_have_you_ha_50')
        THEN 'REDACTED'
        ELSE sbirt_during_the_past_three_months_how_often_have_you_ha_50::text
    END as sbirt_during_the_past_three_months_how_often_have_you_ha_50,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_on_average_how_many_days_per_week_do_you_use_stree')
        THEN 'REDACTED'
        ELSE sbirt_on_average_how_many_days_per_week_do_you_use_stree::text
    END as sbirt_on_average_how_many_days_per_week_do_you_use_stree,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_during_the_past_three_months_how_often_have_you_ha_52')
        THEN 'REDACTED'
        ELSE sbirt_during_the_past_three_months_how_often_have_you_ha_52::text
    END as sbirt_during_the_past_three_months_how_often_have_you_ha_52,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_cannabis_marijuana_pot_grass_hash_etc')
        THEN 'REDACTED'
        ELSE sbirt_cannabis_marijuana_pot_grass_hash_etc::text
    END as sbirt_cannabis_marijuana_pot_grass_hash_etc,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_during_the_past_three_months_how_often_have_you_ha_54')
        THEN 'REDACTED'
        ELSE sbirt_during_the_past_three_months_how_often_have_you_ha_54::text
    END as sbirt_during_the_past_three_months_how_often_have_you_ha_54,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_during_the_past_three_months_how_often_have_you_ha_55')
        THEN 'REDACTED'
        ELSE sbirt_during_the_past_three_months_how_often_have_you_ha_55::text
    END as sbirt_during_the_past_three_months_how_often_have_you_ha_55,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_would_you_be_interested_in_learning_more_about_sub')
        THEN 'REDACTED'
        ELSE sbirt_would_you_be_interested_in_learning_more_about_sub::text
    END as sbirt_would_you_be_interested_in_learning_more_about_sub,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_1579996975654c3eb488d6a65d66c6c9')
        THEN 'REDACTED'
        ELSE sbirt_1579996975654c3eb488d6a65d66c6c9::text
    END as sbirt_1579996975654c3eb488d6a65d66c6c9,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_things_you_can_do')
        THEN 'REDACTED'
        ELSE sbirt_things_you_can_do::text
    END as sbirt_things_you_can_do,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_in_the_past_three_months_has_your_use_of_prescript')
        THEN 'REDACTED'
        ELSE sbirt_in_the_past_three_months_has_your_use_of_prescript::text
    END as sbirt_in_the_past_three_months_has_your_use_of_prescript,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_in_the_past_three_months_has_your_use_of_methamphe')
        THEN 'REDACTED'
        ELSE sbirt_in_the_past_three_months_has_your_use_of_methamphe::text
    END as sbirt_in_the_past_three_months_has_your_use_of_methamphe,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_in_the_past_three_months_has_your_use_of_other_dru')
        THEN 'REDACTED'
        ELSE sbirt_in_the_past_three_months_has_your_use_of_other_dru::text
    END as sbirt_in_the_past_three_months_has_your_use_of_other_dru,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_in_the_past_three_months_has_your_use_of_inhalants')
        THEN 'REDACTED'
        ELSE sbirt_in_the_past_three_months_has_your_use_of_inhalants::text
    END as sbirt_in_the_past_three_months_has_your_use_of_inhalants,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_in_the_past_three_months_has_your_use_of_cannabis_')
        THEN 'REDACTED'
        ELSE sbirt_in_the_past_three_months_has_your_use_of_cannabis_::text
    END as sbirt_in_the_past_three_months_has_your_use_of_cannabis_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_during_the_past_three_months_how_often_have_you_ha_64')
        THEN 'REDACTED'
        ELSE sbirt_during_the_past_three_months_how_often_have_you_ha_64::text
    END as sbirt_during_the_past_three_months_how_often_have_you_ha_64,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_what_are_some_good_things_about_staying_the_same')
        THEN 'REDACTED'
        ELSE sbirt_what_are_some_good_things_about_staying_the_same::text
    END as sbirt_what_are_some_good_things_about_staying_the_same,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_street_opioids_heroin_opium_etc_or_prescription_op')
        THEN 'REDACTED'
        ELSE sbirt_street_opioids_heroin_opium_etc_or_prescription_op::text
    END as sbirt_street_opioids_heroin_opium_etc_or_prescription_op,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_in_the_past_three_months_has_your_use_of_hallucino')
        THEN 'REDACTED'
        ELSE sbirt_in_the_past_three_months_has_your_use_of_hallucino::text
    END as sbirt_in_the_past_three_months_has_your_use_of_hallucino,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_in_the_past_three_months_has_your_use_of_cocaine_l')
        THEN 'REDACTED'
        ELSE sbirt_in_the_past_three_months_has_your_use_of_cocaine_l::text
    END as sbirt_in_the_past_three_months_has_your_use_of_cocaine_l,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_in_the_past_three_months_has_your_use_of_sedatives')
        THEN 'REDACTED'
        ELSE sbirt_in_the_past_three_months_has_your_use_of_sedatives::text
    END as sbirt_in_the_past_three_months_has_your_use_of_sedatives,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_what_are_some_good_things_about_making_a_change')
        THEN 'REDACTED'
        ELSE sbirt_what_are_some_good_things_about_making_a_change::text
    END as sbirt_what_are_some_good_things_about_making_a_change,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_what_are_your_health_goals_around_substance_use')
        THEN 'REDACTED'
        ELSE sbirt_what_are_your_health_goals_around_substance_use::text
    END as sbirt_what_are_your_health_goals_around_substance_use,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_cocaine_coke_crack_etc')
        THEN 'REDACTED'
        ELSE sbirt_cocaine_coke_crack_etc::text
    END as sbirt_cocaine_coke_crack_etc,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_in_the_past_three_months_how_often_have_you_failed')
        THEN 'REDACTED'
        ELSE sbirt_in_the_past_three_months_how_often_have_you_failed::text
    END as sbirt_in_the_past_three_months_how_often_have_you_failed,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_in_the_past_three_months_how_often_have_you_failed_74')
        THEN 'REDACTED'
        ELSE sbirt_in_the_past_three_months_how_often_have_you_failed_74::text
    END as sbirt_in_the_past_three_months_how_often_have_you_failed_74,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_in_the_past_three_months_how_often_have_you_failed_75')
        THEN 'REDACTED'
        ELSE sbirt_in_the_past_three_months_how_often_have_you_failed_75::text
    END as sbirt_in_the_past_three_months_how_often_have_you_failed_75,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_in_the_past_three_months_how_often_have_you_failed_76')
        THEN 'REDACTED'
        ELSE sbirt_in_the_past_three_months_how_often_have_you_failed_76::text
    END as sbirt_in_the_past_three_months_how_often_have_you_failed_76,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_in_the_past_three_months_has_your_use_of_street_op')
        THEN 'REDACTED'
        ELSE sbirt_in_the_past_three_months_has_your_use_of_street_op::text
    END as sbirt_in_the_past_three_months_has_your_use_of_street_op,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_in_the_past_three_months_how_often_have_you_failed_78')
        THEN 'REDACTED'
        ELSE sbirt_in_the_past_three_months_how_often_have_you_failed_78::text
    END as sbirt_in_the_past_three_months_how_often_have_you_failed_78,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_other_drugs')
        THEN 'REDACTED'
        ELSE sbirt_other_drugs::text
    END as sbirt_other_drugs,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_in_the_past_three_months_how_often_have_you_failed_80')
        THEN 'REDACTED'
        ELSE sbirt_in_the_past_three_months_how_often_have_you_failed_80::text
    END as sbirt_in_the_past_three_months_how_often_have_you_failed_80,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_in_the_past_three_months_how_often_have_you_failed_81')
        THEN 'REDACTED'
        ELSE sbirt_in_the_past_three_months_how_often_have_you_failed_81::text
    END as sbirt_in_the_past_three_months_how_often_have_you_failed_81,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_in_the_past_three_months_how_often_have_you_failed_82')
        THEN 'REDACTED'
        ELSE sbirt_in_the_past_three_months_how_often_have_you_failed_82::text
    END as sbirt_in_the_past_three_months_how_often_have_you_failed_82,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_edced3286f82458abce435987bff19fb')
        THEN 'REDACTED'
        ELSE sbirt_edced3286f82458abce435987bff19fb::text
    END as sbirt_edced3286f82458abce435987bff19fb,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_can_i_share_additional_treatment_options_with_you')
        THEN 'REDACTED'
        ELSE sbirt_can_i_share_additional_treatment_options_with_you::text
    END as sbirt_can_i_share_additional_treatment_options_with_you,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expre')
        THEN 'REDACTED'
        ELSE sbirt_has_a_friend_or_relative_or_anyone_else_ever_expre::text
    END as sbirt_has_a_friend_or_relative_or_anyone_else_ever_expre,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_prescription_stimulants_just_for_the_feeling_more_')
        THEN 'REDACTED'
        ELSE sbirt_prescription_stimulants_just_for_the_feeling_more_::text
    END as sbirt_prescription_stimulants_just_for_the_feeling_more_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expre_87')
        THEN 'REDACTED'
        ELSE sbirt_has_a_friend_or_relative_or_anyone_else_ever_expre_87::text
    END as sbirt_has_a_friend_or_relative_or_anyone_else_ever_expre_87,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expre_88')
        THEN 'REDACTED'
        ELSE sbirt_has_a_friend_or_relative_or_anyone_else_ever_expre_88::text
    END as sbirt_has_a_friend_or_relative_or_anyone_else_ever_expre_88,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expre_89')
        THEN 'REDACTED'
        ELSE sbirt_has_a_friend_or_relative_or_anyone_else_ever_expre_89::text
    END as sbirt_has_a_friend_or_relative_or_anyone_else_ever_expre_89,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expre_90')
        THEN 'REDACTED'
        ELSE sbirt_has_a_friend_or_relative_or_anyone_else_ever_expre_90::text
    END as sbirt_has_a_friend_or_relative_or_anyone_else_ever_expre_90,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expre_91')
        THEN 'REDACTED'
        ELSE sbirt_has_a_friend_or_relative_or_anyone_else_ever_expre_91::text
    END as sbirt_has_a_friend_or_relative_or_anyone_else_ever_expre_91,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expre_92')
        THEN 'REDACTED'
        ELSE sbirt_has_a_friend_or_relative_or_anyone_else_ever_expre_92::text
    END as sbirt_has_a_friend_or_relative_or_anyone_else_ever_expre_92,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expre_93')
        THEN 'REDACTED'
        ELSE sbirt_has_a_friend_or_relative_or_anyone_else_ever_expre_93::text
    END as sbirt_has_a_friend_or_relative_or_anyone_else_ever_expre_93,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_in_the_past_three_months_how_often_have_you_failed_94')
        THEN 'REDACTED'
        ELSE sbirt_in_the_past_three_months_how_often_have_you_failed_94::text
    END as sbirt_in_the_past_three_months_how_often_have_you_failed_94,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down')
        THEN 'REDACTED'
        ELSE sbirt_have_you_ever_tried_and_failed_to_control_cut_down::text
    END as sbirt_have_you_ever_tried_and_failed_to_control_cut_down,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down_96')
        THEN 'REDACTED'
        ELSE sbirt_have_you_ever_tried_and_failed_to_control_cut_down_96::text
    END as sbirt_have_you_ever_tried_and_failed_to_control_cut_down_96,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down_97')
        THEN 'REDACTED'
        ELSE sbirt_have_you_ever_tried_and_failed_to_control_cut_down_97::text
    END as sbirt_have_you_ever_tried_and_failed_to_control_cut_down_97,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expre_98')
        THEN 'REDACTED'
        ELSE sbirt_has_a_friend_or_relative_or_anyone_else_ever_expre_98::text
    END as sbirt_has_a_friend_or_relative_or_anyone_else_ever_expre_98,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down_99')
        THEN 'REDACTED'
        ELSE sbirt_have_you_ever_tried_and_failed_to_control_cut_down_99::text
    END as sbirt_have_you_ever_tried_and_failed_to_control_cut_down_99,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down_100')
        THEN 'REDACTED'
        ELSE sbirt_have_you_ever_tried_and_failed_to_control_cut_down_100::text
    END as sbirt_have_you_ever_tried_and_failed_to_control_cut_down_100,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_things_others_can_help_you_do')
        THEN 'REDACTED'
        ELSE sbirt_things_others_can_help_you_do::text
    END as sbirt_things_others_can_help_you_do,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down_102')
        THEN 'REDACTED'
        ELSE sbirt_have_you_ever_tried_and_failed_to_control_cut_down_102::text
    END as sbirt_have_you_ever_tried_and_failed_to_control_cut_down_102,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_methamphetamine_meth_crystal_speed_ecstasy_molly_e')
        THEN 'REDACTED'
        ELSE sbirt_methamphetamine_meth_crystal_speed_ecstasy_molly_e::text
    END as sbirt_methamphetamine_meth_crystal_speed_ecstasy_molly_e,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down_104')
        THEN 'REDACTED'
        ELSE sbirt_have_you_ever_tried_and_failed_to_control_cut_down_104::text
    END as sbirt_have_you_ever_tried_and_failed_to_control_cut_down_104,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down_105')
        THEN 'REDACTED'
        ELSE sbirt_have_you_ever_tried_and_failed_to_control_cut_down_105::text
    END as sbirt_have_you_ever_tried_and_failed_to_control_cut_down_105,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_inhalants_nitrous_glue_paint_thinner_poppers_whipp')
        THEN 'REDACTED'
        ELSE sbirt_inhalants_nitrous_glue_paint_thinner_poppers_whipp::text
    END as sbirt_inhalants_nitrous_glue_paint_thinner_poppers_whipp,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down_107')
        THEN 'REDACTED'
        ELSE sbirt_have_you_ever_tried_and_failed_to_control_cut_down_107::text
    END as sbirt_have_you_ever_tried_and_failed_to_control_cut_down_107,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_sedatives_just_for_the_feeling_more_than_prescribe')
        THEN 'REDACTED'
        ELSE sbirt_sedatives_just_for_the_feeling_more_than_prescribe::text
    END as sbirt_sedatives_just_for_the_feeling_more_than_prescribe,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_hallucinogens_lsd_acid_mushrooms_pcp_special_k_ecs')
        THEN 'REDACTED'
        ELSE sbirt_hallucinogens_lsd_acid_mushrooms_pcp_special_k_ecs::text
    END as sbirt_hallucinogens_lsd_acid_mushrooms_pcp_special_k_ecs,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_any_other_drugs_to_get_high')
        THEN 'REDACTED'
        ELSE sbirt_any_other_drugs_to_get_high::text
    END as sbirt_any_other_drugs_to_get_high,
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
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = '2_times_in_a_day')
        THEN 'REDACTED'
        ELSE "2_times_in_a_day"::text
    END as "2_times_in_a_day",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'join_a_community_group_and')
        THEN 'REDACTED'
        ELSE join_a_community_group_and::text
    END as join_a_community_group_and,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'therapy')
        THEN 'REDACTED'
        ELSE therapy::text
    END as therapy,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = '2_days_per_week')
        THEN 'REDACTED'
        ELSE "2_days_per_week"::text
    END as "2_days_per_week",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'field_445adcc0')
        THEN 'REDACTED'
        ELSE field_445adcc0::text
    END as field_445adcc0,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = '3_days_per_week')
        THEN 'REDACTED'
        ELSE "3_days_per_week"::text
    END as "3_days_per_week",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'not_ready')
        THEN 'REDACTED'
        ELSE not_ready::text
    END as not_ready,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'quit')
        THEN 'REDACTED'
        ELSE quit::text
    END as quit,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = '20250224')
        THEN 'REDACTED'
        ELSE "20250224"::text
    END as "20250224",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = '2_times_in_a_day_124')
        THEN 'REDACTED'
        ELSE "2_times_in_a_day_124"::text
    END as "2_times_in_a_day_124",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'field_859d58d6')
        THEN 'REDACTED'
        ELSE field_859d58d6::text
    END as field_859d58d6,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'field_8a6c4c5b')
        THEN 'REDACTED'
        ELSE field_8a6c4c5b::text
    END as field_8a6c4c5b,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'social_places_you_can_go_to_eg_take_a_walk_at_the_park')
        THEN 'REDACTED'
        ELSE social_places_you_can_go_to_eg_take_a_walk_at_the_park::text
    END as social_places_you_can_go_to_eg_take_a_walk_at_the_park,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'internally')
        THEN 'REDACTED'
        ELSE internally::text
    END as internally,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'work_toward_a_goal')
        THEN 'REDACTED'
        ELSE work_toward_a_goal::text
    END as work_toward_a_goal,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'drug-to-use-alcohol')
        THEN 'REDACTED'
        ELSE "drug-to-use-alcohol"::text
    END as "drug-to-use-alcohol",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'drug-to-use-marijuana-or-other')
        THEN 'REDACTED'
        ELSE "drug-to-use-marijuana-or-other"::text
    END as "drug-to-use-marijuana-or-other",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'health-goals-do-not-want-to-change')
        THEN 'REDACTED'
        ELSE "health-goals-do-not-want-to-change"::text
    END as "health-goals-do-not-want-to-change",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'is-drug-dependence')
        THEN 'REDACTED'
        ELSE "is-drug-dependence"::text
    END as "is-drug-dependence",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'is-high-risk-alcohol')
        THEN 'REDACTED'
        ELSE "is-high-risk-alcohol"::text
    END as "is-high-risk-alcohol",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'is-high-risk-drug')
        THEN 'REDACTED'
        ELSE "is-high-risk-drug"::text
    END as "is-high-risk-drug",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'is-low-risk-alcohol')
        THEN 'REDACTED'
        ELSE "is-low-risk-alcohol"::text
    END as "is-low-risk-alcohol",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'is-low-risk-drug')
        THEN 'REDACTED'
        ELSE "is-low-risk-drug"::text
    END as "is-low-risk-drug",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'is-medium-risk-alcohol')
        THEN 'REDACTED'
        ELSE "is-medium-risk-alcohol"::text
    END as "is-medium-risk-alcohol",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'is-medium-risk-drug')
        THEN 'REDACTED'
        ELSE "is-medium-risk-drug"::text
    END as "is-medium-risk-drug",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'is-opioid-dependence')
        THEN 'REDACTED'
        ELSE "is-opioid-dependence"::text
    END as "is-opioid-dependence",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'other-drug-list')
        THEN 'REDACTED'
        ELSE "other-drug-list"::text
    END as "other-drug-list",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'patient-age')
        THEN 'REDACTED'
        ELSE "patient-age"::text
    END as "patient-age",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'patient-dob')
        THEN 'REDACTED'
        ELSE "patient-dob"::text
    END as "patient-dob",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'patient-gender')
        THEN 'REDACTED'
        ELSE "patient-gender"::text
    END as "patient-gender",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'patient-name')
        THEN 'REDACTED'
        ELSE "patient-name"::text
    END as "patient-name",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'score-audit')
        THEN 'REDACTED'
        ELSE "score-audit"::text
    END as "score-audit",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'score-dast')
        THEN 'REDACTED'
        ELSE "score-dast"::text
    END as "score-dast",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'should-converse-about-substance-use')
        THEN 'REDACTED'
        ELSE "should-converse-about-substance-use"::text
    END as "should-converse-about-substance-use",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'should-discuss-health-goals')
        THEN 'REDACTED'
        ELSE "should-discuss-health-goals"::text
    END as "should-discuss-health-goals",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'show-client-use-with-other-drugs')
        THEN 'REDACTED'
        ELSE "show-client-use-with-other-drugs"::text
    END as "show-client-use-with-other-drugs",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'show-discuss-next-steps')
        THEN 'REDACTED'
        ELSE "show-discuss-next-steps"::text
    END as "show-discuss-next-steps",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'show-goals')
        THEN 'REDACTED'
        ELSE "show-goals"::text
    END as "show-goals",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'show-intervention-when-low-risk')
        THEN 'REDACTED'
        ELSE "show-intervention-when-low-risk"::text
    END as "show-intervention-when-low-risk",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'show-intervention-when-medium-high-risk')
        THEN 'REDACTED'
        ELSE "show-intervention-when-medium-high-risk"::text
    END as "show-intervention-when-medium-high-risk",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'show-is-medium-high-risk')
        THEN 'REDACTED'
        ELSE "show-is-medium-high-risk"::text
    END as "show-is-medium-high-risk",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'show-summary-for-low-risk')
        THEN 'REDACTED'
        ELSE "show-summary-for-low-risk"::text
    END as "show-summary-for-low-risk",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'show-things-to-do')
        THEN 'REDACTED'
        ELSE "show-things-to-do"::text
    END as "show-things-to-do",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'task-id-sbirt-pdf')
        THEN 'REDACTED'
        ELSE "task-id-sbirt-pdf"::text
    END as "task-id-sbirt-pdf",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'using-opioids-and-low-risk')
        THEN 'REDACTED'
        ELSE "using-opioids-and-low-risk"::text
    END as "using-opioids-and-low-risk",
        CURRENT_TIMESTAMP as anonymized_at

from source_data