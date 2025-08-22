{{ config(materialized='view') }}

select
    rownumber_id,
    primarykey_id,
    episodes_id,
    "system.interchange.cds_control_reference" as system_interchange_cds_control_reference,
    "system.interchange.cds_record_identifier" as system_interchange_cds_record_identifier,
    "system.interchange.received_date" as system_interchange_received_date,
    "system.interchange.received_time" as system_interchange_received_time,
    "system.interchange.cds_message_version_number"
    as system_interchange_cds_message_version_number,
    "system.interchange.cds_message_reference" as system_interchange_cds_message_reference,
    "system.interchange.identifier" as system_interchange_identifier,
    "system.transaction.cds_record_type" as system_transaction_cds_record_type,
    "system.transaction.cds_unique_identifier" as system_transaction_cds_unique_identifier,
    "system.transaction.cds_activity_date" as system_transaction_cds_activity_date,
    "system.transaction.cds_sender" as system_transaction_cds_sender,
    "system.transaction.cds_extract_date" as system_transaction_cds_extract_date,
    "system.transaction.cds_extract_time" as system_transaction_cds_extract_time,
    "system.transaction.cds_protocol_identifier" as system_transaction_cds_protocol_identifier,
    "system.transaction.cds_bulk_replacement_group"
    as system_transaction_cds_bulk_replacement_group,
    "system.transaction.cds_report_period_start_date"
    as system_transaction_cds_report_period_start_date,
    "system.transaction.cds_report_period_end_date"
    as system_transaction_cds_report_period_end_date,
    "system.transaction.cds_prime_recipient" as system_transaction_cds_prime_recipient,
    "system.record.identifier" as system_record_identifier,
    "system.record.version" as system_record_version,
    "start_time" as start_time,
    "end_date" as end_date,
    "end_time" as end_time,
    "duration" as duration,
    "last_episode_indicator" as last_episode_indicator,
    "neonatal_level_of_care" as neonatal_level_of_care,
    "psychiatric_patient_status" as psychiatric_patient_status,
    "dominant_episode_flag" as dominant_episode_flag,
    "care_professional.consultant" as care_professional_consultant,
    "care_professional.main_specialty" as care_professional_main_specialty,
    "care_professional.treatment_function" as care_professional_treatment_function,
    "clinical_coding.grouper_derived.dominant_procedure"
    as clinical_coding_grouper_derived_dominant_procedure,
    "clinical_coding.procedure.operation_status" as clinical_coding_procedure_operation_status,
    "commissioning.service_agreement.provider" as commissioning_service_agreement_provider,
    "commissioning.service_agreement.provider_derived"
    as commissioning_service_agreement_provider_derived,
    "commissioning.service_agreement.commissioner" as commissioning_service_agreement_commissioner,
    "commissioning.service_agreement.is_commissioner_recognised"
    as commissioning_service_agreement_is_commissioner_recognised,
    "commissioning.service_agreement.commissioner_derived"
    as commissioning_service_agreement_commissioner_derived,
    "commissioning.service_agreement.commissioning_serial_number"
    as commissioning_service_agreement_commissioning_serial_number,
    "commissioning.service_agreement.commissioner_reference_number"
    as commissioning_service_agreement_commissioner_reference_number,
    "commissioning.grouping.core_hrg" as commissioning_grouping_core_hrg,
    "commissioning.grouping.grouping_method" as commissioning_grouping_grouping_method,
    "commissioning.grouping.programme_budgeting_category"
    as commissioning_grouping_programme_budgeting_category,
    "system.record.cds_group_indicator" as system_record_cds_group_indicator,
    "system.record.finished_indicator" as system_record_finished_indicator,
    "spell.hospital_provider_spell_number" as spell_hospital_provider_spell_number,
    "spell.open_spell_indicator" as spell_open_spell_indicator,
    "patient.identity.nhs_number.value Pseudo" as patient_identity_nhs_number_value_pseudo,  -- sk_patient_id
    "patient.identity.nhs_number.status" as patient_identity_nhs_number_status,
    "patient.identity.nhs_number.is_valid" as patient_identity_nhs_number_is_valid,
    "patient.identity.local_patient_identifier.value"
    as patient_identity_local_patient_identifier_value,
    "patient.identity.local_patient_identifier.issuer"
    as patient_identity_local_patient_identifier_issuer,
    "patient.identity.birth_date" as patient_identity_birth_date,
    "patient.identity.birth_month" as patient_identity_birth_month,
    "patient.identity.birth_year" as patient_identity_birth_year,
    "patient.identity.age_range" as patient_identity_age_range,
    "patient.identity.age_at_cds_activity_date" as patient_identity_age_at_cds_activity_date,
    "patient.identity.age_on_admission" as patient_identity_age_on_admission,
    "patient.identity.age_at_record_start" as patient_identity_age_at_record_start,
    "patient.identity.fractional_age_at_record_start"
    as patient_identity_fractional_age_at_record_start,
    "patient.identity.hes_age_at_record_start" as patient_identity_hes_age_at_record_start,
    "patient.identity.age_at_record_end" as patient_identity_age_at_record_end,
    "patient.identity.hes_age_at_record_end" as patient_identity_hes_age_at_record_end,
    "patient.identity.gender" as patient_identity_gender,
    "patient.identity.ethnic_category" as patient_identity_ethnic_category,
    "patient.identity.marital_status" as patient_identity_marital_status,
    "commissioning.grouping.code_cleaning_applied" as commissioning_grouping_code_cleaning_applied,
    "commissioning.tariff_calculation.excluded_from_pbr"
    as commissioning_tariff_calculation_excluded_from_pbr,
    "commissioning.tariff_calculation.pbr_length_of_stay.critical_care_days"
    as commissioning_tariff_calculation_pbr_length_of_stay_critical_care_days,
    "commissioning.tariff_calculation.pbr_length_of_stay.rehabilitation_days"
    as commissioning_tariff_calculation_pbr_length_of_stay_rehabilitation_days,
    "commissioning.tariff_calculation.pbr_length_of_stay.palliative_care_days"
    as commissioning_tariff_calculation_pbr_length_of_stay_palliative_care_days,
    "commissioning.tariff_calculation.pbr_length_of_stay.pbr_delayed_discharge_days"
    as commissioning_tariff_calculation_pbr_length_of_stay_pbr_delayed_discharge_days,
    "system.transaction.cds_update_type" as system_transaction_cds_update_type,
    "system.transaction.cds_applicable_date" as system_transaction_cds_applicable_date,
    "system.transaction.cds_applicable_time" as system_transaction_cds_applicable_time,
    "referral.direct_access_indicator" as referral_direct_access_indicator,
    "first_regular_day_or_night_admission" as first_regular_day_or_night_admission,
    "commissioning.service_agreement.line_number" as commissioning_service_agreement_line_number,
    "patient.identity.carer_support_indicator" as patient_identity_carer_support_indicator,
    "referral_to_treatment.ubrn.issuer" as referral_to_treatment_ubrn_issuer,
    "referral_to_treatment.patient_pathway_identifier.value Pseudo"
    as referral_to_treatment_patient_pathway_identifier_value_pseudo,
    "referral_to_treatment.patient_pathway_identifier.issuer"
    as referral_to_treatment_patient_pathway_identifier_issuer,
    "referral_to_treatment.period_status" as referral_to_treatment_period_status,
    "referral_to_treatment.waiting_time_measurement_type"
    as referral_to_treatment_waiting_time_measurement_type,
    "referral_to_treatment.period_start_date" as referral_to_treatment_period_start_date,
    "discharge.to_hospital_at_home" as discharge_to_hospital_at_home,
    "care_professional.local_sub_specialty" as care_professional_local_sub_specialty,
    "commissioning.service_agreement.provider_reference_number"
    as commissioning_service_agreement_provider_reference_number,
    "patient.mother.nhs_number.value Pseudo" as patient_mother_nhs_number_value_pseudo,
    "patient.residence_derived.index_of_multiple_deprivation.decile_description"
    as patient_residence_derived_index_of_multiple_deprivation_decile_description,
    "patient.residence_derived.rural_urban_indicator"
    as patient_residence_derived_rural_urban_indicator,
    "patient.residence_derived.cancer_registry" as patient_residence_derived_cancer_registry,
    "patient.residence_derived.grid_reference" as patient_residence_derived_grid_reference,
    "patient.mental_health_act_legal_status.classification"
    as patient_mental_health_act_legal_status_classification,
    "referral.referrer" as referral_referrer,
    "referral.referring_organisation" as referral_referring_organisation,
    "referral.decided_to_admit_date" as referral_decided_to_admit_date,
    "admission.administrative_category" as admission_administrative_category,
    "admission.intended_management" as admission_intended_management,
    "admission.patient_classification" as admission_patient_classification,
    "admission.method" as admission_method,
    "admission.source" as admission_source,
    "admission.date" as admission_date,
    "admission.time" as admission_time,
    "admission.duration_elective_wait" as admission_duration_elective_wait,
    "admission.admission_type" as admission_admission_type,
    "admission.admission_sub_type" as admission_admission_sub_type,
    "discharge.destination" as discharge_destination,
    "discharge.method" as discharge_method,
    "discharge.date" as discharge_date,
    "discharge.time" as discharge_time,
    "number" as number,
    "start_date" as start_date,
    "patient.mother.nhs_number.status" as patient_mother_nhs_number_status,
    "patient.mother.local_patient_identifier.value"
    as patient_mother_local_patient_identifier_value,
    "patient.mother.local_patient_identifier.issuer"
    as patient_mother_local_patient_identifier_issuer,
    "patient.mother.birth_date" as patient_mother_birth_date,
    "patient.mother.birth_month" as patient_mother_birth_month,
    "patient.mother.birth_year" as patient_mother_birth_year,
    "patient.mother.age_range" as patient_mother_age_range,
    "patient.mother.postcode Pseudo" as patient_mother_postcode_pseudo,
    "patient.mother.residence_ccg" as patient_mother_residence_ccg,
    "patient.mother.postcode_from_pds Pseudo" as patient_mother_postcode_from_pds_pseudo,
    "patient.mother.residence_ccg_from_pds" as patient_mother_residence_ccg_from_pds,
    "patient.mother.general_practice_from_pds" as patient_mother_general_practice_from_pds,
    "patient.mother.responsible_ccg_from_pds" as patient_mother_responsible_ccg_from_pds,
    "pregnancy.number_of_babies" as pregnancy_number_of_babies,
    "pregnancy.first_assessment_date" as pregnancy_first_assessment_date,
    "pregnancy.general_practitioner_antenatal" as pregnancy_general_practitioner_antenatal,
    "pregnancy.general_practice_antenatal" as pregnancy_general_practice_antenatal,
    "labour_delivery.intended_delivery_place.type" as labour_delivery_intended_delivery_place_type,
    "labour_delivery.intended_delivery_place.change_reason"
    as labour_delivery_intended_delivery_place_change_reason,
    "labour_delivery.anaesthetic_during" as labour_delivery_anaesthetic_during,
    "labour_delivery.anaesthetic_post" as labour_delivery_anaesthetic_post,
    "labour_delivery.gestation_length_onset" as labour_delivery_gestation_length_onset,
    "labour_delivery.onset_method" as labour_delivery_onset_method,
    "patient.registration.general_practitioner" as patient_registration_general_practitioner,
    "patient.registration.general_practice" as patient_registration_general_practice,
    "patient.registration.responsible_ccg" as patient_registration_responsible_ccg,
    "patient.registration_derived.general_practice_from_pds"
    as patient_registration_derived_general_practice_from_pds,
    "patient.registration_derived.responsible_ccg_from_pds"
    as patient_registration_derived_responsible_ccg_from_pds,
    "patient.residence.postcode Pseudo" as patient_residence_postcode_pseudo,
    "patient.residence.postcode_is_valid" as patient_residence_postcode_is_valid,
    "patient.residence.residence_ccg" as patient_residence_residence_ccg,
    "patient.residence_derived.postcode_from_pds Pseudo"
    as patient_residence_derived_postcode_from_pds_pseudo,
    "patient.residence_derived.residence_ccg_from_patient_postcode"
    as patient_residence_derived_residence_ccg_from_patient_postcode,
    "patient.residence_derived.postcode_district" as patient_residence_derived_postcode_district,
    "patient.residence_derived.oa_11" as patient_residence_derived_oa_11,
    "patient.residence_derived.msoa_11" as patient_residence_derived_msoa_11,
    "patient.residence_derived.lsoa_11" as patient_residence_derived_lsoa_11,
    "patient.residence_derived.country" as patient_residence_derived_country,
    "patient.residence_derived.government_office_region"
    as patient_residence_derived_government_office_region,
    "patient.residence_derived.residence_county" as patient_residence_derived_residence_county,
    "patient.residence_derived.local_authority_district"
    as patient_residence_derived_local_authority_district,
    "patient.residence_derived.parliamentary_constituency"
    as patient_residence_derived_parliamentary_constituency,
    "patient.residence_derived.ed_county_code" as patient_residence_derived_ed_county_code,
    "patient.residence_derived.ed_district_code" as patient_residence_derived_ed_district_code,
    "patient.residence_derived.electoral_ward_1998"
    as patient_residence_derived_electoral_ward_1998,
    "patient.residence_derived.index_of_multiple_deprivation.decile"
    as patient_residence_derived_index_of_multiple_deprivation_decile,
    "labour_delivery.delivery_date" as labour_delivery_delivery_date,
    "discharge.ready_date" as discharge_ready_date,
    "referral_to_treatment.period_end_date" as referral_to_treatment_period_end_date,
    "referral_to_treatment.period_length" as referral_to_treatment_period_length,
    "commissioning.pss_grouping.national_programme_code"
    as commissioning_pss_grouping_national_programme_code,
    "commissioning.pss_grouping.prescribed_service_line_code"
    as commissioning_pss_grouping_prescribed_service_line_code,
    "care_professional.multiple_professional_or_discipline_indicator"
    as care_professional_multiple_professional_or_discipline_indicator,
    "referral_to_treatment.ubrn.value Pseudo" as referral_to_treatment_ubrn_value_pseudo,
    "referral.earliest_reasonable_offer_date" as referral_earliest_reasonable_offer_date,
    "ambulance.conveying_trust" as ambulance_conveying_trust,
    "care_professional.rehabilitation_assessment_team_type"
    as care_professional_rehabilitation_assessment_team_type,
    "labour_delivery.intended_delivery_place.location_class"
    as labour_delivery_intended_delivery_place_location_class,
    "labour_delivery.intended_delivery_place.location_type"
    as labour_delivery_intended_delivery_place_location_type,
    "pregnancy.total_previous_pregnancies" as pregnancy_total_previous_pregnancies,
    "system.record.confidentiality_category" as system_record_confidentiality_category,
    "patient.identity.withheld_identity_reason" as patient_identity_withheld_identity_reason,
    "patient.mother.overseas_visitor_status" as patient_mother_overseas_visitor_status,
    "ambulance.incident_number" as ambulance_incident_number,
    "patient.mother.withheld_identity_reason" as patient_mother_withheld_identity_reason,
    "dmicImportLogId" as dmicimportlogid,
    "patient.identity.ethnic_category_2021" as patient_identity_ethnic_category_2021,
    "labour_delivery.delivery_timestamp" as labour_delivery_delivery_timestamp,
    "referral.latest_clinically_appropriate_date" as referral_latest_clinically_appropriate_date,
    "patient.mother.overseas_visitor_charging_category_at_cds_activity_date"
    as patient_mother_overseas_visitor_charging_category_at_cds_activity_date,
from {{ source("sus_apc", "spell_episodes") }}
