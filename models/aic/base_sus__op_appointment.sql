{{ config(materialized='view') }}

select
    rownumber_id,
    primarykey_id,
    "type" as type,
    "action" as action,
    "schema_version" as schema_version,
    "dmicImportLogId" as dmic_import_log_id,
    "appointment.patient.registration.general_practice"
    as appointment_patient_registration_general_practice,
    "appointment.patient.registration.responsible_ccg"
    as appointment_patient_registration_responsible_ccg,
    "appointment.patient.registration_derived.general_practice_from_pds"
    as appointment_patient_registration_derived_general_practice_from_pds,
    "appointment.patient.registration_derived.responsible_ccg_from_pds"
    as appointment_patient_registration_derived_responsible_ccg_from_pds,
    "appointment.patient.residence.postcode Pseudo"
    as appointment_patient_residence_postcode_pseudo,
    "appointment.patient.residence.postcode_is_valid"
    as appointment_patient_residence_postcode_is_valid,
    "appointment.patient.residence.residence_ccg" as appointment_patient_residence_residence_ccg,
    "appointment.patient.residence_derived.postcode_from_pds Pseudo"
    as appointment_patient_residence_derived_postcode_from_pds_pseudo,
    "appointment.patient.residence_derived.residence_ccg_from_patient_postcode"
    as appointment_patient_residence_derived_residence_ccg_from_patient_postcode,
    "appointment.patient.residence_derived.postcode_district"
    as appointment_patient_residence_derived_postcode_district,
    "appointment.patient.residence_derived.oa_11" as appointment_patient_residence_derived_oa_11,
    "appointment.patient.residence_derived.msoa_11"
    as appointment_patient_residence_derived_msoa_11,
    "appointment.patient.residence_derived.lsoa_11"
    as appointment_patient_residence_derived_lsoa_11,
    "appointment.patient.residence_derived.country"
    as appointment_patient_residence_derived_country,
    "appointment.patient.residence_derived.government_office_region"
    as appointment_patient_residence_derived_government_office_region,
    "appointment.patient.residence_derived.residence_county"
    as appointment_patient_residence_derived_residence_county,
    "appointment.patient.residence_derived.local_authority_district"
    as appointment_patient_residence_derived_local_authority_district,
    "appointment.patient.residence_derived.parliamentary_constituency"
    as appointment_patient_residence_derived_parliamentary_constituency,
    "appointment.patient.residence_derived.ed_county_code"
    as appointment_patient_residence_derived_ed_county_code,
    "appointment.patient.residence_derived.ed_district_code"
    as appointment_patient_residence_derived_ed_district_code,
    "appointment.patient.residence_derived.electoral_ward_1998"
    as appointment_patient_residence_derived_electoral_ward_1998,
    "appointment.patient.residence_derived.index_of_multiple_deprivation.decile"
    as appointment_patient_residence_derived_index_of_multiple_deprivation_decile,
    "appointment.patient.residence_derived.index_of_multiple_deprivation.decile_description"
    as appointment_patient_residence_derived_index_of_multiple_deprivation_decile_description,
    "appointment.patient.residence_derived.rural_urban_indicator"
    as appointment_patient_residence_derived_rural_urban_indicator,
    "appointment.patient.residence_derived.cancer_registry"
    as appointment_patient_residence_derived_cancer_registry,
    "appointment.patient.residence_derived.grid_reference"
    as appointment_patient_residence_derived_grid_reference,
    "appointment.patient.registration.general_practitioner"
    as appointment_patient_registration_general_practitioner,
    "appointment.patient.identity.nhs_number.value Pseudo"
    as appointment_patient_identity_nhs_number_value_pseudo,
    "appointment.patient.identity.nhs_number.status"
    as appointment_patient_identity_nhs_number_status,
    "appointment.patient.identity.nhs_number.is_valid"
    as appointment_patient_identity_nhs_number_is_valid,
    "appointment.patient.identity.local_patient_identifier.value"
    as appointment_patient_identity_local_patient_identifier_value,
    "appointment.patient.identity.local_patient_identifier.issuer"
    as appointment_patient_identity_local_patient_identifier_issuer,
    "appointment.patient.identity.birth_date" as appointment_patient_identity_birth_date,
    "appointment.patient.identity.birth_month" as appointment_patient_identity_birth_month,
    "appointment.patient.identity.birth_year" as appointment_patient_identity_birth_year,
    "appointment.patient.identity.age_range" as appointment_patient_identity_age_range,
    "appointment.patient.identity.age_at_cds_activity_date"
    as appointment_patient_identity_age_at_cds_activity_date,
    "appointment.patient.identity.age_at_record_start"
    as appointment_patient_identity_age_at_record_start,
    "appointment.patient.identity.fractional_age_at_record_start"
    as appointment_patient_identity_fractional_age_at_record_start,
    "appointment.patient.identity.hes_age_at_record_start"
    as appointment_patient_identity_hes_age_at_record_start,
    "appointment.patient.identity.age_at_record_end"
    as appointment_patient_identity_age_at_record_end,
    "appointment.patient.identity.hes_age_at_record_end"
    as appointment_patient_identity_hes_age_at_record_end,
    "appointment.patient.identity.gender" as appointment_patient_identity_gender,
    "appointment.patient.identity.ethnic_category" as appointment_patient_identity_ethnic_category,
    "appointment.patient.identity.ethnic_category_2021"
    as appointment_patient_identity_ethnic_category_2021,
    "appointment.patient.identity.pds_date_of_death"
    as appointment_patient_identity_pds_date_of_death,
    "appointment.patient.identity.pds_death_status"
    as appointment_patient_identity_pds_death_status,
    "appointment.patient.identity.withheld_identity_reason"
    as appointment_patient_identity_withheld_identity_reason,
    "appointment.patient.identity.carer_support_indicator"
    as appointment_patient_identity_carer_support_indicator,
    "appointment.patient.overseas_visitor.classification"
    as appointment_patient_overseas_visitor_classification,
    "appointment.patient.overseas_visitor_charging_category_at_cds_activity_date"
    as appointment_patient_overseas_visitor_charging_category_at_cds_activity_date,
    "appointment.identifier" as appointment_identifier,
    "appointment.date" as appointment_date,
    "appointment.time" as appointment_time,
    "appointment.first_attendance" as appointment_first_attendance,
    "appointment.attended_or_dna" as appointment_attended_or_dna,
    "appointment.outcome" as appointment_outcome,
    "appointment.administrative_category" as appointment_administrative_category,
    "appointment.expected_duration" as appointment_expected_duration,
    "appointment.consultation_medium" as appointment_consultation_medium,
    "appointment.consultation_type" as appointment_consultation_type,
    "appointment.booked_reason" as appointment_booked_reason,
    "appointment.last_dna_or_cancelled_date" as appointment_last_dna_or_cancelled_date,
    "appointment.last_patient_did_not_attend_date" as appointment_last_patient_did_not_attend_date,
    "appointment.last_patient_cancelled_date" as appointment_last_patient_cancelled_date,
    "appointment.personalised_outpatient_follow_up_pathway_review_date"
    as appointment_personalised_outpatient_follow_up_pathway_review_date,
    "appointment.personalised_outpatient_follow_up_pathway_expiry_date"
    as appointment_personalised_outpatient_follow_up_pathway_expiry_date,
    "appointment.patient_on_patient_initiated_outpatient_follow_up_pathway_indicator_at_cds_activity_date"
    as appointment_patient_on_patient_initiated_outpatient_follow_up_pathway_indicator_at_cds_activity_date,
    "appointment.patient_subject_to_remote_monitoring_indicator_at_cds_activity_date"
    as appointment_patient_subject_to_remote_monitoring_indicator_at_cds_activity_date,
    "appointment.referral.referrer" as appointment_referral_referrer,
    "appointment.referral.referring_organisation" as appointment_referral_referring_organisation,
    "appointment.referral.received_date" as appointment_referral_received_date,
    "appointment.referral.source_op" as appointment_referral_source_op,
    "appointment.referral.priority_type" as appointment_referral_priority_type,
    "appointment.referral.service_type_requested" as appointment_referral_service_type_requested,
    "appointment.referral.direct_access_indicator" as appointment_referral_direct_access_indicator,
    "appointment.referral.referred_by_first_contact_practitioner_indicator"
    as appointment_referral_referred_by_first_contact_practitioner_indicator,
    "appointment.referral.service_request_identifier"
    as appointment_referral_service_request_identifier,
    "appointment.referral.earliest_clinically_appropriate_date"
    as appointment_referral_earliest_clinically_appropriate_date,
    "appointment.referral.latest_clinically_appropriate_date"
    as appointment_referral_latest_clinically_appropriate_date,
    "appointment.referral.earliest_reasonable_offer_date"
    as appointment_referral_earliest_reasonable_offer_date,
    "appointment.referral_to_treatment.ubrn.value Pseudo"
    as appointment_referral_to_treatment_ubrn_value_pseudo,
    "appointment.referral_to_treatment.ubrn.is_valid"
    as appointment_referral_to_treatment_ubrn_is_valid,
    "appointment.referral_to_treatment.ubrn.occurrences"
    as appointment_referral_to_treatment_ubrn_occurrences,
    "appointment.referral_to_treatment.ubrn.issuer"
    as appointment_referral_to_treatment_ubrn_issuer,
    "appointment.referral_to_treatment.patient_pathway_identifier.value Pseudo"
    as appointment_referral_to_treatment_patient_pathway_identifier_value_pseudo,
    "appointment.referral_to_treatment.patient_pathway_identifier.issuer"
    as appointment_referral_to_treatment_patient_pathway_identifier_issuer,
    "appointment.referral_to_treatment.period_status"
    as appointment_referral_to_treatment_period_status,
    "appointment.referral_to_treatment.period_start_date"
    as appointment_referral_to_treatment_period_start_date,
    "appointment.referral_to_treatment.period_end_date"
    as appointment_referral_to_treatment_period_end_date,
    "appointment.referral_to_treatment.period_length"
    as appointment_referral_to_treatment_period_length,
    "appointment.referral_to_treatment.waiting_time_measurement_type"
    as appointment_referral_to_treatment_waiting_time_measurement_type,
    "appointment.care_professional.consultant" as appointment_care_professional_consultant,
    "appointment.care_professional.main_specialty" as appointment_care_professional_main_specialty,
    "appointment.care_professional.treatment_function"
    as appointment_care_professional_treatment_function,
    "appointment.care_professional.local_sub_specialty"
    as appointment_care_professional_local_sub_specialty,
    "appointment.care_professional.medical_staff_type"
    as appointment_care_professional_medical_staff_type,
    "appointment.care_professional.multiple_professional_or_discipline_indicator"
    as appointment_care_professional_multiple_professional_or_discipline_indicator,
    "appointment.care_professional.rehabilitation_assessment_team_type"
    as appointment_care_professional_rehabilitation_assessment_team_type,
    "appointment.care_location.site_code_of_treatment"
    as appointment_care_location_site_code_of_treatment,
    "appointment.care_location.class" as appointment_care_location_class,
    "appointment.care_location.clinic_code" as appointment_care_location_clinic_code,
    "appointment.care_location.type" as appointment_care_location_type,
    "appointment.care_location.organisation_geographies.postcode_district"
    as appointment_care_location_organisation_geographies_postcode_district,
    "appointment.care_location.organisation_geographies.government_office_region"
    as appointment_care_location_organisation_geographies_government_office_region,
    "appointment.care_location.organisation_geographies.sha_provider"
    as appointment_care_location_organisation_geographies_sha_provider,
    "appointment.care_location.organisation_geographies.ccg_from_treatment_site.value"
    as appointment_care_location_organisation_geographies_ccg_from_treatment_site_value,
    "appointment.care_location.organisation_geographies.ccg_from_treatment_site.origin"
    as appointment_care_location_organisation_geographies_ccg_from_treatment_site_origin,
    "appointment.care_location.organisation_geographies.lsoa_provider_distance.value"
    as appointment_care_location_organisation_geographies_lsoa_provider_distance_value,
    "appointment.care_location.organisation_geographies.lsoa_provider_distance.origin"
    as appointment_care_location_organisation_geographies_lsoa_provider_distance_origin,
    "appointment.care_location.organisation_geographies.lsoa_site_of_treatment_distance.value"
    as appointment_care_location_organisation_geographies_lsoa_site_of_treatment_distance_value,
    "appointment.care_location.organisation_geographies.lsoa_site_of_treatment_distance.origin"
    as appointment_care_location_organisation_geographies_lsoa_site_of_treatment_distance_origin,
    "appointment.clinical_coding.grouper_derived.dominant_procedure"
    as appointment_clinical_coding_grouper_derived_dominant_procedure,
    "appointment.clinical_coding.procedure.operation_status"
    as appointment_clinical_coding_procedure_operation_status,
    "appointment.commissioning.tariff_calculation.aggregate_unbundled_tariff"
    as appointment_commissioning_tariff_calculation_aggregate_unbundled_tariff,
    "appointment.commissioning.tariff_calculation.direct_access_applies"
    as appointment_commissioning_tariff_calculation_direct_access_applies,
    "appointment.commissioning.tariff_calculation.tariff_applied"
    as appointment_commissioning_tariff_calculation_tariff_applied,
    "appointment.commissioning.tariff_calculation.specialised_services_tariff_uplift"
    as appointment_commissioning_tariff_calculation_specialised_services_tariff_uplift,
    "appointment.commissioning.tariff_calculation.market_forces_factor"
    as appointment_commissioning_tariff_calculation_market_forces_factor,
    "appointment.commissioning.tariff_calculation.initial_tariff"
    as appointment_commissioning_tariff_calculation_initial_tariff,
    "appointment.commissioning.tariff_calculation.final_price"
    as appointment_commissioning_tariff_calculation_final_price,
    "appointment.commissioning.tariff_calculation.bpt_indicator"
    as appointment_commissioning_tariff_calculation_bpt_indicator,
    "appointment.commissioning.tariff_calculation.bpt_indicator_action"
    as appointment_commissioning_tariff_calculation_bpt_indicator_action,
    "appointment.commissioning.tariff_calculation.excluded_from_pbr"
    as appointment_commissioning_tariff_calculation_excluded_from_pbr,
    "appointment.commissioning.tariff_calculation.patient_type"
    as appointment_commissioning_tariff_calculation_patient_type,
    "appointment.commissioning.tariff_calculation.outpatient_type"
    as appointment_commissioning_tariff_calculation_outpatient_type,
    "appointment.commissioning.commissioner_assignment.commissioner"
    as appointment_commissioning_commissioner_assignment_commissioner,
    "appointment.commissioning.commissioner_assignment.flowchart_reference"
    as appointment_commissioning_commissioner_assignment_flowchart_reference,
    "appointment.commissioning.service_agreement.provider"
    as appointment_commissioning_service_agreement_provider,
    "appointment.commissioning.service_agreement.provider_derived"
    as appointment_commissioning_service_agreement_provider_derived,
    "appointment.commissioning.service_agreement.provider_reference_number"
    as appointment_commissioning_service_agreement_provider_reference_number,
    "appointment.commissioning.service_agreement.commissioner"
    as appointment_commissioning_service_agreement_commissioner,
    "appointment.commissioning.service_agreement.commissioner_derived"
    as appointment_commissioning_service_agreement_commissioner_derived,
    "appointment.commissioning.service_agreement.commissioner_reference_number"
    as appointment_commissioning_service_agreement_commissioner_reference_number,
    "appointment.commissioning.service_agreement.is_commissioner_recognised"
    as appointment_commissioning_service_agreement_is_commissioner_recognised,
    "appointment.commissioning.service_agreement.commissioning_serial_number"
    as appointment_commissioning_service_agreement_commissioning_serial_number,
    "appointment.commissioning.service_agreement.line_number"
    as appointment_commissioning_service_agreement_line_number,
    "appointment.commissioning.service_agreement.applicable_costing_period"
    as appointment_commissioning_service_agreement_applicable_costing_period,
    "appointment.commissioning.grouping.core_hrg" as appointment_commissioning_grouping_core_hrg,
    "appointment.commissioning.grouping.sus_hrg" as appointment_commissioning_grouping_sus_hrg,
    "appointment.commissioning.grouping.grouping_method"
    as appointment_commissioning_grouping_grouping_method,
    "appointment.commissioning.grouping.code_cleaning_applied"
    as appointment_commissioning_grouping_code_cleaning_applied,
    "appointment.commissioning.pss_grouping.national_programme_code"
    as appointment_commissioning_pss_grouping_national_programme_code,
    "appointment.commissioning.pss_grouping.prescribed_service_line_code"
    as appointment_commissioning_pss_grouping_prescribed_service_line_code,
    "appointment.commissioning.pss_grouping.tariff_uplift_rate"
    as appointment_commissioning_pss_grouping_tariff_uplift_rate,
    "system.transaction.cds_sender" as system_transaction_cds_sender,
    "system.transaction.cds_protocol_identifier" as system_transaction_cds_protocol_identifier,
    "system.transaction.cds_update_type" as system_transaction_cds_update_type,
    "system.transaction.cds_applicable_date" as system_transaction_cds_applicable_date,
    "system.transaction.cds_applicable_time" as system_transaction_cds_applicable_time,
    "system.transaction.cds_prime_recipient" as system_transaction_cds_prime_recipient,
    "system.transaction.cds_extract_date" as system_transaction_cds_extract_date,
    "system.transaction.cds_extract_time" as system_transaction_cds_extract_time,
    "system.transaction.cds_bulk_replacement_group"
    as system_transaction_cds_bulk_replacement_group,
    "system.transaction.cds_report_period_start_date"
    as system_transaction_cds_report_period_start_date,
    "system.transaction.cds_report_period_end_date"
    as system_transaction_cds_report_period_end_date,
    "system.transaction.cds_record_type" as system_transaction_cds_record_type,
    "system.transaction.cds_unique_identifier" as system_transaction_cds_unique_identifier,
    "system.transaction.cds_activity_date" as system_transaction_cds_activity_date,
    "system.record.identifier" as system_record_identifier,
    "system.record.version" as system_record_version,
    "system.record.cds_group_indicator" as system_record_cds_group_indicator,
    "system.record.finished_indicator" as system_record_finished_indicator,
    "system.record.confidentiality_category" as system_record_confidentiality_category,
    "system.report.as_at" as system_report_as_at,
    "system.report.date_range" as system_report_date_range,
    "system.report.clear_data" as system_report_clear_data,
    "system.report.grouper_version" as system_report_grouper_version,
    "system.report.tariff_version" as system_report_tariff_version,
    "system.report.query_identifier" as system_report_query_identifier,
    "system.report.query_date" as system_report_query_date,
    "system.report.query_time" as system_report_query_time,
    "system.report.activity_date_from" as system_report_activity_date_from,
    "system.report.activity_date_to" as system_report_activity_date_to,
    "system.interchange.cds_control_reference" as system_interchange_cds_control_reference,
    "system.interchange.cds_record_identifier" as system_interchange_cds_record_identifier,
    "system.interchange.received_date" as system_interchange_received_date,
    "system.interchange.received_time" as system_interchange_received_time,
    "system.interchange.cds_message_version_number"
    as system_interchange_cds_message_version_number,
    "system.interchange.cds_message_reference" as system_interchange_cds_message_reference,
    "system.interchange.identifier" as system_interchange_identifier
from {{ source("sus_op", "appointment") }}
