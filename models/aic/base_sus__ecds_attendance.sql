{{ config(materialized='view') }}

select
    rownumber_id,
    primarykey_id,
    "patient.nhs_number.value Pseudo" as patient_nhs_number_value_pseudo,
    "patient.usual_address.postcode Pseudo" as patient_usual_address_postcode_pseudo,
    "patient.ethnic_category" as patient_ethnic_category,
    "patient.local_patient_identifier.value" as patient_local_patient_identifier_value,
    "attendance.arrival.attendance_identifier" as attendance_arrival_attendance_identifier,
    "referral.patient_pathway_identifier.value Pseudo"
    as referral_patient_pathway_identifier_value_pseudo,
    "system.interchange.identifier" as system_interchange_identifier,
    "system.interchange.received_date" as system_interchange_received_date,
    "system.interchange.schema_version" as system_interchange_schema_version,
    "system.transaction.cds_protocol_identifier" as system_transaction_cds_protocol_identifier,
    "system.transaction.cds_unique_identifier" as system_transaction_cds_unique_identifier,
    "system.transaction.cds_update_type" as system_transaction_cds_update_type,
    "system.transaction.cds_applicable_date" as system_transaction_cds_applicable_date,
    "system.transaction.cds_applicable_time" as system_transaction_cds_applicable_time,
    "system.record.identifier" as system_record_identifier,
    "system.record.version" as system_record_version,
    "system.record.cds_record_identifier" as system_record_cds_record_identifier,
    "system.record.cds_record_type" as system_record_cds_record_type,
    "system.record.cds_group_indicator" as system_record_cds_group_indicator,
    "system.record.finished_indicator" as system_record_finished_indicator,
    "system.record.cds_activity_date" as system_record_cds_activity_date,
    "system.record.sender" as system_record_sender,
    "system.record.prime_recipient" as system_record_prime_recipient,
    "system.record.provider" as system_record_provider,
    "system.record.commissioner" as system_record_commissioner,
    "system.report.query_identifier" as system_report_query_identifier,
    "system.report.query_date" as system_report_query_date,
    "system.report.query_time" as system_report_query_time,
    "system.reference_data.snomed_version" as system_reference_data_snomed_version,
    "patient.nhs_number.status" as patient_nhs_number_status,
    "patient.nhs_number.is_valid" as patient_nhs_number_is_valid,
    "patient.local_patient_identifier.issuer" as patient_local_patient_identifier_issuer,
    "patient.patient_type" as patient_patient_type,
    "patient.age_range" as patient_age_range,
    "patient.age_at_cds_activity_date" as patient_age_at_cds_activity_date,
    "patient.age_at_arrival" as patient_age_at_arrival,
    "patient.fractional_age_at_arrival" as patient_fractional_age_at_arrival,
    "patient.hes_age_at_arrival" as patient_hes_age_at_arrival,
    "patient.hes_age_at_departure" as patient_hes_age_at_departure,
    "patient.usual_address.is_postcode_valid" as patient_usual_address_is_postcode_valid,
    "patient.usual_address.postcode_district" as patient_usual_address_postcode_district,
    "patient.usual_address.oa_11" as patient_usual_address_oa_11,
    "patient.usual_address.msoa_11" as patient_usual_address_msoa_11,
    "patient.usual_address.lsoa_11" as patient_usual_address_lsoa_11,
    "patient.usual_address.country" as patient_usual_address_country,
    "patient.usual_address.government_office_region"
    as patient_usual_address_government_office_region,
    "patient.usual_address.residence_county" as patient_usual_address_residence_county,
    "patient.usual_address.local_authority_district"
    as patient_usual_address_local_authority_district,
    "patient.usual_address.parliamentary_constituency"
    as patient_usual_address_parliamentary_constituency,
    "patient.usual_address.ed_county_code" as patient_usual_address_ed_county_code,
    "patient.usual_address.ed_district_code" as patient_usual_address_ed_district_code,
    "patient.usual_address.electoral_ward_1998" as patient_usual_address_electoral_ward_1998,
    "patient.usual_address.index_of_multiple_deprivation.decile"
    as patient_usual_address_index_of_multiple_deprivation_decile,
    "clinical.chief_complaint.code" as clinical_chief_complaint_code,
    "clinical.chief_complaint.is_code_approved" as clinical_chief_complaint_is_code_approved,
    "clinical.chief_complaint.is_injury_related" as clinical_chief_complaint_is_injury_related,
    "clinical.injury.date" as clinical_injury_date,
    "clinical.injury.time" as clinical_injury_time,
    "clinical.injury.activity_status.code" as clinical_injury_activity_status_code,
    "clinical.injury.activity_status.is_code_approved"
    as clinical_injury_activity_status_is_code_approved,
    "clinical.injury.activity_type.code" as clinical_injury_activity_type_code,
    "clinical.injury.activity_type.is_code_approved"
    as clinical_injury_activity_type_is_code_approved,
    "clinical.injury.intent.code" as clinical_injury_intent_code,
    "clinical.injury.intent.is_code_approved" as clinical_injury_intent_is_code_approved,
    "clinical.injury.mechanism.code" as clinical_injury_mechanism_code,
    "clinical.injury.mechanism.is_code_approved" as clinical_injury_mechanism_is_code_approved,
    "clinical.injury.place.type" as clinical_injury_place_type,
    "clinical.injury.place.is_type_approved" as clinical_injury_place_is_type_approved,
    "data_quality.chief_complaint.valid" as data_quality_chief_complaint_valid,
    "data_quality.primary_diagnosis.valid" as data_quality_primary_diagnosis_valid,
    "commissioning.service_agreement.commissioning_serial_number"
    as commissioning_service_agreement_commissioning_serial_number,
    "system.transaction.cds_bulk_replacement_group"
    as system_transaction_cds_bulk_replacement_group,
    "system.transaction.cds_extract_date" as system_transaction_cds_extract_date,
    "system.transaction.cds_extract_time" as system_transaction_cds_extract_time,
    "system.transaction.cds_report_period_start_date"
    as system_transaction_cds_report_period_start_date,
    "system.transaction.cds_report_period_end_date"
    as system_transaction_cds_report_period_end_date,
    "attendance.arrival.ambulance_incident_number" as attendance_arrival_ambulance_incident_number,
    "attendance.arrival.conveying_ambulance_trust" as attendance_arrival_conveying_ambulance_trust,
    "clinical.acuity.code" as clinical_acuity_code,
    "clinical.acuity.is_code_approved" as clinical_acuity_is_code_approved,
    "patient.usual_address.index_of_multiple_deprivation.decile_description"
    as patient_usual_address_index_of_multiple_deprivation_decile_description,
    "patient.usual_address.rural_urban_indicator" as patient_usual_address_rural_urban_indicator,
    "patient.usual_address.cancer_registry" as patient_usual_address_cancer_registry,
    "patient.usual_address.commissioning_region" as patient_usual_address_commissioning_region,
    "patient.residence_ccg" as patient_residence_ccg,
    "patient.residence_ccg_from_patient_postcode" as patient_residence_ccg_from_patient_postcode,
    "patient.hes_ccg_from_patient_postcode" as patient_hes_ccg_from_patient_postcode,
    "patient.stated_gender" as patient_stated_gender,
    "patient.accommodation_status.code" as patient_accommodation_status_code,
    "patient.accommodation_status.is_code_approved"
    as patient_accommodation_status_is_code_approved,
    "patient.preferred_spoken_language.code" as patient_preferred_spoken_language_code,
    "patient.preferred_spoken_language.is_code_approved"
    as patient_preferred_spoken_language_is_code_approved,
    "patient.accessible_information_professional_required.code"
    as patient_accessible_information_professional_required_code,
    "patient.accessible_information_professional_required.is_code_approved"
    as patient_accessible_information_professional_required_is_code_approved,
    "patient.overseas_visitor_charging_category" as patient_overseas_visitor_charging_category,
    "patient.gp_registration.general_practitioner" as patient_gp_registration_general_practitioner,
    "patient.gp_registration.general_practice" as patient_gp_registration_general_practice,
    "patient.gp_registration.pds_general_practice" as patient_gp_registration_pds_general_practice,
    "patient.gp_registration.responsible_ccg_from_general_practice"
    as patient_gp_registration_responsible_ccg_from_general_practice,
    "patient.gp_registration.hes_ccg_from_general_practice"
    as patient_gp_registration_hes_ccg_from_general_practice,
    "patient.gp_registration.hes_responsible_ccg.value"
    as patient_gp_registration_hes_responsible_ccg_value,
    "patient.gp_registration.hes_responsible_ccg.origin"
    as patient_gp_registration_hes_responsible_ccg_origin,
    "attendance.unique_identifier" as attendance_unique_identifier,
    "attendance.location.site" as attendance_location_site,
    "attendance.location.hes_provider_3" as attendance_location_hes_provider_3,
    "attendance.location.hes_provider_5" as attendance_location_hes_provider_5,
    "attendance.location.postcode_district" as attendance_location_postcode_district,
    "attendance.clinically_ready_to_proceed.time_since_arrival"
    as attendance_clinically_ready_to_proceed_time_since_arrival,
    "referral.ubrn.value Pseudo" as referral_ubrn_value_pseudo,
    "referral.ubrn.issuer" as referral_ubrn_issuer,
    "patient.name.suffix" as patient_name_suffix,
    "clinical.disease_notification_code" as clinical_disease_notification_code,
    "clinical.disease_notification_description" as clinical_disease_notification_description,
    "schema_version" as schema_version,
    "attendance.location.activity_type" as attendance_location_activity_type,
    "attendance.arrival.timestamp" as attendance_arrival_timestamp,
    "attendance.arrival.consultation_mechanism" as attendance_arrival_consultation_mechanism,
    "attendance.initial_assessment.timestamp" as attendance_initial_assessment_timestamp,
    "attendance.seen_for_treatment.timestamp" as attendance_seen_for_treatment_timestamp,
    "attendance.extended_care_episode_identifier" as attendance_extended_care_episode_identifier,
    "attendance.departure.timestamp" as attendance_departure_timestamp,
    "attendance.decision_to_admit.timestamp" as attendance_decision_to_admit_timestamp,
    "clinical.coding.fit_note.assessment_date" as clinical_coding_fit_note_assessment_date,
    "clinical.coding.fit_note.condition" as clinical_coding_fit_note_condition,
    "clinical.coding.fit_note.condition_approved" as clinical_coding_fit_note_condition_approved,
    "clinical.coding.fit_note.diagnosis" as clinical_coding_fit_note_diagnosis,
    "clinical.coding.fit_note.diagnosis_code_approved"
    as clinical_coding_fit_note_diagnosis_code_approved,
    "clinical.coding.fit_note.start_date" as clinical_coding_fit_note_start_date,
    "clinical.coding.fit_note.end_date" as clinical_coding_fit_note_end_date,
    "clinical.coding.fit_note.duration" as clinical_coding_fit_note_duration,
    "clinical.coding.fit_note.recorded_date" as clinical_coding_fit_note_recorded_date,
    "clinical.coding.fit_note.follow_up_assessment_required_indicator"
    as clinical_coding_fit_note_follow_up_assessment_required_indicator,
    "clinical.coding.fit_note.issuer" as clinical_coding_fit_note_issuer,
    "attendance.discharge.information_given.indicator"
    as attendance_discharge_information_given_indicator,
    "clinical.injury.timestamp" as clinical_injury_timestamp,
    "attendance.decision_to_admit.date" as attendance_decision_to_admit_date,
    "attendance.decision_to_admit.time" as attendance_decision_to_admit_time,
    "attendance.decision_to_admit.time_since_arrival"
    as attendance_decision_to_admit_time_since_arrival,
    "attendance.decision_to_admit.treatment_function_code"
    as attendance_decision_to_admit_treatment_function_code,
    "commissioning.service_agreement.provider_reference_number"
    as commissioning_service_agreement_provider_reference_number,
    "attendance.discharge.information_given.code" as attendance_discharge_information_given_code,
    "attendance.discharge.information_given.is_code_approved"
    as attendance_discharge_information_given_is_code_approved,
    "attendance.arrival.attendance_source_organisation"
    as attendance_arrival_attendance_source_organisation,
    "clinical.chief_complaint.extended_code" as clinical_chief_complaint_extended_code,
    "attendance.decision_to_admit.receiving_site" as attendance_decision_to_admit_receiving_site,
    "commissioning.service_agreement.line_number" as commissioning_service_agreement_line_number,
    "referral.patient_pathway_identifier.issuer" as referral_patient_pathway_identifier_issuer,
    "referral.period_status" as referral_period_status,
    "referral.waiting_time_measurement_type" as referral_waiting_time_measurement_type,
    "patient.interpreter_language.code" as patient_interpreter_language_code,
    "patient.interpreter_language.is_code_approved"
    as patient_interpreter_language_is_code_approved,
    "clinical.disease_notification" as clinical_disease_notification,
    "patient.withheld_identity_reason" as patient_withheld_identity_reason,
    "clinical.injury.place.latitude" as clinical_injury_place_latitude,
    "clinical.injury.place.longitude" as clinical_injury_place_longitude,
    "referral.period_start_date" as referral_period_start_date,
    "referral.period_end_date" as referral_period_end_date,
    "referral.period_length" as referral_period_length,
    "system.record.confidentiality_category" as system_record_confidentiality_category,
    "patient.ethnic_category_2021" as patient_ethnic_category_2021,
    "attendance.arrival.ambulance_care_contact_identifier"
    as attendance_arrival_ambulance_care_contact_identifier,
    "attendance.clinically_ready_to_proceed.timestamp"
    as attendance_clinically_ready_to_proceed_timestamp,
    "attendance.location.government_office_region" as attendance_location_government_office_region,
    "attendance.location.sha_provider" as attendance_location_sha_provider,
    "attendance.location.hes_ccg_from_treatment_site.value"
    as attendance_location_hes_ccg_from_treatment_site_value,
    "attendance.location.hes_ccg_from_treatment_site.origin"
    as attendance_location_hes_ccg_from_treatment_site_origin,
    "attendance.location.department_type" as attendance_location_department_type,
    "attendance.location.lsoa_provider_distance.value"
    as attendance_location_lsoa_provider_distance_value,
    "attendance.location.lsoa_provider_distance.origin"
    as attendance_location_lsoa_provider_distance_origin,
    "attendance.location.lsoa_site_of_treatment_distance.value"
    as attendance_location_lsoa_site_of_treatment_distance_value,
    "attendance.location.lsoa_site_of_treatment_distance.origin"
    as attendance_location_lsoa_site_of_treatment_distance_origin,
    "attendance.arrival.date" as attendance_arrival_date,
    "attendance.arrival.time" as attendance_arrival_time,
    "attendance.arrival.month" as attendance_arrival_month,
    "attendance.arrival.arrival_mode.code" as attendance_arrival_arrival_mode_code,
    "attendance.arrival.arrival_mode.is_code_approved"
    as attendance_arrival_arrival_mode_is_code_approved,
    "attendance.arrival.attendance_category" as attendance_arrival_attendance_category,
    "attendance.arrival.planned" as attendance_arrival_planned,
    "attendance.arrival.attendance_source.code" as attendance_arrival_attendance_source_code,
    "attendance.arrival.attendance_source.is_code_approved"
    as attendance_arrival_attendance_source_is_code_approved,
    "attendance.conclusion.date" as attendance_conclusion_date,
    "attendance.conclusion.time" as attendance_conclusion_time,
    "attendance.conclusion.time_since_arrival" as attendance_conclusion_time_since_arrival,
    "attendance.departure.date" as attendance_departure_date,
    "attendance.departure.time" as attendance_departure_time,
    "attendance.departure.time_since_arrival" as attendance_departure_time_since_arrival,
    "attendance.discharge.status.code" as attendance_discharge_status_code,
    "attendance.discharge.status.is_code_approved" as attendance_discharge_status_is_code_approved,
    "commissioning.service_agreement.provider" as commissioning_service_agreement_provider,
    "commissioning.service_agreement.commissioner" as commissioning_service_agreement_commissioner,
    "commissioning.service_agreement.commissioner_reference_number"
    as commissioning_service_agreement_commissioner_reference_number,
    "commissioning.service_agreement.sha_commissioner"
    as commissioning_service_agreement_sha_commissioner,
    "commissioning.service_agreement.is_commissioner_recognised"
    as commissioning_service_agreement_is_commissioner_recognised,
    "commissioning.service_agreement.provider_type"
    as commissioning_service_agreement_provider_type,
    "commissioning.grouping.grouper_version" as commissioning_grouping_grouper_version,
    "commissioning.grouping.code_cleaning_applied" as commissioning_grouping_code_cleaning_applied,
    "commissioning.grouping.health_resource_group" as commissioning_grouping_health_resource_group,
    "commissioning.national_pricing.costing_period"
    as commissioning_national_pricing_costing_period,
    "commissioning.national_pricing.excluded" as commissioning_national_pricing_excluded,
    "commissioning.national_pricing.tariff" as commissioning_national_pricing_tariff,
    "commissioning.national_pricing.tariff_description"
    as commissioning_national_pricing_tariff_description,
    "commissioning.national_pricing.market_forces_factor"
    as commissioning_national_pricing_market_forces_factor,
    "commissioning.national_pricing.market_forces_adjustment"
    as commissioning_national_pricing_market_forces_adjustment,
    "commissioning.national_pricing.final_price" as commissioning_national_pricing_final_price,
    "data_quality.chief_complaint.expected" as data_quality_chief_complaint_expected,
    "data_quality.chief_complaint.completed" as data_quality_chief_complaint_completed,
    "data_quality.primary_diagnosis.expected" as data_quality_primary_diagnosis_expected,
    "data_quality.primary_diagnosis.completed" as data_quality_primary_diagnosis_completed,
    "attendance.initial_assessment.date" as attendance_initial_assessment_date,
    "attendance.initial_assessment.time" as attendance_initial_assessment_time,
    "attendance.initial_assessment.time_since_arrival"
    as attendance_initial_assessment_time_since_arrival,
    "attendance.seen_for_treatment.date" as attendance_seen_for_treatment_date,
    "attendance.seen_for_treatment.time" as attendance_seen_for_treatment_time,
    "attendance.seen_for_treatment.time_since_arrival"
    as attendance_seen_for_treatment_time_since_arrival,
    "attendance.discharge.destination.code" as attendance_discharge_destination_code,
    "attendance.discharge.destination.is_code_approved"
    as attendance_discharge_destination_is_code_approved,
    "attendance.discharge.follow_up.code" as attendance_discharge_follow_up_code,
    "attendance.discharge.follow_up.is_code_approved"
    as attendance_discharge_follow_up_is_code_approved,
    "commissioning.commissioner_assignment.commissioner"
    as commissioning_commissioner_assignment_commissioner,
    "commissioning.commissioner_assignment.flowchart_reference"
    as commissioning_commissioner_assignment_flowchart_reference,
    "dmicImportLogId" as dmicimportlogid
from {{ source("sus_ecds", "emergency_care") }}
