-- Find lds_record_id values that exist in Clinical_Validation but not in Alpha
-- Returns up to 100 examples per table to support investigation
USE ROLE "ISL-USERGROUP-SECONDEES-NCL";

SELECT * FROM (
    SELECT 'ALLERGY_INTOLERANCE' AS table_name, cv."lds_record_id"
    FROM "Data_Store_OLIDS_Clinical_Validation".OLIDS_COMMON.ALLERGY_INTOLERANCE cv
    LEFT JOIN "Data_Store_OLIDS_Alpha".OLIDS_COMMON.ALLERGY_INTOLERANCE a
        ON a.lds_record_id = cv."lds_record_id"
    WHERE a.lds_record_id IS NULL
    LIMIT 100
)
UNION ALL
SELECT * FROM (
    SELECT 'APPOINTMENT' AS table_name, cv."lds_record_id"
    FROM "Data_Store_OLIDS_Clinical_Validation".OLIDS_COMMON.APPOINTMENT cv
    LEFT JOIN "Data_Store_OLIDS_Alpha".OLIDS_COMMON.APPOINTMENT a
        ON a.lds_record_id = cv."lds_record_id"
    WHERE a.lds_record_id IS NULL
    LIMIT 100
)
UNION ALL
SELECT * FROM (
    SELECT 'APPOINTMENT_PRACTITIONER' AS table_name, cv."lds_record_id"
    FROM "Data_Store_OLIDS_Clinical_Validation".OLIDS_COMMON.APPOINTMENT_PRACTITIONER cv
    LEFT JOIN "Data_Store_OLIDS_Alpha".OLIDS_COMMON.APPOINTMENT_PRACTITIONER a
        ON a.lds_record_id = cv."lds_record_id"
    WHERE a.lds_record_id IS NULL
    LIMIT 100
)
UNION ALL
SELECT * FROM (
    SELECT 'DIAGNOSTIC_ORDER' AS table_name, cv."lds_record_id"
    FROM "Data_Store_OLIDS_Clinical_Validation".OLIDS_COMMON.DIAGNOSTIC_ORDER cv
    LEFT JOIN "Data_Store_OLIDS_Alpha".OLIDS_COMMON.DIAGNOSTIC_ORDER a
        ON a.lds_record_id = cv."lds_record_id"
    WHERE a.lds_record_id IS NULL
    LIMIT 100
)
UNION ALL
SELECT * FROM (
    SELECT 'ENCOUNTER' AS table_name, cv."lds_record_id"
    FROM "Data_Store_OLIDS_Clinical_Validation".OLIDS_COMMON.ENCOUNTER cv
    LEFT JOIN "Data_Store_OLIDS_Alpha".OLIDS_COMMON.ENCOUNTER a
        ON a.lds_record_id = cv."lds_record_id"
    WHERE a.lds_record_id IS NULL
    LIMIT 100
)
UNION ALL
SELECT * FROM (
    SELECT 'EPISODE_OF_CARE' AS table_name, cv."lds_record_id"
    FROM "Data_Store_OLIDS_Clinical_Validation".OLIDS_COMMON.EPISODE_OF_CARE cv
    LEFT JOIN "Data_Store_OLIDS_Alpha".OLIDS_COMMON.EPISODE_OF_CARE a
        ON a.lds_record_id = cv."lds_record_id"
    WHERE a.lds_record_id IS NULL
    LIMIT 100
)
UNION ALL
SELECT * FROM (
    SELECT 'FLAG' AS table_name, cv."lds_record_id"
    FROM "Data_Store_OLIDS_Clinical_Validation".OLIDS_COMMON.FLAG cv
    LEFT JOIN "Data_Store_OLIDS_Alpha".OLIDS_COMMON.FLAG a
        ON a.lds_record_id = cv."lds_record_id"
    WHERE a.lds_record_id IS NULL
    LIMIT 100
)
UNION ALL
SELECT * FROM (
    SELECT 'LOCATION' AS table_name, cv."lds_record_id"
    FROM "Data_Store_OLIDS_Clinical_Validation".OLIDS_COMMON.LOCATION cv
    LEFT JOIN "Data_Store_OLIDS_Alpha".OLIDS_COMMON.LOCATION a
        ON a.lds_record_id = cv."lds_record_id"
    WHERE a.lds_record_id IS NULL
    LIMIT 100
)
UNION ALL
SELECT * FROM (
    SELECT 'LOCATION_CONTACT' AS table_name, cv."lds_record_id"
    FROM "Data_Store_OLIDS_Clinical_Validation".OLIDS_COMMON.LOCATION_CONTACT cv
    LEFT JOIN "Data_Store_OLIDS_Alpha".OLIDS_COMMON.LOCATION_CONTACT a
        ON a.lds_record_id = cv."lds_record_id"
    WHERE a.lds_record_id IS NULL
    LIMIT 100
)
UNION ALL
SELECT * FROM (
    SELECT 'MEDICATION_ORDER' AS table_name, cv."lds_record_id"
    FROM "Data_Store_OLIDS_Clinical_Validation".OLIDS_COMMON.MEDICATION_ORDER cv
    LEFT JOIN "Data_Store_OLIDS_Alpha".OLIDS_COMMON.MEDICATION_ORDER a
        ON a.lds_record_id = cv."lds_record_id"
    WHERE a.lds_record_id IS NULL
    LIMIT 100
)
UNION ALL
SELECT * FROM (
    SELECT 'MEDICATION_STATEMENT' AS table_name, cv."lds_record_id"
    FROM "Data_Store_OLIDS_Clinical_Validation".OLIDS_COMMON.MEDICATION_STATEMENT cv
    LEFT JOIN "Data_Store_OLIDS_Alpha".OLIDS_COMMON.MEDICATION_STATEMENT a
        ON a.lds_record_id = cv."lds_record_id"
    WHERE a.lds_record_id IS NULL
    LIMIT 100
)
UNION ALL
SELECT * FROM (
    SELECT 'OBSERVATION' AS table_name, cv."lds_record_id"
    FROM "Data_Store_OLIDS_Clinical_Validation".OLIDS_COMMON.OBSERVATION cv
    LEFT JOIN "Data_Store_OLIDS_Alpha".OLIDS_COMMON.OBSERVATION a
        ON a.lds_record_id = cv."lds_record_id"
    WHERE a.lds_record_id IS NULL
    LIMIT 100
)
UNION ALL
SELECT * FROM (
    SELECT 'ORGANISATION' AS table_name, cv."lds_record_id"
    FROM "Data_Store_OLIDS_Clinical_Validation".OLIDS_COMMON.ORGANISATION cv
    LEFT JOIN "Data_Store_OLIDS_Alpha".OLIDS_COMMON.ORGANISATION a
        ON a.lds_record_id = cv."lds_record_id"
    WHERE a.lds_record_id IS NULL
    LIMIT 100
)
UNION ALL
SELECT * FROM (
    SELECT 'PATIENT' AS table_name, cv."lds_record_id"
    FROM "Data_Store_OLIDS_Clinical_Validation".OLIDS_MASKED.PATIENT cv
    LEFT JOIN "Data_Store_OLIDS_Alpha".OLIDS_MASKED.PATIENT a
        ON a.lds_record_id = cv."lds_record_id"
    WHERE a.lds_record_id IS NULL
    LIMIT 100
)
UNION ALL
SELECT * FROM (
    SELECT 'PATIENT_ADDRESS' AS table_name, cv."lds_record_id"
    FROM "Data_Store_OLIDS_Clinical_Validation".OLIDS_MASKED.PATIENT_ADDRESS cv
    LEFT JOIN "Data_Store_OLIDS_Alpha".OLIDS_MASKED.PATIENT_ADDRESS a
        ON a.lds_record_id = cv."lds_record_id"
    WHERE a.lds_record_id IS NULL
    LIMIT 100
)
UNION ALL
SELECT * FROM (
    SELECT 'PATIENT_CONTACT' AS table_name, cv."lds_record_id"
    FROM "Data_Store_OLIDS_Clinical_Validation".OLIDS_MASKED.PATIENT_CONTACT cv
    LEFT JOIN "Data_Store_OLIDS_Alpha".OLIDS_MASKED.PATIENT_CONTACT a
        ON a.lds_record_id = cv."lds_record_id"
    WHERE a.lds_record_id IS NULL
    LIMIT 100
)
UNION ALL
SELECT * FROM (
    SELECT 'PATIENT_REGISTERED_PRACTITIONER_IN_ROLE' AS table_name, cv."lds_record_id"
    FROM "Data_Store_OLIDS_Clinical_Validation".OLIDS_COMMON.PATIENT_REGISTERED_PRACTITIONER_IN_ROLE cv
    LEFT JOIN "Data_Store_OLIDS_Alpha".OLIDS_COMMON.PATIENT_REGISTERED_PRACTITIONER_IN_ROLE a
        ON a.lds_record_id = cv."lds_record_id"
    WHERE a.lds_record_id IS NULL
    LIMIT 100
)
UNION ALL
SELECT * FROM (
    SELECT 'PATIENT_UPRN' AS table_name, cv."lds_record_id"
    FROM "Data_Store_OLIDS_Clinical_Validation".OLIDS_MASKED.PATIENT_UPRN cv
    LEFT JOIN "Data_Store_OLIDS_Alpha".OLIDS_MASKED.PATIENT_UPRN a
        ON a.lds_record_id = cv."lds_record_id"
    WHERE a.lds_record_id IS NULL
    LIMIT 100
)
UNION ALL
SELECT * FROM (
    SELECT 'PERSON' AS table_name, cv."lds_record_id"
    FROM "Data_Store_OLIDS_Clinical_Validation".OLIDS_MASKED.PERSON cv
    LEFT JOIN "Data_Store_OLIDS_Alpha".OLIDS_MASKED.PERSON a
        ON a.lds_record_id = cv."lds_record_id"
    WHERE a.lds_record_id IS NULL
    LIMIT 100
)
UNION ALL
SELECT * FROM (
    SELECT 'PRACTITIONER' AS table_name, cv."lds_record_id"
    FROM "Data_Store_OLIDS_Clinical_Validation".OLIDS_COMMON.PRACTITIONER cv
    LEFT JOIN "Data_Store_OLIDS_Alpha".OLIDS_COMMON.PRACTITIONER a
        ON a.lds_record_id = cv."lds_record_id"
    WHERE a.lds_record_id IS NULL
    LIMIT 100
)
UNION ALL
SELECT * FROM (
    SELECT 'PRACTITIONER_IN_ROLE' AS table_name, cv."lds_record_id"
    FROM "Data_Store_OLIDS_Clinical_Validation".OLIDS_COMMON.PRACTITIONER_IN_ROLE cv
    LEFT JOIN "Data_Store_OLIDS_Alpha".OLIDS_COMMON.PRACTITIONER_IN_ROLE a
        ON a.lds_record_id = cv."lds_record_id"
    WHERE a.lds_record_id IS NULL
    LIMIT 100
)
UNION ALL
SELECT * FROM (
    SELECT 'PROCEDURE_REQUEST' AS table_name, cv."lds_record_id"
    FROM "Data_Store_OLIDS_Clinical_Validation".OLIDS_COMMON.PROCEDURE_REQUEST cv
    LEFT JOIN "Data_Store_OLIDS_Alpha".OLIDS_COMMON.PROCEDURE_REQUEST a
        ON a.lds_record_id = cv."lds_record_id"
    WHERE a.lds_record_id IS NULL
    LIMIT 100
)
UNION ALL
SELECT * FROM (
    SELECT 'REFERRAL_REQUEST' AS table_name, cv."lds_record_id"
    FROM "Data_Store_OLIDS_Clinical_Validation".OLIDS_COMMON.REFERRAL_REQUEST cv
    LEFT JOIN "Data_Store_OLIDS_Alpha".OLIDS_COMMON.REFERRAL_REQUEST a
        ON a.lds_record_id = cv."lds_record_id"
    WHERE a.lds_record_id IS NULL
    LIMIT 100
)
UNION ALL
SELECT * FROM (
    SELECT 'SCHEDULE' AS table_name, cv."lds_record_id"
    FROM "Data_Store_OLIDS_Clinical_Validation".OLIDS_COMMON.SCHEDULE cv
    LEFT JOIN "Data_Store_OLIDS_Alpha".OLIDS_COMMON.SCHEDULE a
        ON a.lds_record_id = cv."lds_record_id"
    WHERE a.lds_record_id IS NULL
    LIMIT 100
)
UNION ALL
SELECT * FROM (
    SELECT 'SCHEDULE_PRACTITIONER' AS table_name, cv."lds_record_id"
    FROM "Data_Store_OLIDS_Clinical_Validation".OLIDS_COMMON.SCHEDULE_PRACTITIONER cv
    LEFT JOIN "Data_Store_OLIDS_Alpha".OLIDS_COMMON.SCHEDULE_PRACTITIONER a
        ON a.lds_record_id = cv."lds_record_id"
    WHERE a.lds_record_id IS NULL
    LIMIT 100
)
ORDER BY table_name, "lds_record_id";
