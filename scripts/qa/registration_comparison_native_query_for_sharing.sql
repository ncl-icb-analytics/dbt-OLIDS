/*
Registration Count Comparison - PDS vs OLIDS
Compares active registrations between PDS registry and OLIDS episode of care data.
Target Date: CURRENT_DATE()
ICB: NHS North Central London (93C)

Purpose: Identify discrepancies in registration counts to investigate data quality issues
*/

-- =================================================================================================
-- PDS REGISTRATIONS (Gold Standard)
-- =================================================================================================
WITH pds_registrations AS (
    SELECT
        "REG"."Primary Care Provider" AS practice_code,
        Prac."Organisation_Name" AS practice_name,
        ICB."Organisation_Name" AS icb_name,
        COUNT(*) AS pds_unmerged_persons,
        COUNT(DISTINCT COALESCE("MERG"."Pseudo Superseded NHS Number", "REG"."Pseudo NHS Number")) AS pds_registered_patients
    FROM "Data_Store_Registries"."pds"."PDS_Patient_Care_Practice" "REG"

    -- Person merger handling
    LEFT JOIN "Data_Store_Registries"."pds"."PDS_Person_Merger" "MERG"
        ON "REG"."Pseudo NHS Number" = "MERG"."Pseudo NHS Number"

    -- Person details with temporal validity
    LEFT JOIN "Data_Store_Registries"."pds"."PDS_Person" "PER"
        ON "REG"."Pseudo NHS Number" = "PER"."Pseudo NHS Number"
        AND "PER"."Person Business Effective From Date" <= COALESCE("REG"."Primary Care Provider Business Effective To Date", '9999-12-31')
        AND COALESCE("PER"."Person Business Effective To Date", '9999-12-31') >= "REG"."Primary Care Provider Business Effective From Date"
        AND CURRENT_DATE() BETWEEN
            "PER"."Person Business Effective From Date"
            AND COALESCE("PER"."Person Business Effective To Date", '9999-12-31')

    -- Reason for removal with temporal validity
    LEFT JOIN "Data_Store_Registries"."pds"."PDS_Reason_For_Removal" "REAS"
        ON "REG"."Pseudo NHS Number" = "REAS"."Pseudo NHS Number"
        AND "REAS"."Reason for Removal Business Effective From Date" <= COALESCE("REG"."Primary Care Provider Business Effective To Date", '9999-12-31')
        AND COALESCE("REAS"."Reason for Removal Business Effective To Date", '9999-12-31') >= "REG"."Primary Care Provider Business Effective From Date"
        AND CURRENT_DATE() BETWEEN
            "REAS"."Reason for Removal Business Effective From Date"
            AND COALESCE("REAS"."Reason for Removal Business Effective To Date", '9999-12-31')

    -- Practice and ICB lookup
    INNER JOIN "Dictionary"."dbo"."Organisation" Prac
        ON "REG"."Primary Care Provider" = Prac."Organisation_Code"
    INNER JOIN "Dictionary"."dbo"."Organisation" ICB
        ON Prac."SK_ParentOrg_ID" = ICB."SK_OrganisationID"
        AND ICB."Organisation_Code" = '93C'  -- NCL ICB only
        AND Prac."EndDate" IS NULL

    WHERE "PER"."Death Status" IS NULL
        AND "PER"."Date of Death" IS NULL
        AND "REG"."Pseudo NHS Number" IS NOT NULL
        AND CURRENT_DATE() BETWEEN
            "REG"."Primary Care Provider Business Effective From Date"
            AND COALESCE("REG"."Primary Care Provider Business Effective To Date", '9999-12-31')
        AND "REAS"."Reason for Removal" IS NULL

    GROUP BY
        "REG"."Primary Care Provider",
        Prac."Organisation_Name",
        ICB."Organisation_Name"
),

-- =================================================================================================
-- OLIDS REGISTRATIONS (Episode of Care Method)
-- =================================================================================================
olids_patient_death AS (
    SELECT
        id AS patient_id,
        death_year,
        death_month,
        death_year IS NOT NULL AS is_deceased,
        CASE
            WHEN death_year IS NOT NULL AND death_month IS NOT NULL
                THEN DATEADD(
                    DAY,
                    FLOOR(DAY(LAST_DAY(TO_DATE(death_year || '-' || death_month || '-01'))) / 2),
                    TO_DATE(death_year || '-' || death_month || '-01')
                )
        END AS death_date_approx
    FROM "Data_Store_OLIDS_Alpha".OLIDS_MASKED.PATIENT
),

olids_registration_type AS (
    -- Get the concept ID for "Registration type" episode
    SELECT DISTINCT c.id AS concept_id
    FROM "Data_Store_OLIDS_Alpha".OLIDS_TERMINOLOGY.CONCEPT c
    WHERE c.code = '24531000000104'
        AND c.display = 'Registration type'
),

olids_active_episodes AS (
    SELECT
        eoc.patient_id,
        eoc.organisation_id,
        eoc.episode_of_care_start_date,
        eoc.id,
        p.sk_patient_id
    FROM "Data_Store_OLIDS_Alpha".OLIDS_COMMON.EPISODE_OF_CARE eoc

    INNER JOIN "Data_Store_OLIDS_Alpha".OLIDS_MASKED.PATIENT p
        ON eoc.patient_id = p.id

    LEFT JOIN olids_patient_death pdd
        ON eoc.patient_id = pdd.patient_id

    -- Filter to Registration type episodes only
    LEFT JOIN "Data_Store_OLIDS_Alpha".OLIDS_TERMINOLOGY.CONCEPT_MAP cm
        ON eoc.episode_type_source_concept_id = cm.source_code_id
    INNER JOIN olids_registration_type ort
        ON cm.target_code_id = ort.concept_id

    WHERE eoc.episode_of_care_start_date IS NOT NULL
        AND eoc.patient_id IS NOT NULL
        AND eoc.organisation_id IS NOT NULL
        AND p.sk_patient_id IS NOT NULL

        -- Episode active today
        AND eoc.episode_of_care_start_date <= CURRENT_DATE()
        AND (
            eoc.episode_of_care_end_date IS NULL
            OR eoc.episode_of_care_end_date > CURRENT_DATE()
        )

        -- Patient not deceased, or deceased after today
        AND (
            NOT pdd.is_deceased
            OR pdd.death_date_approx IS NULL
            OR pdd.death_date_approx > CURRENT_DATE()
        )

    -- Deduplicate: one registration per patient per practice
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY p.sk_patient_id, eoc.organisation_id
        ORDER BY eoc.episode_of_care_start_date DESC, eoc.id
    ) = 1
),

olids_registrations AS (
    SELECT
        o.organisation_code AS practice_code,
        o.name AS practice_name,
        COUNT(DISTINCT ae.sk_patient_id) AS olids_registered_patients
    FROM olids_active_episodes ae
    INNER JOIN "Data_Store_OLIDS_Alpha".OLIDS_COMMON.ORGANISATION o
        ON ae.organisation_id = o.id
    WHERE o.organisation_code IS NOT NULL
    GROUP BY o.organisation_code, o.name
),

-- =================================================================================================
-- COMPARISON
-- =================================================================================================
comparison AS (
    SELECT
        COALESCE(pds.practice_code, olids.practice_code) AS practice_code,
        COALESCE(pds.practice_name, olids.practice_name) AS practice_name,
        pds.icb_name,
        pds.pds_registered_patients,
        olids.olids_registered_patients,
        olids.olids_registered_patients - pds.pds_registered_patients AS difference,
        CASE
            WHEN pds.pds_registered_patients > 0
                THEN ROUND(100.0 * (olids.olids_registered_patients - pds.pds_registered_patients) / pds.pds_registered_patients, 2)
            ELSE NULL
        END AS diff_percentage,
        CASE
            WHEN pds.pds_registered_patients IS NULL THEN 'OLIDS Only'
            WHEN olids.olids_registered_patients IS NULL THEN 'PDS Only'
            WHEN ABS(100.0 * (olids.olids_registered_patients - pds.pds_registered_patients) / pds.pds_registered_patients) >= 20 THEN 'Major Difference'
            WHEN ABS(100.0 * (olids.olids_registered_patients - pds.pds_registered_patients) / pds.pds_registered_patients) >= 5 THEN 'Minor Difference'
            ELSE 'Good Match'
        END AS match_category
    FROM pds_registrations pds
    FULL OUTER JOIN olids_registrations olids
        ON pds.practice_code = olids.practice_code
)

-- =================================================================================================
-- RESULTS
-- =================================================================================================
SELECT
    practice_code,
    practice_name,
    match_category,
    pds_registered_patients AS pds_count,
    olids_registered_patients AS olids_count,
    difference AS diff_count,
    diff_percentage AS diff_pct
FROM comparison
ORDER BY
    CASE match_category
        WHEN 'Major Difference' THEN 1
        WHEN 'Minor Difference' THEN 2
        WHEN 'Good Match' THEN 3
        WHEN 'PDS Only' THEN 4
        WHEN 'OLIDS Only' THEN 5
    END,
    ABS(difference) DESC NULLS LAST;

-- Summary Statistics
-- SELECT
--     match_category,
--     COUNT(*) AS practice_count,
--     SUM(pds_registered_patients) AS total_pds,
--     SUM(olids_registered_patients) AS total_olids,
--     SUM(difference) AS total_diff
-- FROM comparison
-- GROUP BY match_category
-- ORDER BY
--     CASE match_category
--         WHEN 'Major Difference' THEN 1
--         WHEN 'Minor Difference' THEN 2
--         WHEN 'Good Match' THEN 3
--         WHEN 'PDS Only' THEN 4
--         WHEN 'OLIDS Only' THEN 5
--     END;
