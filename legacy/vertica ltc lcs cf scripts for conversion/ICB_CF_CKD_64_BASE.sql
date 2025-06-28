--7.8.23 New 64 base with a CTE for EMIS only EGFR codes coalesced with CTE for OTHER where patients can have either an EGFR from EMIS or EGFR from provider
--8.8.23 LOINC and SNOMED codes now combined in the 'EGFR_COD_LCS' cluster, removed POPHEALTH_QOF_LTCS exclusions as duplicated in ICS_LTC_01 and reformatted query to improve clarity
--7.9.23 repoint to ICS_LTC_01
--17.09.23 Diabetes added back in as not in ICS_LTC_01 (type 2 only)

WITH emisbase AS (
    SELECT DISTINCT
        empi_id,
        1 AS 'EMIS'
    FROM (
        SELECT empi_id
        FROM ltc_lcs_base
        WHERE age >= 17

        EXCEPT

        SELECT empi_id
        FROM ics_ltc_01

        EXCEPT

        SELECT empi_id
        FROM pophealth_qof_ltcs_list
        WHERE ltc_name = 'Diabetes'

        EXCEPT

        SELECT empi_id
        FROM ph_f_result
        INNER JOIN joined_ltc_lookup ON snomed_code = result_code
        WHERE
            cluster_id IN ('EGFR_COD_LCS')
            AND norm_numeric_value > 0
            AND DATE(service_date) >= ADD_MONTHS(CURRENT_DATE(), -12)
            AND source_description = 'EMIS GP'
    ) AS emisexclusions
),

otherbase AS (
    SELECT DISTINCT
        empi_id,
        1 AS 'OTHER'
    FROM (
        SELECT empi_id
        FROM ltc_lcs_base
        WHERE age >= 17

        EXCEPT

        SELECT empi_id
        FROM ics_ltc_01

        EXCEPT

        SELECT empi_id
        FROM ph_f_result
        INNER JOIN joined_ltc_lookup ON snomed_code = result_code
        WHERE
            cluster_id IN ('EGFR_COD_LCS')
            AND norm_numeric_value > 0
            AND DATE(service_date) >= ADD_MONTHS(CURRENT_DATE(), -12)
    ) AS otherexclusions
)

SELECT
    e.emis,
    o.other,
    COALESCE(e.empi_id, o.empi_id) AS empi_id
FROM emisbase AS e
FULL OUTER JOIN otherbase AS o ON e.empi_id = o.empi_id
