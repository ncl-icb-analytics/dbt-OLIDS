--7.8.23 New 64 base with a CTE for EMIS only EGFR codes coalesced with CTE for OTHER where patients can have either an EGFR from EMIS or EGFR from provider
--8.8.23 LOINC and SNOMED codes now combined in the 'EGFR_COD_LCS' cluster, removed POPHEALTH_QOF_LTCS exclusions as duplicated in ICS_LTC_01 and reformatted query to improve clarity
--7.9.23 repoint to ICS_LTC_01
--17.09.23 Diabetes added back in as not in ICS_LTC_01 (type 2 only)

WITH emisbase AS (
  SELECT DISTINCT EMPI_ID, 1 AS 'EMIS'
  FROM (
    SELECT EMPI_ID
    FROM LTC_LCS_BASE
    WHERE AGE >= 17

    EXCEPT

    SELECT EMPI_ID
    FROM ICS_LTC_01
     
    EXCEPT 

    SELECT EMPI_ID
    FROM POPHEALTH_QOF_LTCS_LIST l
    WHERE LTC_NAME = 'Diabetes'

    EXCEPT

    SELECT EMPI_ID
    FROM PH_F_RESULT
    JOIN JOINED_LTC_LOOKUP ON SNOMED_CODE = RESULT_CODE
    WHERE cluster_id IN ('EGFR_COD_LCS')
      AND NORM_NUMERIC_VALUE > 0
      AND DATE(SERVICE_DATE) >= add_months(CURRENT_DATE(), -12)
      AND SOURCE_DESCRIPTION = 'EMIS GP'
  ) emisexclusions
),
otherbase AS (
  SELECT DISTINCT EMPI_ID, 1 AS 'OTHER'
  FROM (
    SELECT EMPI_ID
    FROM LTC_LCS_BASE
    WHERE AGE >= 17

    EXCEPT

    SELECT EMPI_ID
    FROM ICS_LTC_01

    EXCEPT

    SELECT EMPI_ID
    FROM PH_F_RESULT
    JOIN JOINED_LTC_LOOKUP ON SNOMED_CODE = RESULT_CODE
    WHERE cluster_id IN ('EGFR_COD_LCS')
      AND NORM_NUMERIC_VALUE > 0
      AND DATE(SERVICE_DATE) >= add_months(CURRENT_DATE(), -12)
  ) otherexclusions
)

SELECT 
COALESCE(E.empi_id, O.empi_id) AS empi_id,
E.EMIS,
O.OTHER
FROM emisbase AS E
FULL OUTER JOIN otherbase AS O USING (empi_id)