--20.8.24 new query for HTN66B using external BP results table (all sources) LTC_LATEST_BP_RESULTS - NOTE clusters are different to the other HTN indicators
-- Creating a population from base that excludes the higher priority patients in HTN_61-65 (64 does not exist) and includes AGE
-- This removes all patients with clinic BP >160/100 or home BP >150/95
WITH pop AS (
    SELECT
        a.*,
        b.age
    FROM icb_cf_htn_61_base AS a
    LEFT JOIN ltc_lcs_base AS b ON a.empi_id = b.empi_id
    LEFT JOIN icb_cf_htn_61_gp_1 AS d ON a.empi_id = d.empi_id
    LEFT JOIN icb_cf_htn_62_gp_2a AS e ON a.empi_id = e.empi_id
    LEFT JOIN icb_cf_htn_63_gp_2b AS f ON a.empi_id = f.empi_id
    LEFT JOIN icb_cf_htn_65_gp_3a AS g ON a.empi_id = g.empi_id
    WHERE
        d.empi_id IS NULL
        AND e.empi_id IS NULL
        AND f.empi_id IS NULL
        AND g.empi_id IS NULL
),

--CREATING CTE FOR EMIS SOURCED
emis AS (
    SELECT DISTINCT
        empi_id,
        1 AS 'EMIS'
    FROM
        (

            ------- Rule 7 - limiting to people with clinic BP more than 140/90 or home BP more than 135/85

            SELECT * FROM ( --r7

                SELECT pop.empi_id FROM pop
                --join LTC_LCS_BASE b using (EMPI_ID)
                INNER JOIN ltc_latest_bp_results AS c ON pop.empi_id = c.empi_id
                WHERE
                    c.cluster_id IN ('SYSBP_COD')
                    AND c.norm_numeric_value >= 140
                    AND c.source_description = 'EMIS GP'
                    AND pop.emis = '1'

                UNION

                SELECT pop.empi_id FROM pop
                --join LTC_LCS_BASE b using (EMPI_ID)
                INNER JOIN ltc_latest_bp_results AS c ON pop.empi_id = c.empi_id
                WHERE
                    c.cluster_id IN ('DIASBP_COD')
                    AND c.norm_numeric_value >= 90
                    AND c.source_description = 'EMIS GP'
                    AND pop.emis = '1'

                UNION

                SELECT pop.empi_id FROM pop
                --join LTC_LCS_BASE b using (EMPI_ID)
                INNER JOIN ltc_latest_bp_results AS c ON pop.empi_id = c.empi_id
                WHERE
                    c.cluster_id IN ('BP_AMB_HOM_SYS')
                    AND c.norm_numeric_value >= 135
                    AND c.source_description = 'EMIS GP'
                    AND pop.emis = '1'

                UNION

                SELECT pop.empi_id FROM pop
                --join LTC_LCS_BASE b using (EMPI_ID)
                INNER JOIN ltc_latest_bp_results AS c ON pop.empi_id = c.empi_id
                WHERE
                    c.cluster_id IN ('BP_AMB_HOM_DIA')
                    AND c.norm_numeric_value >= 85
                    AND c.source_description = 'EMIS GP'
                    AND pop.emis = '1'

            ) AS r7

            ------- Rule 5 - Excluding Over 80 with clinic BP less than 150/90
            EXCEPT

            SELECT * FROM ( --r5

                SELECT pop.empi_id FROM pop
                --join LTC_LCS_BASE b using (EMPI_ID)
                INNER JOIN ltc_latest_bp_results AS c ON pop.empi_id = c.empi_id
                WHERE
                    age >= 80
                    AND c.cluster_id IN ('SYSBP_COD')
                    AND c.norm_numeric_value < 150
                    AND c.source_description = 'EMIS GP'
                    AND pop.emis = '1'

                UNION

                SELECT pop.empi_id FROM pop
                --join LTC_LCS_BASE b using (EMPI_ID)
                INNER JOIN ltc_latest_bp_results AS c ON pop.empi_id = c.empi_id
                WHERE
                    age >= 80
                    AND c.cluster_id IN ('DIASBP_COD')
                    AND c.norm_numeric_value < 90
                    AND c.source_description = 'EMIS GP'
                    AND pop.emis = '1'

            ) AS r5

            ------- Rule 6 - Excluding Over 80 with home BP less than 145/85
            EXCEPT

            SELECT * FROM ( --r6

                SELECT pop.empi_id FROM pop
                --join LTC_LCS_BASE b using (EMPI_ID)
                INNER JOIN ltc_latest_bp_results AS c ON pop.empi_id = c.empi_id
                WHERE
                    age >= 80
                    AND c.cluster_id IN ('BP_AMB_HOM_SYS')
                    AND c.norm_numeric_value < 145
                    AND c.source_description = 'EMIS GP'
                    AND pop.emis = '1'

                UNION

                SELECT pop.empi_id FROM pop
                --join LTC_LCS_BASE b using (EMPI_ID)
                INNER JOIN ltc_latest_bp_results AS c ON pop.empi_id = c.empi_id
                WHERE
                    age >= 80
                    AND c.cluster_id IN ('BP_AMB_HOM_DIA')
                    AND c.norm_numeric_value < 85
                    AND c.source_description = 'EMIS GP'
                    AND pop.emis = '1'
            ) AS r6
        ) AS sub
), --emis cte

-- CREATING CTE FOR 'OTHER' SOURCED
other AS (
    SELECT DISTINCT
        empi_id,
        1 AS 'OTHER'
    FROM
        (

            ------- Rule 7 - limiting to people with clinic BP more than 140/90 or home BP more than 135/85

            SELECT * FROM ( --r7

                SELECT pop.empi_id FROM pop
                --join LTC_LCS_BASE b using (EMPI_ID)
                INNER JOIN ltc_latest_bp_results AS c ON pop.empi_id = c.empi_id
                WHERE
                    c.cluster_id IN ('SYSBP_COD')
                    AND c.norm_numeric_value >= 140

                UNION

                SELECT pop.empi_id FROM pop
                --join LTC_LCS_BASE b using (EMPI_ID)
                INNER JOIN ltc_latest_bp_results AS c ON pop.empi_id = c.empi_id
                WHERE
                    c.cluster_id IN ('DIASBP_COD')
                    AND c.norm_numeric_value >= 90

                UNION

                SELECT pop.empi_id FROM pop
                --join LTC_LCS_BASE b using (EMPI_ID)
                INNER JOIN ltc_latest_bp_results AS c ON pop.empi_id = c.empi_id
                WHERE
                    c.cluster_id IN ('BP_AMB_HOM_SYS')
                    AND c.norm_numeric_value >= 135

                UNION

                SELECT pop.empi_id FROM pop
                --join LTC_LCS_BASE b using (EMPI_ID)
                INNER JOIN ltc_latest_bp_results AS c ON pop.empi_id = c.empi_id
                WHERE
                    c.cluster_id IN ('BP_AMB_HOM_DIA')
                    AND c.norm_numeric_value >= 85
            ) AS r7

            ------- Rule 5 - Excluding Over 80 with clinic BP less than 150/90
            EXCEPT

            SELECT * FROM ( --r5

                SELECT pop.empi_id FROM pop
                --join LTC_LCS_BASE b using (EMPI_ID)
                INNER JOIN ltc_latest_bp_results AS c ON pop.empi_id = c.empi_id
                WHERE
                    age >= 80
                    AND c.cluster_id IN ('SYSBP_COD')
                    AND c.norm_numeric_value < 150

                UNION

                SELECT pop.empi_id FROM pop
                --join LTC_LCS_BASE b using (EMPI_ID)
                INNER JOIN ltc_latest_bp_results AS c ON pop.empi_id = c.empi_id
                WHERE
                    age >= 80
                    AND c.cluster_id IN ('DIASBP_COD')
                    AND c.norm_numeric_value < 90
            ) AS r5

            ------- Rule 6 - Excluding Over 80 with home BP less than 145/85
            EXCEPT

            SELECT * FROM ( --r6

                SELECT pop.empi_id FROM pop
                --join LTC_LCS_BASE b using (EMPI_ID)
                INNER JOIN ltc_latest_bp_results AS c ON pop.empi_id = c.empi_id
                WHERE
                    age >= 80
                    AND c.cluster_id IN ('BP_AMB_HOM_SYS')
                    AND c.norm_numeric_value < 145

                UNION

                SELECT pop.empi_id FROM pop
                --join LTC_LCS_BASE b using (EMPI_ID)
                INNER JOIN ltc_latest_bp_results AS c ON pop.empi_id = c.empi_id
                WHERE
                    age >= 80
                    AND c.cluster_id IN ('BP_AMB_HOM_DIA')
                    AND c.norm_numeric_value < 85
            ) AS r6
        ) AS sub
) -- other cte
-- USING 'OTHER' TABLE TO GET WHOLE COHORT AND LEFT JOINING TO SEE WHETHER THEY ARE EMIS SOURCED OR NOT

SELECT
    COALESCE(e.empi_id, o.empi_id) AS empi_id,
    CASE WHEN e.emis = 1 THEN 'EMIS' ELSE 'Other' END AS source
FROM emis AS e
FULL OUTER JOIN other AS o ON e.empi_id = o.empi_id
