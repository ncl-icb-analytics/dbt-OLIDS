CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PRACTICE_PCN (
    PRACTICE_CODE VARCHAR COMMENT 'GP practice code (ServiceProviderCode)',
    PRACTICE_NAME VARCHAR COMMENT 'GP practice name (ServiceProviderName)',
    PCN_NAME VARCHAR COMMENT 'Primary Care Network name',
    PCN_CODE VARCHAR COMMENT 'Primary Care Network code'
)
COMMENT = 'Dimension table providing current practice to PCN mapping for NCL practices. Sources from Dictionary schema ServiceProvider and OrganisationMatrixPracticeView tables.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
SELECT
    pnl.PRACTICECODE AS PRACTICE_CODE,
    pnl.PRACTICENAME AS PRACTICE_NAME,
    pnl.PCNCODE AS PCN_CODE,
    -- We don't have PCN name in the neighbourhood lookup, so we'll join to get it
    COALESCE(pcn."NetworkName", 'Unknown PCN') AS PCN_NAME
FROM DATA_LAB_NCL_TRAINING_TEMP.POPULATION_HEALTH.PRACTICE_NEIGHBOURHOOD_LOOKUP pnl
LEFT JOIN "Dictionary"."dbo"."OrganisationMatrixPracticeView" pcn
    ON pnl.PRACTICECODE = pcn."PracticeCode"
WHERE pnl.PCNCODE IS NOT NULL;
