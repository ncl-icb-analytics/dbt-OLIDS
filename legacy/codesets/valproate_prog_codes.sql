-- Consolidated lookup table for codes related to the Valproate Program.
CREATE OR REPLACE TABLE DATA_LAB_OLIDS_UAT.REFERENCE.VALPROATE_PROG_CODES (
    CODE VARCHAR COMMENT 'The medical code (e.g., SNOMED CT, ICD-10). Primary Key (potentially combined with category if codes overlap).',
    CODE_CATEGORY VARCHAR COMMENT 'Category identifying the purpose of the code (e.g., \'ARAF Form\', \'Perm Absence Preg Risk\', \'Psychiatry\', \'ARAF Referral\').',
    LOOKBACK_YEARS_OFFSET NUMBER COMMENT 'Offset in years for determining relevant record window. Primarily used for \'ARAF Form\' category; NULL otherwise.'
    -- Consider adding a primary key constraint if appropriate, e.g., PRIMARY KEY (CODE, CODE_CATEGORY)
)
COMMENT = 'Stores various medical codes relevant to the Valproate safety program, categorized by their purpose (ARAF Form, Pregnancy Risk Absence, Psychiatry, ARAF Referral).';
