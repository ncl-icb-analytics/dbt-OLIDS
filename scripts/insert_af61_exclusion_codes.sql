-- Insert hardcoded exclusion codes for AF_61 into LTC_LCS_CODES table
INSERT INTO DATA_LAB_NCL_TRAINING_TEMP.CODESETS.LTC_LCS_CODES 
(CLUSTER_ID, CLUSTER_DESCRIPTION, SNOMED_CODE, SNOMED_DESCRIPTION)
VALUES 
('AF61_EXCLUSIONS', 'AF_61 Case Finding Exclusion Conditions', '1119304009', 'Long-haul COVID-19'),
('AF61_EXCLUSIONS', 'AF_61 Case Finding Exclusion Conditions', '62067003', 'Hypoplastic left heart syndrome (disorder)'),
('AF61_EXCLUSIONS', 'AF_61 Case Finding Exclusion Conditions', '132221000119109', 'Deep vein thrombosis'); 