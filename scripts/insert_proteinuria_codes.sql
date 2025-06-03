-- Insert proteinuria codes into LTC_LCS_CODES
INSERT INTO DATA_LAB_NCL_TRAINING_TEMP.CODESETS.LTC_LCS_CODES (
    CLUSTER_ID,
    CLUSTER_DESCRIPTION,
    SNOMED_CODE,
    SNOMED_DESCRIPTION
)
VALUES
    ('PROTEINURIA_FINDINGS', 'Proteinuria findings and related conditions', '29738008', 'Proteinuria (finding)'),
    ('PROTEINURIA_FINDINGS', 'Proteinuria findings and related conditions', '439033000', 'AD7c neuronal thread protein concentration in urine above reference range (finding)'),
    ('PROTEINURIA_FINDINGS', 'Proteinuria findings and related conditions', '87865005', 'Adventitious proteinuria (finding)'),
    ('PROTEINURIA_FINDINGS', 'Proteinuria findings and related conditions', '274769005', 'Albuminuria (finding)'),
    ('PROTEINURIA_FINDINGS', 'Proteinuria findings and related conditions', '236720004', 'Asymptomatic proteinuria (finding)'),
    ('PROTEINURIA_FINDINGS', 'Proteinuria findings and related conditions', '274771005', 'Bence-Jones proteinuria (finding)'),
    ('PROTEINURIA_FINDINGS', 'Proteinuria findings and related conditions', '35727008', 'Cardiac proteinuria (finding)'),
    ('PROTEINURIA_FINDINGS', 'Proteinuria findings and related conditions', '8875000', 'Colliquative proteinuria (finding)'),
    ('PROTEINURIA_FINDINGS', 'Proteinuria findings and related conditions', '324984006', 'Dietetic proteinuria (finding)'),
    ('PROTEINURIA_FINDINGS', 'Proteinuria findings and related conditions', '73883007', 'Emulsion proteinuria (finding)'),
    ('PROTEINURIA_FINDINGS', 'Proteinuria findings and related conditions', '55662002', 'Enterogenic proteinuria (finding)'),
    ('PROTEINURIA_FINDINGS', 'Proteinuria findings and related conditions', '77250007', 'Essential proteinuria (finding)'),
    ('PROTEINURIA_FINDINGS', 'Proteinuria findings and related conditions', '8022000', 'Exercise proteinuria (finding)'),
    ('PROTEINURIA_FINDINGS', 'Proteinuria findings and related conditions', '23891001', 'Febrile proteinuria (finding)'),
    ('PROTEINURIA_FINDINGS', 'Proteinuria findings and related conditions', '57009009', 'Functional proteinuria (finding)'),
    ('PROTEINURIA_FINDINGS', 'Proteinuria findings and related conditions', '34165000', 'Gestational proteinuria (disorder)'),
    ('PROTEINURIA_FINDINGS', 'Proteinuria findings and related conditions', '42827006', 'Globular proteinuria (finding)'),
    ('PROTEINURIA_FINDINGS', 'Proteinuria findings and related conditions', '2740001', 'Gouty proteinuria (finding)'),
    ('PROTEINURIA_FINDINGS', 'Proteinuria findings and related conditions', '22794007', 'Hematogenous proteinuria (finding)'),
    ('PROTEINURIA_FINDINGS', 'Proteinuria findings and related conditions', '12491000132101', 'Isolated proteinuria (finding)'),
    ('PROTEINURIA_FINDINGS', 'Proteinuria findings and related conditions', '48160003', 'Lordotic proteinuria (finding)'),
    ('PROTEINURIA_FINDINGS', 'Proteinuria findings and related conditions', '263808002', 'Microproteinuria (finding)'),
    ('PROTEINURIA_FINDINGS', 'Proteinuria findings and related conditions', '34993002', 'Mixed proteinuria (finding)'),
    ('PROTEINURIA_FINDINGS', 'Proteinuria findings and related conditions', '12178007', 'Nephrogenous proteinuria (finding)'),
    ('PROTEINURIA_FINDINGS', 'Proteinuria findings and related conditions', '264867001', 'Nephrotic range proteinuria (finding)'),
    ('PROTEINURIA_FINDINGS', 'Proteinuria findings and related conditions', '2657005', 'Overflow proteinuria (finding)'),
    ('PROTEINURIA_FINDINGS', 'Proteinuria findings and related conditions', '2663001', 'Palpatory proteinuria (finding)'),
    ('PROTEINURIA_FINDINGS', 'Proteinuria findings and related conditions', '12511000132108', 'Persistent proteinuria (finding)'),
    ('PROTEINURIA_FINDINGS', 'Proteinuria findings and related conditions', '43820004', 'Postrenal proteinuria (finding)'),
    ('PROTEINURIA_FINDINGS', 'Proteinuria findings and related conditions', '73630001', 'Prerenal proteinuria (finding)');

-- Update the notepad to include the new cluster ID
-- Note: This is a reminder to add PROTEINURIA_FINDINGS to the Available Cluster IDs section
-- in docs/ltc_lcs_migration_notes.md 