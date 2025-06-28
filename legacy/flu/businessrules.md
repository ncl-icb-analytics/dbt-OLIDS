2. Dataset Specification
   2.1 Date definitions
   RUN_DAT is defined as:
   The date of data extraction
   REF_DAT is defined as: 31/03/2025
   START_DAT is defined as 01/09/2024
   CHILD_DAT is defined at 31/08/2024
   AUDITEND_DAT is defined as:
   •
   31/10/2024 for the October submission in November 24
   •
   30/11/2024 for the November submission in December 24
   •
   31/12/2024 for the December submission in January 25
   •
   31/01/2025 for the January submission in February 25
   •
   28/02/2025 for the February submission in March 25
   2.2 Patient selection criteria
   a) Registration status:
   Current registration status Qualifying criteria
   Currently registered for GMS
   Most recent registration date ≤ (RUN_DAT)
   b) Search population
   Action Criteria
   Include
   All patients aged ≥ 65 years
   Patients aged ≥ 6 months AND <65 years AND in at least one core clinical At Risk disease category
   Patients with Morbid Obesity (BMI ≥40) aged ≥ 18 years AND <65 years
   Patients who are already pregnant on 1st September 2024 or become pregnant between 1st September 2024 and 28th February 2025 (inclusive)
   Carers aged ≥5 and <65 years
   Health & Social Care Worker aged ≥16 AND <65 years
   Household contact of immunosuppressed ≥ 6 months
   Long stay residential care patient aged ≥ 6 months
   Homeless patients aged ≥16 yrs
   Patients with a Learning Disability aged ≥ 6 months

c) Clinical Code Groups
No. Code Group Name Code Group Description
1.
AST_COD
Asthma diagnosis codes
2.
ASTMED_COD
Asthma inhaled or oral steroid use (administration) codes
3.
ASTRX_COD
Asthma inhaled corticosteroid codes (dm+d)
4.
ASTADM_COD
Asthma admission codes
5.
RESP_COD
Chronic respiratory disease diagnosis codes
6.
CHD_COD
Chronic heart disease codes diagnosis codes
7.
CKD_COD
Chronic kidney disease diagnosis codes
8.
CKD15_COD
Chronic kidney disease codes – all stages
9.
CKD35_COD
Chronic kidney disease codes - stages 3 – 5
10.
CLD_COD
Chronic liver disease diagnosis codes
11.
DIAB_COD
Diabetes diagnosis codes
12.
DMRES_COD
Diabetes resolved codes
13.
IMMDX_COD
Immunosuppression diagnosis codes
14.
IMMRX_COD
Immunosuppression medication codes (dm+d)
15.
IMMADM_COD
Immunosuppression admin codes
16.
DXT_CHEMO_COD
Chemotherapy or radiotherapy codes
17.
CNSGROUP_COD
Chronic neurological disease diagnosis codes (including Stroke/TIA, Cerebral Palsy and MS)
18.
PNSPLEEN_COD
Asplenia or dysfunction of the spleen codes
19.
BMI_COD
BMI codes
20.
BMI_STAGE_COD
BMI stage codes
21.
SEV_OBESITY_COD
Severe Obesity codes
22.
ADDIS_COD
Addison’s disease & Pan-hypopituitary diagnosis codes
23.
SEV_LRNDIS_COD
Severe Learning Disability codes
24.
LEARNDIS_COD
Learning Disability codes
25.
HHLD_IMDEF_COD
Household contact of immunodeficient individual
26.
HOMELESS_COD
Homeless codes
27.
RESIDE_COD
Residence codes
28.
LONGRES_COD
Longterm residential care codes
29.
FLUVAX_COD
Influenza vaccination administration codes
30.
FLURX_COD
Influenza vaccination medication codes (dm+d)
31.
FLUVAXOHP_COD
Influenza vaccination by other health care provider codes
32.
FLUVXOPCHILD_COD
Influenza vaccination by other health care provider codes (children only)
33.
LAIVRX_COD
Live attenuated influenza vaccine medication codes (Nasal Fluenz Tetra) (dm+d)
34.
AQIVRX_COD
Adjuvanted Quadrivalent vaccination medication codes (FLUAD tetra) (dm+d)
35.
QIVCRX_COD
Quadrivalent cell grown influenza vaccination medication codes (Flucelvax Tetra) (dm+d)
36.
QIVERX_COD
Quadrivalent egg grown vaccination medication codes (Masta, (Mylan)Viatris Influvac Tetra, Sanofi) (dm+d)
37.
QIVHDRX_COD
High dose quadrivalent flu vaccination medication codes (dm+d)
38.
LAIV_COD
Administration of intranasal influenza vaccination codes
39.
FLUPHARM_COD
Influenza vaccination by pharmacist codes
40.
SCHOOLVAC_COD
Influenza vaccination given at school codes
41.
MWIFEVAC_COD
Influenza vaccination given by Midwife codes
42.
CONTRA_COD
Influenza vaccination contraindication or intolerance codes
43.
DECL_COD
Influenza vaccination declined codes
44.
NOCONS_COD
No consent for influenza vaccination codes
45.
ALLERG_COD
Influenza vaccination allergy codes
46.
CARER_COD
Carer codes
47.
NOTCARER_COD
No longer a carer codes
48.
PREGDEL_COD
Pregnancy or delivery codes
49.
PREG_COD
Pregnancy (only) codes
50.
CAREHOME_COD
Employed by care home codes
51.
NURSEHOME_COD
Employed by nursing home codes
52.
DOMCARE_COD
Employed by domiciliary care provider codes
53.
ETH_CENSUS_COD
All 2001 or 2011 census ethnicity codes
54.
ETH_WHIBRIT_COD
White - British ethnicity census codes
55.
ETH_WHIIRISH_COD
White - Irish ethnicity census codes
56.
ETH_WHIOTHER_COD
White - Any other White background ethnicity census codes
57.
ETH_MXDWHIBLKCAR_COD
Mixed – White and Black Caribbean ethnicity census codes
58.
ETH_MXDWHIBLKAFR_COD
Mixed – White and Black African ethnicity census codes
59.
ETH_MXDWHIASN_COD
Mixed – White and Asian ethnicity census codes
60.
ETH_MXDOTHER_COD
Mixed – Any other mixed background ethnicity census codes
61.
ETH_ASNINDIAN_COD
Asian or Asian British – Indian ethnicity census codes
62.
ETH_ASNPAK_COD
Asian or Asian British – Pakistani ethnicity census codes
63.
ETH_ASNBANG_COD
Asian or Asian British – Bangladeshi ethnicity census codes
64.
ETH_ASNOTHER_COD
Asian or Asian British – Any other Asian background ethnicity census codes
65.
ETH_BLKCARIB_COD
Black or Black British – Caribbean ethnicity census codes
66.
ETH_BLKAFRIC_COD
Black or Black British – African ethnicity census codes
67.
ETH_BLKOTH_COD
Black or Black British – Any other Black background ethnicity census codes
68.
ETH_CHINESE_COD
Other ethnic groups – Chinese Ethnicity census codes
69.
ETH_OTHER_COD
Other ethnic groups – Any other ethnic group ethnicity census codes
70.
ETH_UNMAPPABLE_COD
Any other ethnicity codes (all non 2001 or 2011 census codes)
71.
ETH_NOTGIVPTREF_COD
Ethnicity not given – patient refused
72.
ETH_NOTSTATED_COD
Ethnicity not stated codes
73.
ETH_NORECORD_COD
Ethnicity not recorded

3 Ethnic Groups No. Rule True False
1
IF ETH_CENSUS_DAT = ETH_WHIBRIT_DAT
White – British
Next
2
IF ETH_CENSUS_DAT= ETH_WHIIRISH_DAT
White – Irish
Next
3
IF ETH_CENSUS_DAT= ETH_WHIOTHER_DAT
White – Any other White background
Next
4
IF ETH_CENSUS_DAT= ETH_MXDWHIBLKCAR_DAT
Mixed – White and Black Caribbean
Next
5
IF ETH_CENSUS_DAT= ETH_MXDWHIBLKAFR_DAT
Mixed – White and Black African
Next
6
IF ETH_CENSUS_DAT= ETH_MXDWHIASN_DAT
Mixed – White and Asian
Next
7
IF ETH_CENSUS_DAT= ETH_MXDOTHER_DAT
Mixed – Any other mixed background
Next
8
IF ETH_CENSUS_DAT= ETH_ASNINDIAN_DAT
Asian or Asian British – Indian
Next
9
IF ETH_CENSUS_DAT= ETH_ASNPAK_DAT
Asian or Asian British – Pakistani
Next
10
IF ETH_CENSUS_DAT= ETH_ASNBANG_DAT
Asian or Asian British – Bangladeshi
Next
11
IF ETH_CENSUS_DAT= ETH_ASNOTHER_DAT
Asian or Asian British – Any other Asian background
Next
12
IF ETH_CENSUS_DAT= ETH_BLKCARIB_DAT
Black or Black British – Caribbean
Next
13
IF ETH_CENSUS_DAT= ETH_BLKAFRIC_DAT
Black or Black British – African
Next
14
IF ETH_CENSUS_DAT= ETH_BLKOTH_DAT
Black or Black British – Any other Black background
Next
15
IF ETH_CENSUS_DAT= ETH_CHINESE_DAT
Other ethnic groups – Chinese
Next
16
IF ETH_CENSUS_DAT= ETH_OTHER_DAT
Other ethnic groups – Any other ethnic group
Next
17
IF ETH_CENSUS_DAT= NULL
AND
ETH_UNMAPPABLE_DAT <> NULL
Patients with any other ethnicity code
Next
18
IF ETH_CENSUS_DAT= NULL
AND
IF ETH_UNMAPPABLE_DAT= NULL AND
IF ETH_NOTGIVPTREF_DAT <> NULL
Ethnicity not given – patient refused
Next
19
IF ETH_CENSUS_DAT= NULL
AND
IF ETH_UNMAPPABLE_DAT= NULL AND
IF ETH_NOTGIVPTREF_DAT= NULL AND
IF ETH_NOTSTATED_DAT <> NULL
Ethnicity not stated
Next
20
IF ETH_CENSUS_DAT= NULL
AND
IF ETH_UNMAPPABLE_DAT= NULL AND
IF ETH_NOTGIVPTREF_DAT= NULL AND
IF ETH_NOTSTATED_DAT = NULL
AND
IF ETH_NORECORD_COD <> NULL
Ethnicity not recorded*
Ethnicity not recorded*

*Note: Band 20 represents “Ethnicity not recorded”. The rules will allocate a patient to this band if they have no ethnicity code at all, or if they only have an “Ethnicity not recorded” code on their record.
The above rules ensure that if a patient has given their ethnicity, then at a later date, has either a “refused”, “not stated” or “not known” ethnicity code, the earlier ethnicity code will be used.

Patients with Immunosuppression True False 4
IMMUNO_GROUP
IF IMMDX_DAT <> NULL
Select
Next
IF IMMRX_DAT <> NULL
Select
Next
IF IMMADM_DAT <> NULL
Select
Next
IF DXT_CHEMO_DAT <> NULL
Select
Reject
Note: The patient can have either a diagnosis code (ever), a recent prescription code or code indicating recent chemotherapy or a recent entry stating patient is immunosuppressed to be included in the At Risk Group
Patients with CKD True False 5
CKD_GROUP
IF CKD_DAT <> NULL (diagnoses)
Select
Next
IF CKD15_DAT = NULL (all stages)
Reject
Next
IF CKD35_DAT = NULL (stages 3-5)
Reject
Next
IF CKD35_DAT ≥ CKD15_DAT
Select
Reject
Note: The patient can have either a Chronic Kidney disease diagnosis code recorded or a Chronic Kidney disease stage code. However if just the latter, then the latest Stage code must be stage 3-5.

Patients with Asthma True False 6
AST_GROUP
IF ASTADM_DAT <> NULL
Select
Next
(IF AST_DAT <> NULL) AND
((IF ASTMED_DAT <> NULL) OR
(IF ASTRX_DAT <> NULL))
Select
Reject
Note: The patient can have an Emergency Asthma admission code only to be included in the At Risk Group. Otherwise, the patient must have an Asthma diagnosis code AND either a recent prescription code or a recent admin code indicating the patient is on medication for inhaled steroids
7 Patients with CNS Disease (including Stroke/TIA) True False
CNS_GROUP
IF CNSGROUP_DAT <> NULL
Select
Reject
Patients who have Chronic Respiratory Disease True False 8
RESP_GROUP
IF AST_GROUP <> NULL
Select
Next
IF RESP_DAT <> NULL
Select
Reject
Note: Patients with asthma OR another respiratory diagnosis fall into this group (ie. they do not need to have both)
9 Patients with Morbid Obesity True False
BMI_GROUP
IF AGE AT AUDITEND_DAT<18YRS
Reject
Next
IF SEV_OBESITY_DAT > BMI_DAT
OR
IF SEV_OBESITY_DAT <> NULL AND IF BMI_DAT = NULL
Select
Next
IF BMI_DAT ≥ BMI_STAGE_DAT
AND
IF BMI_VAL ≥40
Select
Next
IF BMI_STAGE_DAT = NULL
AND BMI_VAL ≥40
Select
Reject
10 Patients with Diabetes and other relevant endocrine disorders True False
DIAB_GROUP
IF ADDIS_DAT <> NULL
Select
Next
IF DIAB_DAT = NULL
Reject
Next
IF DMRES_DAT = NULL
Select
Next
IF DIAB_DAT > DMRES_DAT
Select
Reject
Note: If patients have a more recent “diabetes resolved” code than a diabetes diagnosis code they should not be selected in DIAB_GROUP unless they have a code representing Addison’s disease (or similar).

Patients in Any Clinical Risk Group True False 11
ATRISK_GROUP
IF IMMUNO_GROUP <> NULL
Select
Next
IF CKD_GROUP <> NULL
Select
Next
IF RESP_GROUP <> NULL
Select
Next
IF DIAB_GROUP <> NULL
Select
Next
IF CLD_DAT <> NULL
Select
Next
IF CNS_GROUP <> NULL
Select
Next
IF CHD_DAT <> NULL
Select
Next
IF PNSPLEEN_DAT <> NULL
Select
Reject
Note: Patients who are in the Pregnant, Carer, Health & Social care worker or 65 and over At Risk Groups are not included here unless they also fall into one of the above Clinical At Risk Groups. Note also that the BMI_GROUP representing Morbid Obesity has been removed from this group and is included as a separate entity where needed to avoid circular arguments in several clauses. Also that the wider Learning Disability Group is not regarded as an “At Risk” group in its own right.
Pregnant on 1st September 24 or becoming pregnant between 01/09/2024 and 28/02/2025 (inclusive) True False 12
PREG_GROUP
IF PREG2_DAT <> NULL
Select
Next
(IF PREGDEL_DAT <> NULL)
AND
((IF PREG_DAT <> NULL)
AND
(IF PREG_DAT ≥ PREGDEL_DAT))
Select
Reject
Note: There are two different groups that are combined here:
•
Group 1 (“PREG2”) is any patient that has a pregnancy code recorded from 1st September 2024 to 28th February 2025.
•
Group 2 is any patient with a pregnancy, delivered, miscarriage or termination code where the latest code recorded between 01/01/2024 and 31/08/2024 is a pregnancy code
Both of these groups are then combined to capture patients that are pregnant at a specified point in time or become pregnant from a specified point.
13 Patients currently pregnant True False
PREGCURR_GROUP
IF PREGCURR_DAT = NULL
Reject
Next
IF PREGCURR_DAT ≥ PDELCURR_DAT
Select
Reject
Note: This identifies patients who are currently pregnant (at RUN_DAT) defined by presence of a pregnancy code in the defined period without a subsequent delivery code.

Patients who have received Influenza Vaccination True False 14
FLUVAX_GROUP
IF FLUVAX_DAT <> NULL
Select
Next
IF FLURX_DAT <> NULL
Select
Reject
Note: Combine any Influenza Vaccination code given with any Influenza prescription code issues.
Patients who have not had a vaccination due to refusal/declining True False 15
FLUDECLINED_GROUP
IF FLUVAX_DAT <> NULL OR FLURX_DAT <> NULL
Reject
Next
IF DECL_DAT <> NULL OR NOCONS_DAT <> NULL
Select
Reject
Note: We are looking for any patient who has not had a vaccination in the current flu campaign and who has either a declined or a no consent code on their medical record.
Patients who have an unstated vaccination type True False 16
UNSTATVACC_GROUP
IF FLURX_DAT<> NULL
Reject
Next
IF FLUVAX_DAT <> NULL
Next
Reject
IF LAIV_GROUP <> NULL
Reject
Select
Note: Patients should be counted in this group if vaccine type cannot be determined (ie. they do not have a dm+d code, SNOMED CT or vaccination node entry (system specific functionality) that indicates the specific type of vaccine administered. For vaccine node entries, an entry of vaccine contents = influenza with no other information should be classified as unstated type.
Patients meeting Carer Criteria True False 17
CARER_GROUP
IF ATRISK_GROUP <> NULL
Reject
Next
IF BMI_GROUP <> NULL
Reject
Next
IF PREG_GROUP <> NULL
Reject
Next
IF CARER_DAT = NULL
Reject
Next
IF NOTCARER_DAT = NULL
Select
Next
IF CARER_DAT > NOTCARER_DAT
Select
Reject
Patients currently in precarious accomodation True False 18
HOMELESS_GROUP
IF HOMELESS_DAT <> NULL
Next
Reject
IF HOMELESS_DAT ≥ RESIDE_DAT
Select
Reject
Note: The patient is only classed as homeless if their latest code in RESIDE_COD is from the HOMELESS_COD cluster.

Patients in long term residential care True False 19
LONGRES_GROUP
IF LONGRES_DAT <> NULL
Next
Reject
IF LONGRES_DAT ≥ RESIDE_DAT
Select
Reject
Note: The patient is only classed as long term residential care if their latest code in RESIDE_COD is from the LONGRES_COD cluster.
Patients who have received LAIV Vaccination True False 20
LAIV_GROUP
IF LAIV_DAT <> NULL
Select
Next
IF LAIV_RX_DAT <> NULL
Select
Reject
Note: Combine any LAIV Vaccination (SNOMED) code given with any LAIV dm+d entry (or vaccination node).

2.3 Clinical data extraction criteria:
a) Patient Details
Field No Field name Data item Qualifying criteria

1. PAT_ID Patient ID number Unconditional
2. 

PAT_AGE
Patients age (years) at RUN_DAT or REF_DAT
Unconditional
Note
The Patient age is used to determine the age band of the patient. See AGE_BANDS for correct date reference for each band.
3. PAT_ENDAGE Patients age (years) at REF_DAT Unconditional Note The Patient End age is used to determine the age band of the patient. The End age is defined by the age of the patient on 31st March 2025. This is not taken into account for the lower limit of the ’16 to under 50’ age band.
4.
PAT_SEX
Patients sex at REF_DAT
Latest
5. PAT_STARTAGE Patients age (years) at CHILD_DAT Unconditional Note This is used to assist in assessing whether a child is in one of the annual cohorts potentially vaccinated in schools – not used in the Main Report ( ≥ 4 yrs to <17 yrs)



2.3 Clinical data extraction criteria:
a) Patient Details
Field No Field name Data item Qualifying criteria

1. PAT_ID Patient ID number Unconditional
2. 

PAT_AGE
Patients age (years) at RUN_DAT or REF_DAT
Unconditional
Note
The Patient age is used to determine the age band of the patient. See AGE_BANDS for correct date reference for each band.
3. PAT_ENDAGE Patients age (years) at REF_DAT Unconditional Note The Patient End age is used to determine the age band of the patient. The End age is defined by the age of the patient on 31st March 2025. This is not taken into account for the lower limit of the ’16 to under 50’ age band.
4.
PAT_SEX
Patients sex at REF_DAT
Latest
5. PAT_STARTAGE Patients age (years) at CHILD_DAT Unconditional Note This is used to assist in assessing whether a child is in one of the annual cohorts potentially vaccinated in schools – not used in the Main Report ( ≥ 4 yrs to <17 yrs)

b) Clinical data extraction criteria
Field No Field name Code group (if applicable) Qualifying criteria Non-technical description AT RISK GROUPS
6.
AST_DAT
AST_COD
EARLIEST ≤ AUDITEND_DAT
Date of earliest recorded asthma diagnosis before audit end date –
see note
Note
The presence of a diagnosis code is required. The patient must also have a code in ASTMED_COD or prescription code in ASTRX_COD to be included in the Risk Group for Asthma – See AST_GROUP logic
7. ASTMED_DAT ASTMED_COD LATEST ≥ 01/09/2023 AND ≤ (AUDITEND_DAT) Date of latest oral or inhaled steroid admin code on or after 1st Sept 23 – see note Note Codes indicating that the patient is currently taking ‘oral or inhaled steroids’. The code should be in the last 12 months. However to prevent patients from dropping out of the audit as the vaccination campaign progresses, where their latest asthma medication issue was originally within the 12 month timescale but then subsequently exceeds it, we look back for medications from 01/09/2023. E.g. If we only looked back 12 months from the Audit date, an asthma patient whose last medication was in November 2022 would be included in the October 31st 2023 results (as their latest medication is within the last 12 months), but would then drop out of the November 30th 2023 results (as their latest medication would now be over 12 months ago). By fixing the date we look back for medications to 01/09/2022, we prevent this happening.
8.
ASTRX_DAT
ASTRX_COD
LATEST ≥ 01/09/2023 AND
≤ (AUDITEND_DAT)
Date of latest asthma inhaled steroid medication on or after 1st Sept 23 – see note
9. ASTADM_DAT ASTADM_COD LATEST ≤ (AUDITEND_DAT) Date of latest asthma related admission before the audit end date – see note Note The presence of an Emergency Asthma Admission to hospital Read code at any time includes the patient in the Asthma At Risk Group, regardless of the presence of a diagnosis (AST_COD) or medication, (ASTMED_COD) or prescription code (ASTRX_COD) -See AST_GROUP logic

Field No Field name Code group (if applicable) Qualifying criteria Non-technical description
10.
RESP_DAT
RESP_COD
EARLIEST ≤ AUDITEND_DAT
Date of earliest recorded respiratory disease diagnosis before audit end date
11. CHD_DAT CHD_COD EARLIEST ≤ AUDITEND_DAT Date of earliest recorded CHD diagnosis before audit end date
12.
CKD_DAT
CKD_COD
EARLIEST ≤ AUDITEND_DAT
Date of earliest recorded CKD diagnosis before audit end date –
see note
Note
If a patient has any Chronic Kidney disease code, they are included in the CKD Risk Group. CKD stage 3 – 5 codes are handled separately – See CKD_GROUP
13. CKD15_DAT CKD15_COD LATEST ≤ AUDITEND_DAT Date of latest CKD stage (any) code recorded before audit end date – see note Note This CKD15_COD code group first captures all patients with any stage of CKD recorded. The patient record is then checked to see if the most recent Read code is a Stage 3 – 5. If so, the patient is entered into the CKD At Risk Group. If their most recent code is a 1 – 2, they are not (unless they have been brought into the group by having a CKD code from field 12 above) – See CKD_GROUP
14.
CKD35_DAT
CKD35_COD
LATEST ≤ AUDITEND_DAT
Date of latest CKD stage 3, 4 or 5 code recorded before audit end date
15. CLD_DAT CLD_COD EARLIEST ≤ AUDITEND_DAT Date of earliest recorded CLD diagnosis before audit end date
16.
DIAB_DAT
DIAB_COD
LATEST ≤ AUDITEND_DAT
Date of latest recorded diabetes diagnosis before audit end date –
see note
Note
The patient is included if any Diabetes diagnosis code is recorded

Field No Field name Code group (if applicable) Qualifying criteria Non-technical description
17. DMRES_DAT DMRES_COD LATEST ≤ AUDITEND_DAT Date of latest diabetes resolved code recorded before audit end date Note These codes are to be used in partnership with DIAB_COD to assess whether the diagnosis of diabetes has resolved – see DIAB_GROUP logic
18.
ADDIS_DAT
ADDIS_COD
EARLIEST ≤ AUDITEND_DAT
Date of earliest recorded Addison’s disease/pan-hypopituitary diagnosis code recorded before audit end date
19.
IMMADM_DAT
IMMADM_COD
LATEST ≥ 01/03/2024 AND
≤ AUDITEND_DAT
Date of latest “patient immunosuppressed” admin code since 1st March 2024
20. IMMDX_DAT IMMDX_COD LATEST ≤ AUDITEND_DAT Date of latest recorded Immunosuppression diagnosis before audit end date – see note Note The patient included if any immunosuppressant code is recorded
21.
IMMRX_DAT
IMMRX_COD
LATEST ≥ 01/03/2024 AND
≤ AUDITEND_DAT
Date of latest immunosuppression medication code issued on or after 1st March 24 and before audit end date – see note
Note
The patient is included if a prescription code is recorded in the last six months. The timeframe is limited to six months to increase the specificity of capturing patients that are currently immunosuppressed. For brevity purposes, this group includes some immunosuppressant medication that may only be issued in hospital. (This also applies to DXT_CHEMO_DAT below).
The code should be in the last six months, however, to prevent patients from dropping out of the audit as the vaccination campaign progresses, where their latest immunosuppressant medication issue was originally within the six month timescale but then subsequently exceeds it, we look back for medications from 01/03/2024.Eg. If we only looked back six months from the Audit date, an immunosuppression patient whose last medication was in May 2024 would be included in the October 31st 2024 results (as their latest medication is within the last six months), but would then drop out of the November 30th 2024 results (as their latest medication would now be over six months ago). By fixing the date we look back for medications to 01/03/2024, we prevent this happening.

Field No Field name Code group (if applicable) Qualifying criteria Non-technical description
22. DXT_CHEMO_DAT DXT_CHEMO_COD LATEST ≥ 01/03/2024 AND ≤ AUDITEND_DAT Date of latest chemotherapy or radiotherapy code issued on or after 1st March 24 and before audit end date – see note above
23.
CNSGROUP_DAT
CNSGROUP_COD
EARLIEST ≤ AUDITEND_DAT
Date of earliest recorded CNS diagnosis before audit end date
Note
The above used to be represented by three separate code groups, but have now been merged
24. PNSPLEEN_DAT PNSPLEEN_COD EARLIEST ≤ AUDITEND_DAT Date of earliest recorded Asplenia or dysfunction of the spleen code recorded before audit end date
25.
BMI_DAT
BMI_COD
LATEST ≤ AUDITEND_DAT
WHERE BMI_VAL <> NULL
Date of latest recorded BMI value recorded before audit end date
26.
BMI_VAL
BMI_COD
LATEST ≤ AUDITEND_DAT
WHERE BMI_VAL <> NULL
Value of latest recorded BMI entry recorded before audit end date
27. BMI_STAGE_DAT BMI_STAGE_COD LATEST ≤ AUDITEND_DAT Date of latest BMI stage code recorded before audit end date
28.
SEV_OBESITY_DAT
SEV_OBESITY_COD
Most recent of BMI_STAGE_DAT
AND ≤ AUDITEND_DAT
Date of latest severe obesity code where it matches the date of the latest BMI stage entry
Note
SEV_OBESITY_COD is only selected if it is the latest of the codes within BMI_STAGE – See BMI_GROUP logic
Also note removal of paediatric section in 2023-24 as Morbid Obesity adjudged to be only relevant in age 18+

Field No Field name Code group (if applicable) Qualifying criteria Non-technical description
29. SEV_LRNDIS_DAT SEV_LRNDIS_COD LATEST ≤ AUDITEND_DAT Date of latest severe learning disability code recorded before audit end date
30.
LEARNDIS_DAT
LEARNDIS_COD
LATEST ≤ AUDITEND_DAT
Date of latest learning disability code recorded before audit end date –
see note
Note
This group is not defined as an “At Risk” group in its own right
31. HHLD_IMDEF_DAT HHLD_IMDEF_COD LATEST ≤ AUDITEND_DAT Date of latest household contact of immunocompromised code recorded before the audit end date RESIDENTIAL STATUS
32.
RESIDE_DAT
RESIDE_COD
LATEST ≤ RUN_DAT
Date of the latest residence code recorded before the search run date
33. HOMELESS_DAT HOMELESS_COD LATEST ≤ RUN_DAT Date of the latest homelessness code recorded before the search run date – see note Note The patient is only classed as Homeless, if the latest RESIDE_DAT matches HOMELESS_DAT. See HOMELESS_GROUP logic
34.
LONGRES_DAT
LONGRES_COD
LATEST ≤ RUN_DAT
Date of the latest longterm residential code recorded before the search run date – see note
Note
The patient is only classed as still being in long term care, if the latest RESIDE_DAT matches LONGRES_DAT
See LONGRES_GROUP logic

Field No Field name Code group (if applicable) Qualifying criteria Non-technical description VACCINATIONS
35. FLUVAX_DAT FLUVAX_COD LATEST > 31/08/2024 AND ≤ AUDITEND_DAT Date of latest influenza vaccination administration code recorded before the audit end date – see note Note A Flu vaccination code recorded in the timeframe is counted as a Seasonal Influenza vaccination given. Note some GP suppliers use a vaccination “node” recording system and the equivalent codes can be substituted. See the associated code cluster spreadsheet for detailed concept descriptions.
36.
FLURX_DAT
FLURX_COD
LATEST > 31/08/2024 AND
≤ AUDITEND_DAT
Date of latest influenza vaccination medication code recorded before the audit end date – see note
Note
A Flu Prescription code recorded in the timeframe is counted as a Seasonal Influenza vaccination given. Note some GP suppliers use a vaccination “node” recording system and the equivalent codes can be substituted. See the associated code cluster spreadsheet for detailed concept descriptions.
37. FLUVAXOHP_DAT FLUVAXOHP_COD LATEST > 31/08/2024 AND ≤ AUDITEND_DAT Date of latest influenza vaccination given by OHP code recorded before the audit end date – see note Note These codes are a subset of the above FLUVAX code group; the codes for vaccination by pharmacist were removed from this OHP group in v11.3 and now have their own group
38.
FLUVXOPCHILD_DAT
FLUVXOPCHILD_COD
LATEST > 31/08/2024 AND
≤ AUDITEND_DAT
Date of latest influenza vaccination given by OHP code recorded before the audit end date – CHILDREN
39. FLUPHARM_DAT FLUPHARM_COD LATEST > 31/08/2024 AND ≤ AUDITEND_DAT Date of latest influenza vaccination given by Pharmacist code recorded before the audit end date
40.
SCHOOLVAC_DAT
SCHOOLVAC_COD
LATEST > 31/08/2024 AND
≤ AUDITEND_DAT
Date of latest influenza vaccination given at school code recorded before the audit end date
41. MWIFEVAC_DAT MWIFEVAC_COD LATEST > 31/08/2024 AND ≤ AUDITEND_DAT Date of latest influenza vaccination given by Midwife code recorded before the audit end date

Field No Field name Code group (if applicable) Qualifying criteria Non-technical description
42.
LAIVRX_DAT
LAIVRX_COD
LATEST > 31/08/2024 AND
≤ AUDITEND_DAT
Date of latest Live Attenuated Influenza Vaccine medication code recorded before the audit end date
43. LAIV_DAT LAIV_COD LATEST > 31/08/2024 AND ≤ AUDITEND_DAT Date of latest LAIV admin code recorded before the audit end date
44.
AQIVRX_DAT
AQIVRX_COD
LATEST > 31/08/2024 AND
≤ AUDITEND_DAT
Date of latest Adjuvanted Quadrivalent vaccination medication code recorded before the audit end date
45. QIVCRX_DAT QIVCRX_COD LATEST > 31/08/2024 AND ≤ AUDITEND_DAT Date of latest Quadrivalent cell-grown influenza vaccination medication code recorded before the audit end date
46.
QIVERX_DAT
QIVERX_COD
LATEST > 31/08/2024 AND
≤ AUDITEND_DAT
Date of latest Quadrivalent egg-grown influenza vaccination medication code recorded before the audit end date
47. QIVHDRX_DAT QIVHDRX_COD LATEST > 31/08/2024 AND ≤ AUDITEND_DAT Date of latest high dose quadrivalent influenza vaccination medication code recorded before the audit end date
