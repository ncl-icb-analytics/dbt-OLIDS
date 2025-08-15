{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        persist_docs={"relation": true})
}}

/*
All cervical screening programme observations from clinical records.
Uses QOF cervical screening cluster IDs:
- SMEAR_COD: Cervical screening completed codes
- CSPU_COD: Cervical screening unsuitable codes  
- CSDEC_COD: Cervical screening declined codes
- CSPCAINVITE_COD: Not responded to three invitations codes

Clinical Purpose:
- Cervical screening programme data collection
- Observation-level screening events tracking
- Foundation data for programme analysis

Key Business Rules:
- Women aged 25-49: invited every 3 years
- Women aged 50-64: invited every 5 years
- Declined/non-response status: valid for 12 months only
- Unsuitable status: permanent unless superseded by completed screening

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per cervical screening observation.
Use this model as input for int_cervical_screening_latest.sql and fct_cervical_screening_status.sql.
*/

SELECT
    obs.observation_id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- Screening type classification
    CASE
        WHEN obs.cluster_id = 'SMEAR_COD' THEN 'Cervical Screening Completed'
        WHEN obs.cluster_id = 'CSPU_COD' THEN 'Screening Unsuitable'
        WHEN obs.cluster_id = 'CSDEC_COD' THEN 'Screening Declined'
        WHEN obs.cluster_id = 'CSPCAINVITE_COD' THEN 'Non-response to Invitations'
        ELSE 'Unknown'
    END AS screening_observation_type,

    -- Simple screening type flags
    CASE WHEN obs.cluster_id = 'SMEAR_COD' THEN TRUE ELSE FALSE END AS is_completed_screening,
    CASE WHEN obs.cluster_id = 'CSPU_COD' THEN TRUE ELSE FALSE END AS is_unsuitable_screening,
    CASE WHEN obs.cluster_id = 'CSDEC_COD' THEN TRUE ELSE FALSE END AS is_declined_screening,
    CASE WHEN obs.cluster_id = 'CSPCAINVITE_COD' THEN TRUE ELSE FALSE END AS is_non_response_screening,

    -- Cytology result categorisation for SMEAR_COD (static code interpretation)
    CASE
        WHEN obs.cluster_id != 'SMEAR_COD' THEN NULL
        WHEN LOWER(obs.mapped_concept_display) LIKE '%normal%' 
            OR LOWER(obs.mapped_concept_display) LIKE '%negative%'
            OR LOWER(obs.mapped_concept_display) LIKE '%no abnormality%' THEN 'Normal'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%borderline%' THEN 'Borderline Changes'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%mild dyskaryosis%' THEN 'Mild Dyskaryosis'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%moderate dyskaryosis%' THEN 'Moderate Dyskaryosis'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%severe dyskaryosis%' THEN 'Severe Dyskaryosis'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%cannot exclude invasive%'
            OR LOWER(obs.mapped_concept_display) LIKE '%invasive carcinoma%' THEN 'Severe Dyskaryosis - ?Invasive'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%glandular neoplasia%'
            OR LOWER(obs.mapped_concept_display) LIKE '%cannot exclude glandular%' THEN 'Glandular Abnormality'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%inflammatory%' THEN 'Inflammatory Changes'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%atrophic%' THEN 'Atrophic Changes'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%endocervical cells present%' THEN 'Adequate Sample'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%trichomonas%' THEN 'Trichomonas Infection'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%candida%' THEN 'Candida Infection'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%herpes%' THEN 'Herpes Infection'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%actinomyces%' THEN 'Actinomyces Infection'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%gardnerella%' THEN 'Gardnerella Infection'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%koilocytosis%'
            OR LOWER(obs.mapped_concept_display) LIKE '%viral%'
            OR LOWER(obs.mapped_concept_display) LIKE '%wart virus%' THEN 'Viral Changes (HPV)'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%action needed%'
            OR LOWER(obs.mapped_concept_display) LIKE '%colposcopy%' THEN 'Referral Required'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%nuclear abnormality%' THEN 'Nuclear Abnormality'
        ELSE 'Screening Completed'
    END AS cytology_result_category,

    -- Clinical risk assessment based on cytology results (static interpretation)
    CASE
        WHEN obs.cluster_id != 'SMEAR_COD' THEN NULL
        WHEN LOWER(obs.mapped_concept_display) LIKE '%normal%' 
            OR LOWER(obs.mapped_concept_display) LIKE '%negative%'
            OR LOWER(obs.mapped_concept_display) LIKE '%no abnormality%' THEN 'Low Risk (Normal)'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%borderline%'
            OR LOWER(obs.mapped_concept_display) LIKE '%mild dyskaryosis%' THEN 'Low-Moderate Risk'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%moderate dyskaryosis%' THEN 'Moderate Risk'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%severe dyskaryosis%'
            OR LOWER(obs.mapped_concept_display) LIKE '%cannot exclude invasive%'
            OR LOWER(obs.mapped_concept_display) LIKE '%glandular neoplasia%' THEN 'High Risk (Abnormal)'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%action needed%'
            OR LOWER(obs.mapped_concept_display) LIKE '%colposcopy%' THEN 'Requires Clinical Assessment'
        ELSE 'Risk Assessment Required'
    END AS cervical_screening_risk_category,

    -- Abnormality grade classification
    CASE
        WHEN obs.cluster_id != 'SMEAR_COD' THEN NULL
        WHEN LOWER(obs.mapped_concept_display) LIKE '%normal%' 
            OR LOWER(obs.mapped_concept_display) LIKE '%negative%'
            OR LOWER(obs.mapped_concept_display) LIKE '%no abnormality%' THEN 'Normal'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%borderline%'
            OR LOWER(obs.mapped_concept_display) LIKE '%mild dyskaryosis%' THEN 'Low Grade'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%moderate dyskaryosis%' THEN 'Moderate Grade'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%severe dyskaryosis%'
            OR LOWER(obs.mapped_concept_display) LIKE '%cannot exclude invasive%'
            OR LOWER(obs.mapped_concept_display) LIKE '%glandular neoplasia%' THEN 'High Grade'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%koilocytosis%'
            OR LOWER(obs.mapped_concept_display) LIKE '%viral%'
            OR LOWER(obs.mapped_concept_display) LIKE '%wart virus%'
            OR LOWER(obs.mapped_concept_display) LIKE '%hpv%'
            OR LOWER(obs.mapped_concept_display) LIKE '%human papilloma%' THEN 'HPV Changes'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%trichomonas%'
            OR LOWER(obs.mapped_concept_display) LIKE '%candida%'
            OR LOWER(obs.mapped_concept_display) LIKE '%herpes%'
            OR LOWER(obs.mapped_concept_display) LIKE '%actinomyces%'
            OR LOWER(obs.mapped_concept_display) LIKE '%gardnerella%' THEN 'Infection'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%inflammatory%' THEN 'Inflammatory'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%atrophic%' THEN 'Atrophic Changes'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%nuclear abnormality%' THEN 'Nuclear Abnormality'
        ELSE 'Other Finding'
    END AS abnormality_grade,

    -- Sample adequacy type
    CASE
        WHEN obs.cluster_id != 'SMEAR_COD' THEN NULL
        WHEN LOWER(obs.mapped_concept_display) LIKE '%endocervical cells present%' THEN 'Adequate'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%inadequate%' 
            OR LOWER(obs.mapped_concept_display) LIKE '%insufficient%' THEN 'Inadequate'
        ELSE 'Unspecified'
    END AS sample_adequacy,

    -- Clinical action required
    CASE
        WHEN obs.cluster_id != 'SMEAR_COD' THEN NULL
        WHEN LOWER(obs.mapped_concept_display) LIKE '%action needed%'
            OR LOWER(obs.mapped_concept_display) LIKE '%colposcopy%' THEN 'Colposcopy Required'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%repeat%' 
            OR LOWER(obs.mapped_concept_display) LIKE '%follow%' THEN 'Repeat Required'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%normal%' 
            OR LOWER(obs.mapped_concept_display) LIKE '%negative%' THEN 'Routine Follow-up'
        ELSE 'Clinical Review'
    END AS clinical_action_required

FROM ({{ get_observations("'SMEAR_COD', 'CSPU_COD', 'CSDEC_COD', 'CSPCAINVITE_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date DESC, observation_id 