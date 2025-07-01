/*
Flu Campaign Utility Macros

Helper macros for dynamic flu campaign configuration and data retrieval.
These macros support the campaign-specific model approach while maintaining DRY principles.

UPDATED: Removed unsafe introspection - uses static configuration based on actual seed data
*/

-- Get cluster IDs for a specific rule group and campaign
{% macro get_flu_clusters_for_rule_group(campaign_id, rule_group_id, data_source_type=none) %}
    {%- set config = {
        'flu_2024_25': {
            'clusters': {
                'AST_GROUP': {
                    'observation': ['AST_COD', 'ASTMED_COD'],
                    'medication': ['ASTRX_COD']
                },
                'AST_ADM_GROUP': {
                    'observation': ['ASTADM_COD']
                },
                'RESP_GROUP': {
                    'observation': ['RESP_COD']
                },
                'CHD_GROUP': {
                    'observation': ['CHD_COD']
                },
                'CKD_GROUP': {
                    'observation': ['CKD_COD', 'CKD15_COD', 'CKD35_COD']
                },
                'CLD_GROUP': {
                    'observation': ['CLD_COD']
                },
                'DIAB_GROUP': {
                    'observation': ['DIAB_COD', 'DMRES_COD', 'ADDIS_COD']
                },
                'IMMUNO_GROUP': {
                    'observation': ['IMMDX_COD', 'IMMADM_COD', 'DXT_CHEMO_COD'],
                    'medication': ['IMMRX_COD']
                },
                'CNS_GROUP': {
                    'observation': ['CNSGROUP_COD']
                },
                'ASPLENIA_GROUP': {
                    'observation': ['PNSPLEEN_COD']
                },
                'BMI_GROUP': {
                    'observation': ['BMI_COD', 'BMI_STAGE_COD', 'SEV_OBESITY_COD']
                },
                'PREG_GROUP': {
                    'observation': ['PREGDEL_COD', 'PREG_COD']
                },
                'LEARNDIS_GROUP': {
                    'observation': ['LEARNDIS_COD']
                },
                'CARER_GROUP': {
                    'observation': ['CARER_COD', 'NOTCARER_COD']
                },
                'HOMELESS_GROUP': {
                    'observation': ['RESIDE_COD', 'HOMELESS_COD']
                },
                'LONGRES_GROUP': {
                    'observation': ['RESIDE_COD', 'LONGRES_COD']
                },
                'HHLD_IMDEF_GROUP': {
                    'observation': ['HHLD_IMDEF_COD']
                },
                'HCWORKER_GROUP': {
                    'observation': ['CAREHOME_COD', 'NURSEHOME_COD', 'DOMCARE_COD']
                },
                'FLUVAX_GROUP': {
                    'observation': ['FLUVAX_COD'],
                    'medication': ['FLURX_COD']
                },
                'FLUDECLINED_GROUP': {
                    'observation': ['DECL_COD', 'NOCONS_COD']
                },
                'LAIV_GROUP': {
                    'observation': ['LAIV_COD'],
                    'medication': ['LAIVRX_COD']
                }
            }
        }
    } -%}
    
    {%- set campaign_config = config.get(campaign_id, {}) -%}
    {%- set clusters_config = campaign_config.get('clusters', {}) -%}
    {%- set rule_clusters = clusters_config.get(rule_group_id, {}) -%}
    
    {%- if data_source_type -%}
        {%- set cluster_list = rule_clusters.get(data_source_type, []) -%}
    {%- else -%}
        {%- set cluster_list = [] -%}
        {%- for ds_type, clusters in rule_clusters.items() -%}
            {%- for cluster in clusters -%}
                {%- do cluster_list.append(cluster) -%}
            {%- endfor -%}
        {%- endfor -%}
    {%- endif -%}
    
    {%- if cluster_list|length > 0 -%}
        {{- "'" ~ cluster_list|join("','") ~ "'" -}}
    {%- else -%}
        {{- "'PLACEHOLDER_CLUSTER'" -}}
    {%- endif -%}
{% endmacro %}

-- Get campaign dates for a specific campaign and rule group
{% macro get_flu_campaign_date(campaign_id, rule_group_id, date_type) %}
    {%- set config = {
        'flu_2024_25': {
            'dates': {
                'ALL': {
                    'start_dat': '2024-09-01',
                    'ref_dat': '2025-03-31',
                    'child_dat': '2024-08-31',
                    'audit_end_dat': '2025-02-28'
                },
                'AST_GROUP': {
                    'latest_since_date': '2023-09-01'
                },
                'IMMUNO_GROUP': {
                    'latest_since_date': '2024-03-01'
                },
                'CHILD_2_3': {
                    'birth_start': '2020-09-01',
                    'birth_end': '2022-08-31'
                },
                'CHILD_4_16': {
                    'birth_start': '2008-09-01',
                    'birth_end': '2020-08-31'
                },
                'FLUVAX_GROUP': {
                    'latest_after_date': '2024-08-31'
                },
                'LAIV_GROUP': {
                    'latest_after_date': '2024-08-31'
                }
            }
        }
    } -%}
    
    {%- set campaign_config = config.get(campaign_id, {}) -%}
    {%- set dates_config = campaign_config.get('dates', {}) -%}
    
    {%- set date_value = none -%}
    
    {%- set rule_dates = dates_config.get(rule_group_id, {}) -%}
    {%- if date_type in rule_dates -%}
        {%- set date_value = rule_dates[date_type] -%}
    {%- endif -%}
    
    {%- if not date_value -%}
        {%- set all_dates = dates_config.get('ALL', {}) -%}
        {%- if date_type in all_dates -%}
            {%- set date_value = all_dates[date_type] -%}
        {%- endif -%}
    {%- endif -%}
    
    {%- if date_value -%}
        {{- "'" ~ date_value ~ "'" -}}
    {%- else -%}
        {{- "NULL" -}}
    {%- endif -%}
{% endmacro %}

-- Get audit end date (from variable or campaign config)
{% macro get_flu_audit_date(campaign_id=none) %}
    {%- set audit_date = var('flu_audit_end_date', 'CURRENT_DATE') -%}
    {%- if audit_date == 'CURRENT_DATE' -%}
        CURRENT_DATE
    {%- else -%}
        '{{ audit_date }}'
    {%- endif -%}
{% endmacro %}

-- Get observations for a rule group (replaces hardcoded cluster lists)
{% macro get_flu_observations_for_rule_group(campaign_id, rule_group_id, source='UKHSA_FLU') %}
    {%- set clusters = get_flu_clusters_for_rule_group(campaign_id, rule_group_id, 'observation') -%}
    SELECT person_id, clinical_effective_date, cluster_id, result_value, result_text
    FROM ({{ get_observations(clusters, source) }})
    WHERE clinical_effective_date IS NOT NULL
{% endmacro %}

-- Get medications for a rule group (replaces hardcoded cluster lists)
{% macro get_flu_medications_for_rule_group(campaign_id, rule_group_id, source='UKHSA_FLU') %}
    {%- set clusters = get_flu_clusters_for_rule_group(campaign_id, rule_group_id, 'medication') -%}
    SELECT person_id, order_date, cluster_id
    FROM ({{ get_medication_orders(cluster_id=clusters, source=source) }})
    WHERE order_date IS NOT NULL
{% endmacro %}

-- Get rule configuration for a specific rule group
{% macro get_flu_rule_config(campaign_id, rule_group_id) %}
    {%- set rule_configs = {
        'flu_2024_25': {
            'AST_GROUP': {
                'rule_type': 'COMBINATION',
                'logic_expression': 'AST_COD AND (ASTMED_COD OR ASTRX_COD)',
                'exclusion_groups': '',
                'age_min_months': 6,
                'age_max_years': 65,
                'description': 'People with asthma who have used inhalers since Sept 2023 or been hospitalised'
            },
            'AST_ADM_GROUP': {
                'rule_type': 'SIMPLE',
                'logic_expression': 'ASTADM_COD',
                'exclusion_groups': '',
                'age_min_months': 6,
                'age_max_years': 65,
                'description': 'People with asthma who have been hospitalised for their condition'
            },
            'RESP_GROUP': {
                'rule_type': 'COMBINATION',
                'logic_expression': 'AST_GROUP OR AST_ADM_GROUP OR RESP_COD',
                'exclusion_groups': '',
                'age_min_months': 6,
                'age_max_years': 65,
                'description': 'People with chronic lung conditions (asthma, COPD, cystic fibrosis etc.)'
            },
            'CHD_GROUP': {
                'rule_type': 'SIMPLE',
                'logic_expression': 'CHD_COD',
                'exclusion_groups': '',
                'age_min_months': 6,
                'age_max_years': 65,
                'description': 'People with coronary heart disease, heart failure or stroke'
            },
            'CKD_GROUP': {
                'rule_type': 'HIERARCHICAL',
                'logic_expression': 'CKD_COD OR (CKD35_COD >= CKD15_COD)',
                'exclusion_groups': '',
                'age_min_months': 6,
                'age_max_years': 65,
                'description': 'People with chronic kidney disease stage 3-5'
            },
            'CLD_GROUP': {
                'rule_type': 'SIMPLE',
                'logic_expression': 'CLD_COD',
                'exclusion_groups': '',
                'age_min_months': 6,
                'age_max_years': 65,
                'description': 'People with chronic liver disease (cirrhosis, hepatitis etc.)'
            },
            'DIAB_GROUP': {
                'rule_type': 'EXCLUSION',
                'logic_expression': 'ADDIS_COD OR (DIAB_COD AND (DMRES_COD IS NULL OR DIAB_COD > DMRES_COD))',
                'exclusion_groups': '',
                'age_min_months': 6,
                'age_max_years': 65,
                'description': 'People with diabetes (type 1, type 2) or Addison\'s disease'
            },
            'IMMUNO_GROUP': {
                'rule_type': 'COMBINATION',
                'logic_expression': 'IMMDX_COD OR IMMRX_COD OR IMMADM_COD OR DXT_CHEMO_COD',
                'exclusion_groups': '',
                'age_min_months': 6,
                'age_max_years': 65,
                'description': 'People with weakened immune systems or receiving immunosuppressive treatment'
            },
            'CNS_GROUP': {
                'rule_type': 'SIMPLE',
                'logic_expression': 'CNSGROUP_COD',
                'exclusion_groups': '',
                'age_min_months': 6,
                'age_max_years': 65,
                'description': 'People with chronic neurological conditions (MS, motor neurone disease etc.)'
            },
            'ASPLENIA_GROUP': {
                'rule_type': 'SIMPLE',
                'logic_expression': 'PNSPLEEN_COD',
                'exclusion_groups': '',
                'age_min_months': 6,
                'age_max_years': 65,
                'description': 'People with asplenia or splenic dysfunction'
            },
            'OVER65_GROUP': {
                'rule_type': 'AGE_BASED',
                'logic_expression': '',
                'exclusion_groups': '',
                'age_min_months': 780,
                'age_max_years': null,
                'description': 'Everyone aged 65 and over at end of March 2025'
            },
            'CHILD_2_3': {
                'rule_type': 'AGE_BIRTH_RANGE',
                'logic_expression': '',
                'exclusion_groups': '',
                'age_min_months': null,
                'age_max_years': null,
                'description': 'Children aged 2-3 years (born Sept 2020 - Aug 2022)'
            },
            'CHILD_4_16': {
                'rule_type': 'AGE_BIRTH_RANGE',
                'logic_expression': '',
                'exclusion_groups': '',
                'age_min_months': null,
                'age_max_years': null,
                'description': 'School children aged 4-16 years (Reception to Year 11)'
            },
            'BMI_GROUP': {
                'rule_type': 'HIERARCHICAL',
                'logic_expression': 'BMI_VAL >= 40 OR SEV_OBESITY_COD',
                'exclusion_groups': '',
                'age_min_months': 216,
                'age_max_years': 65,
                'description': 'Adults aged 18-64 with severe obesity (BMI 40+)'
            },
            'PREG_GROUP': {
                'rule_type': 'HIERARCHICAL',
                'logic_expression': 'PREG2_DAT OR (PREGDEL_DAT AND PREG_DAT >= PREGDEL_DAT)',
                'exclusion_groups': '',
                'age_min_months': 144,
                'age_max_years': 65,
                'description': 'Pregnant women aged 12-64'
            },
            'LEARNDIS_GROUP': {
                'rule_type': 'SIMPLE',
                'logic_expression': 'LEARNDIS_COD',
                'exclusion_groups': '',
                'age_min_months': 6,
                'age_max_years': 65,
                'description': 'People with learning disabilities aged 6 months to 64 years'
            },
            'CARER_GROUP': {
                'rule_type': 'EXCLUSION',
                'logic_expression': 'CARER_COD AND (NOTCARER_COD IS NULL OR CARER_COD > NOTCARER_COD)',
                'exclusion_groups': 'NOT IN clinical_risk_groups|BMI_GROUP|PREG_GROUP',
                'age_min_months': 60,
                'age_max_years': 65,
                'description': 'Unpaid carers aged 5-64 (not already eligible for other reasons)'
            },
            'HOMELESS_GROUP': {
                'rule_type': 'HIERARCHICAL',
                'logic_expression': 'Latest RESIDE_COD is HOMELESS_COD',
                'exclusion_groups': '',
                'age_min_months': 192,
                'age_max_years': 65,
                'description': 'People who are homeless aged 16-64'
            },
            'LONGRES_GROUP': {
                'rule_type': 'HIERARCHICAL',
                'logic_expression': 'Latest RESIDE_COD is LONGRES_COD',
                'exclusion_groups': '',
                'age_min_months': 6,
                'age_max_years': null,
                'description': 'People living in care homes or long-term residential care'
            },
            'HHLD_IMDEF_GROUP': {
                'rule_type': 'SIMPLE',
                'logic_expression': 'HHLD_IMDEF_COD',
                'exclusion_groups': '',
                'age_min_months': 6,
                'age_max_years': 65,
                'description': 'People who live with someone who has a weakened immune system'
            },
            'HCWORKER_GROUP': {
                'rule_type': 'COMBINATION',
                'logic_expression': 'CAREHOME_COD OR NURSEHOME_COD OR DOMCARE_COD',
                'exclusion_groups': '',
                'age_min_months': 192,
                'age_max_years': 65,
                'description': 'Health and social care workers aged 16-64'
            },
            'FLUVAX_GROUP': {
                'rule_type': 'COMBINATION',
                'logic_expression': 'FLUVAX_COD OR FLURX_COD',
                'exclusion_groups': '',
                'age_min_months': null,
                'age_max_years': null,
                'description': 'People who have already received flu vaccination this campaign'
            },
            'FLUDECLINED_GROUP': {
                'rule_type': 'EXCLUSION',
                'logic_expression': '(DECL_COD OR NOCONS_COD) AND NOT vaccinated',
                'exclusion_groups': '',
                'age_min_months': null,
                'age_max_years': null,
                'description': 'People who have declined flu vaccination this campaign'
            },
            'LAIV_GROUP': {
                'rule_type': 'COMBINATION',
                'logic_expression': 'LAIV_COD OR LAIVRX_COD',
                'exclusion_groups': '',
                'age_min_months': null,
                'age_max_years': null,
                'description': 'People who have received live attenuated influenza vaccine (nasal spray)'
            }
        }
    } -%}
    
    {%- set campaign_rules = rule_configs.get(campaign_id, {}) -%}
    {%- set rule_config = campaign_rules.get(rule_group_id, {}) -%}
    
    {{- rule_config -}}
{% endmacro %}