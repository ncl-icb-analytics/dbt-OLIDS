{{ 
    config(
        materialized = 'table',
        tags = ['blood_pressure', 'latest']
    )
}}

-- Select the latest blood pressure reading per person
SELECT *
FROM {{ get_latest_events(
    from_table=ref('int_blood_pressure_all'),
    partition_by='person_id',
    order_by='clinical_effective_date',
    direction='DESC'
) }} 