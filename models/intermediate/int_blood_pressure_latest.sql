{{ 
    config(
        materialized = 'table',
        tags = ['blood_pressure', 'latest'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Contains the latest blood pressure reading for each person, based on clinical_effective_date, derived from int_blood_pressure_all.'"
        ]
    )
}}

-- Select the latest blood pressure reading per person
{{ get_latest_events(
    from_table=ref('int_blood_pressure_all'),
    partition_by='person_id',
    order_by='clinical_effective_date',
    direction='DESC'
) }} 