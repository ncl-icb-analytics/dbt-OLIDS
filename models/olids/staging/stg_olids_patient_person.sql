-- Staging model for olids_core.PATIENT_PERSON
-- Base layer: base_olids_patient_person (filtered for NCL practices, excludes sensitive patients)
-- Description: Core OLIDS patient and clinical data

select
    lakehousedateprocessed,
    lakehousedatetimeupdated,
    lds_record_id,
    lds_id,
    id,
    lds_datetime_data_acquired,
    lds_start_date_time,
    lds_dataset_id,
    patient_id,
    person_id
from {{ ref('base_olids_patient_person') }}
