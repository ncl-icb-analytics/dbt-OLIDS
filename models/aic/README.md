# AI Centre dbt models

Models here include PhenoLab dependencies and GP/SUS data transformations.

In the future, these should be refactored into the NCL pipeline.

**Due to id discrepencies, we are temporarily using `sk_patient_id` as person_id**
```sql
select count(distinct "id") from patient;
    4599292

select count(distinct "sk_patient_id") from patient;
    2085721

select count(distinct "nhs_number_hash") from patient;
    2087624

select count(distinct "patient_id") from patient_person;
    203577
```