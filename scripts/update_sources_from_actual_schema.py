#!/usr/bin/env python3
"""
Script to update sources.yml based on actual database schema
Run this after getting the actual schema results from Snowflake
"""

# Based on the actual schema results you provided, here are the key updates needed:

OLIDS_MASKED_SCHEMA_UPDATES = {
    # Key changes we identified:
    "LOCATION_CONTACT": {
        "remove": ["ldsbusinessid_contacttype"],
        "add": [("contact_type", "TEXT")]
    },
    "MEDICATION_STATEMENT": {
        "remove": ["ldsconceptid_authorisationtype", "ldsconceptid_dateprecision"],
        "add": [
            ("authorisation_type_concept_id", "TEXT"),
            ("date_precision_concept_id", "TEXT"),
            ("expiry_date", "TIMESTAMP_NTZ"),
        ]
    },
    "OBSERVATION": {
        "remove": ["ldsbusinessid_practioner"],
        "add": [
            ("person_id", "TEXT"),
            ("practioner_id", "TEXT")  # Note: this has the typo in actual DB
        ]
    },
    "PERSON": {
        "remove": ["lds_business_key", "primary_patient_id"],
        "add": [
            ("LDSBusinessId_PrimaryPatient", "TEXT"),
            ("lds_datetime_data_acquired", "TIMESTAMP_NTZ"),
            ("requesting_patient_record_id", "TEXT"),
            ("unique_reference", "TEXT"),
            ("requesting_nhs_numberhash", "BINARY"),
            ("errror_success_code", "TEXT"),  # Note: has typo in actual DB
            ("matched_nhs_numberhash", "BINARY"),
            ("sensitivity_flag", "TEXT"),
            ("matched_algorithm_indicator", "TEXT"),
            ("requesting_patient_id", "TEXT")
        ]
    },
    # Additional new columns found in other tables:
    "APPOINTMENT": {
        "add": [
            ("is_blocked", "BOOLEAN"),
            ("national_slot_category_name", "TEXT"),
            ("context_type", "TEXT"),
            ("service_setting", "TEXT"),
            ("national_slot_category_description", "TEXT"),
            ("csds_care_contact_identifier", "TEXT"),
            ("person_id", "TEXT")
        ]
    },
    "MEDICATION_ORDER": {
        "add": [("issue_method_description", "TEXT")]
    },
    "PATIENT_ADDRESS": {
        "add": [("person_id", "TEXT")]
    },
    "REFERRAL_REQUEST": {
        "add": [("referral_request_specialty_concept_id", "TEXT")]
    }
}

# Data type changes (VARCHAR -> TEXT, TIMESTAMP_NTZ(9) -> TIMESTAMP_NTZ)
GLOBAL_TYPE_UPDATES = {
    "VARCHAR": "TEXT",
    "TIMESTAMP_NTZ(9)": "TIMESTAMP_NTZ",
    "NUMBER(38,0)": "NUMBER"
}

def print_updates():
    """Print the key updates that need to be made"""
    print("Key Schema Updates Needed:")
    print("=" * 50)
    
    for table, changes in OLIDS_MASKED_SCHEMA_UPDATES.items():
        print(f"\n{table}:")
        if "remove" in changes:
            print(f"  Remove: {changes['remove']}")
        if "add" in changes:
            print(f"  Add: {[f'{col[0]} ({col[1]})' for col in changes['add']]}")
    
    print(f"\nGlobal Type Updates: {GLOBAL_TYPE_UPDATES}")

if __name__ == "__main__":
    print_updates()
    print("\nNext steps:")
    print("1. Update sources.yml manually with these changes")
    print("2. Update staging models to include new columns")
    print("3. Test dbt run to verify fixes")