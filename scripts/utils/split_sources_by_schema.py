"""
Script to split olids_core source into OLIDS_MASKED and OLIDS_COMMON sources.
Tables with patient_id/person_id belong to OLIDS_MASKED.
Other tables belong to OLIDS_COMMON.
"""

# Tables that should be in OLIDS_COMMON (no patient data)
COMMON_TABLES = [
    'APPOINTMENT_PRACTITIONER',
    'LOCATION',
    'LOCATION_CONTACT',
    'ORGANISATION',
    'PRACTITIONER',
    'PRACTITIONER_IN_ROLE',
    'SCHEDULE',
    'SCHEDULE_PRACTITIONER',
]

# Tables that should be in OLIDS_MASKED (contain patient/person data)
MASKED_TABLES = [
    'ALLERGY_INTOLERANCE',
    'APPOINTMENT',
    'DIAGNOSTIC_ORDER',
    'ENCOUNTER',
    'EPISODE_OF_CARE',
    'FLAG',
    'MEDICATION_ORDER',
    'MEDICATION_STATEMENT',
    'OBSERVATION',
    'PATIENT',
    'PATIENT_ADDRESS',
    'PATIENT_CONTACT',
    'PATIENT_REGISTERED_PRACTITIONER_IN_ROLE',
    'PATIENT_UPRN',
    'PROCEDURE_REQUEST',
    'REFERRAL_REQUEST',
]

print("OLIDS_COMMON tables:")
for table in sorted(COMMON_TABLES):
    print(f"  - {table}")

print("\nOLIDS_MASKED tables:")
for table in sorted(MASKED_TABLES):
    print(f"  - {table}")

print(f"\nTotal COMMON: {len(COMMON_TABLES)}")
print(f"Total MASKED: {len(MASKED_TABLES)}")
