import os
from pathlib import Path

# Columns that don't exist in the database but are in sources.yml
columns_to_remove = {
    'stg_olids_encounter.sql': ['encounter_source_concept_id'],
    'stg_olids_medication_order.sql': ['medication_order_source_concept_id'],
    'stg_olids_episode_of_care.sql': ['episode_type_source_concept_id', 'episode_status_source_concept_id'],
    'stg_olids_location_contact.sql': ['ldsbusinessid_contacttype'],
    'stg_olids_medication_statement.sql': ['ldsconceptid_authorisationtype', 'ldsconceptid_dateprecision', 'medication_statement_source_concept_id'],
    'stg_olids_observation.sql': ['ldsbusinessid_practioner', 'parent_obervation_id', 'observation_source_concept_id'],
    'stg_olids_person.sql': ['requesting_nhs_numberhash', 'errror_success_code', 'matched_nhs_numberhash'],
    'stg_olids_procedure_request.sql': ['procedure_source_concept_id']
}

staging_dir = Path('models/staging')

for filename, columns in columns_to_remove.items():
    file_path = staging_dir / filename
    if file_path.exists():
        with open(file_path, 'r') as f:
            lines = f.readlines()
        
        new_lines = []
        for line in lines:
            # Check if this line contains a column we need to remove
            skip_line = False
            for col in columns:
                if f'"{col}"' in line:
                    skip_line = True
                    print(f"Removing {col} from {filename}")
                    break
            
            if not skip_line:
                # If the previous line ends with a comma and this is 'from', remove the comma
                if new_lines and line.strip().startswith('from ') and new_lines[-1].rstrip().endswith(','):
                    new_lines[-1] = new_lines[-1].rstrip()[:-1] + '\n'
                new_lines.append(line)
        
        with open(file_path, 'w') as f:
            f.writelines(new_lines)
        print(f"Updated {filename}")

print("Column fixes complete")