import os
from pathlib import Path

# List of all current staging models
current_models = set()

# Get all .sql files in staging directory
staging_dir = Path('models/staging')
for sql_file in staging_dir.glob('**/*.sql'):
    if sql_file.name.startswith('stg_'):
        model_name = sql_file.stem  # filename without .sql
        current_models.add(model_name)

print(f"Found {len(current_models)} current staging models")

# Read schema.yml
schema_file = staging_dir / 'schema.yml'
with open(schema_file, 'r') as f:
    lines = f.readlines()

# Parse and filter schema.yml
new_lines = []
in_model = False
model_lines = []
model_name = None
skip_model = False

for line in lines:
    if line.strip().startswith('- name:'):
        # Found a model definition
        if in_model and model_lines and not skip_model:
            # Add the previous model if it wasn't skipped
            new_lines.extend(model_lines)
        
        # Start new model
        model_name = line.strip().split('name:')[1].strip()
        in_model = True
        model_lines = [line]
        skip_model = model_name not in current_models
        
        if skip_model:
            print(f"Removing old model from schema: {model_name}")
    elif in_model:
        model_lines.append(line)
        # Check if we've reached the end of this model (next model starts or end of file)
        if line.strip() == '' or not line.startswith(' '):
            if not skip_model:
                new_lines.extend(model_lines)
            model_lines = []
            in_model = False
    else:
        new_lines.append(line)

# Add last model if needed
if in_model and model_lines and not skip_model:
    new_lines.extend(model_lines)

# Write cleaned schema
with open(schema_file, 'w') as f:
    f.writelines(new_lines)

print(f"Schema cleaned. Kept {len([l for l in new_lines if '- name:' in l])} models")