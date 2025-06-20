#!/usr/bin/env python3

import subprocess
import json

print('Testing updated macro that should get ALL models...')

cmd = ['dbt', 'show', '--select', 'utility_get_model_columns', '--limit', '1', '--output', 'json']
result = subprocess.run(cmd, capture_output=True, text=True, cwd='.')

if result.returncode == 0:
    stdout = result.stdout.strip()
    json_start = stdout.find('{"node"')
    if json_start == -1:
        json_start = stdout.find('{\n  "node"')
        
    if json_start >= 0:
        json_output = stdout[json_start:]
        output_data = json.loads(json_output)
        
        if 'show' in output_data and output_data['show']:
            row_data = output_data['show'][0]
            if 'MODEL_COLUMNS_JSON' in row_data:
                json_str = row_data['MODEL_COLUMNS_JSON']
                columns_data = json.loads(json_str)
                print(f'✅ Found column information for {len(columns_data)} models!')
                
                # Show first few models
                model_names = list(columns_data.keys())
                print(f'\nSample models:')
                for i, model_name in enumerate(model_names[:10]):
                    col_count = len(columns_data[model_name])
                    print(f'  {i+1:2d}. {model_name}: {col_count} columns')
                
                if len(model_names) > 10:
                    print(f'\n... and {len(model_names) - 10} more models')
                    
                # Show some stats
                total_columns = sum(len(cols) for cols in columns_data.values())
                avg_columns = total_columns / len(columns_data) if columns_data else 0
                print(f'\nStats:')
                print(f'  Total models: {len(columns_data)}')
                print(f'  Total columns: {total_columns}')
                print(f'  Average columns per model: {avg_columns:.1f}')
            else:
                print('❌ MODEL_COLUMNS_JSON not found')
                print('Row data keys:', list(row_data.keys()))
        else:
            print('❌ No show data')
    else:
        print('❌ Could not find JSON in output')
        print('First 200 chars:', repr(stdout[:200]))
else:
    print(f'❌ dbt command failed: {result.stderr}') 