# Snowflake Table Migration Script

## Overview

The `migrate_snowflake_tables.py` script handles the migration of static tables from `DATA_LAB_OLIDS_UAT` database to production environments using role switching and SSO authentication.

**Important**: Our data sources have migrated from dummy data to real patient data:
- **Source Database**: `Data_Store_OLIDS_UAT` (contains real patient data)
- **Target Database**: `DATA_LAB_OLIDS_UAT` (UAT environment for transformed models)
- **Required Role**: `ISL-USERGROUP-SECONDEES-NCL` for accessing real patient data

## Role Access Pattern

- **ISL-USERGROUP-SECONDEES-NCL**: Required role for accessing real patient data in `Data_Store_OLIDS_UAT`
- **ISL-USERGROUP-SECONDEES-NCL**: Has read/write access to `DATA_LAB_OLIDS_UAT`

**Security Note**: Real patient data requires strict access controls and appropriate role permissions.

The script handles this by creating two separate connections with the appropriate roles.

## Prerequisites

1. **Environment Setup**: Ensure your `.env` file contains:
   ```
   SNOWFLAKE_ACCOUNT=your-account-here
   SNOWFLAKE_USER=your-username-here
   SNOWFLAKE_WAREHOUSE=your-warehouse-here
   ```

2. **Dependencies**: Install required packages:
   ```bash
   pip install -r requirements.txt
   ```

3. **SSO Access**: Ensure you have access to both roles mentioned above.

## Usage Examples

### 1. Dry Run (Recommended First)
Test the migration without actually transferring data:
```bash
python scripts/migrate_snowflake_tables.py --dry-run
```

### 2. Migrate All Tables
Migrate all static tables from both CODESETS and RULESETS schemas:
```bash
python scripts/migrate_snowflake_tables.py
```

### 3. Migrate Specific Schema
Migrate only tables from the CODESETS schema:
```bash
python scripts/migrate_snowflake_tables.py --schema CODESETS
```

### 4. Migrate Specific Table
Migrate only a specific table:
```bash
python scripts/migrate_snowflake_tables.py --schema CODESETS --table your_table_name
```

## What the Script Does

1. **Discovery**: Connects with `ISL-USERGROUP-SECONDEES-NCL` role to list static tables
2. **Extraction**: Reads data from source tables using pandas  
3. **Schema Creation**: Creates target schemas if they don't exist
4. **Data Upload**: Uses Snowflake's `write_pandas` for efficient bulk loading
5. **Logging**: Comprehensive logging to both console and `logs/snowflake_migration.log`

## Important Notes

- **Table Types**: Only migrates `BASE TABLE` types (excludes views, dynamic tables, etc.)
- **Overwrite Behaviour**: Target tables are overwritten if they already exist
- **SSO Authentication**: Uses `externalbrowser` authenticator for SSO
- **Error Handling**: Continues migrating other tables even if one fails
- **Logging**: All operations are logged with timestamps for audit trail

## Monitoring Progress

The script provides detailed logging including:
- Connection establishment
- Table discovery
- Row counts for each table
- Success/failure status
- Final summary

Check the console output or `logs/snowflake_migration.log` for detailed progress.

## Troubleshooting

### Common Issues

1. **Role Access**: Ensure your user has access to both required roles
2. **Database Permissions**: Verify schema creation permissions in target database
3. **Network**: SSO authentication requires browser access
4. **Large Tables**: For very large tables, consider using `--table` option for individual migration

### Error Recovery

If the migration fails for specific tables:
1. Check the error in the logs
2. Use `--schema` and `--table` options to retry specific failures
3. Ensure target database has sufficient storage and permissions 