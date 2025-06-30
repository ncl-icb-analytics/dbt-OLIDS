#!/usr/bin/env python3
"""
Snowflake Table Migration Script

Migrates static tables from DATA_LAB_OLIDS_UAT database (CODESETS and RULESETS schemas)
to DATA_LAB_OLIDS_UAT database using role switching and SSO authentication.

Usage:
    python scripts/migrate_snowflake_tables.py [--dry-run] [--schema SCHEMA] [--table TABLE]

Environment Variables (from .env file):
    SNOWFLAKE_ACCOUNT: Snowflake account identifier
    SNOWFLAKE_USER: Username for authentication
    SNOWFLAKE_WAREHOUSE: Warehouse to use for operations

Roles Used:
    - NCL-USERGROUP-STAFF-BI-ADMIN: For reading from DATA_LAB_OLIDS_UAT
    - ISL-USERGROUP-SECONDEES-NCL: For writing to DATA_LAB_OLIDS_UAT
"""

import os
import sys
import argparse
import logging
from typing import List, Dict, Any, Optional
from pathlib import Path
import snowflake.connector
from snowflake.connector import DictCursor
from dotenv import load_dotenv
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/snowflake_migration.log', mode='a'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class SnowflakeMigrator:
    """Handles Snowflake table migration with role switching."""
    
    def __init__(self):
        """Initialise the migrator with environment configuration."""
        self.load_environment()
        self.source_role = "NCL-USERGROUP-STAFF-SNOWFLAKE-BI-ADMIN"
        self.target_role = "ISL-USERGROUP-SECONDEES-NCL"
        self.source_database = "DATA_LAB_OLIDS_UAT"
        self.target_database = "DATA_LAB_OLIDS_UAT"
        self.schemas = ["CODESETS", "RULESETS"]
        
        # Connection objects
        self.source_conn = None
        self.target_conn = None
        
    def load_environment(self) -> None:
        """Load environment variables from .env file."""
        env_path = Path(__file__).parent.parent / '.env'
        if not env_path.exists():
            raise FileNotFoundError(
                f"Environment file not found at {env_path}. "
                "Please copy env.example to .env and configure your credentials."
            )
        
        load_dotenv(env_path)
        
        required_vars = ['SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_WAREHOUSE']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        self.account = os.getenv('SNOWFLAKE_ACCOUNT')
        self.user = os.getenv('SNOWFLAKE_USER')
        self.warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
        
        logger.info(f"Loaded environment for account: {self.account}")
    
    def create_connection(self, role: str) -> snowflake.connector.SnowflakeConnection:
        """Create a Snowflake connection with SSO authentication."""
        try:
            logger.info(f"Establishing SSO connection with role: {role}")
            
            conn = snowflake.connector.connect(
                user=self.user,
                account=self.account,
                authenticator='externalbrowser',  # SSO authentication
                role=role,
                warehouse=self.warehouse
            )
            
            logger.info(f"Successfully connected to Snowflake with role: {role}")
            return conn
            
        except Exception as e:
            logger.error(f"Failed to connect with role {role}: {str(e)}")
            raise
    
    def get_static_tables(self, schema: str) -> List[str]:
        """Retrieve list of static tables (excluding views, dynamic tables, etc.)."""
        if not self.source_conn:
            raise ValueError("Source connection not established")
        
        query = f"""
        SELECT table_name
        FROM {self.source_database}.INFORMATION_SCHEMA.TABLES
        WHERE table_schema = '{schema}'
          AND table_type = 'BASE TABLE'
        ORDER BY table_name;
        """
        
        try:
            cursor = self.source_conn.cursor(DictCursor)
            cursor.execute(query)
            tables = [row['TABLE_NAME'] for row in cursor.fetchall()]
            cursor.close()
            
            logger.info(f"Found {len(tables)} static tables in {schema} schema: {', '.join(tables)}")
            return tables
            
        except Exception as e:
            logger.error(f"Failed to retrieve tables from {schema}: {str(e)}")
            raise
    
    def get_table_data(self, schema: str, table: str) -> pd.DataFrame:
        """Extract data from a source table."""
        if not self.source_conn:
            raise ValueError("Source connection not established")
        
        query = f"SELECT * FROM {self.source_database}.{schema}.{table};"
        
        try:
            logger.info(f"Extracting data from {schema}.{table}")
            df = pd.read_sql(query, self.source_conn)
            logger.info(f"Extracted {len(df)} rows from {schema}.{table}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to extract data from {schema}.{table}: {str(e)}")
            raise
    
    def create_target_schema(self, schema: str) -> None:
        """Create schema in target database if it doesn't exist."""
        if not self.target_conn:
            raise ValueError("Target connection not established")
        
        try:
            cursor = self.target_conn.cursor()
            cursor.execute(f"USE DATABASE {self.target_database};")
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            cursor.close()
            
            logger.info(f"Ensured schema {schema} exists in {self.target_database}")
            
        except Exception as e:
            logger.error(f"Failed to create schema {schema}: {str(e)}")
            raise
    
    def upload_table_data(self, schema: str, table: str, df: pd.DataFrame) -> None:
        """Upload data to target table."""
        if not self.target_conn:
            raise ValueError("Target connection not established")
        
        if df.empty:
            logger.warning(f"No data to upload for {schema}.{table}")
            return
        
        try:
            # Use Snowflake's write_pandas method for efficient bulk loading
            from snowflake.connector.pandas_tools import write_pandas
            
            logger.info(f"Uploading {len(df)} rows to {self.target_database}.{schema}.{table}")
            
            # Create or replace the table with the data
            success, nchunks, nrows, _ = write_pandas(
                conn=self.target_conn,
                df=df,
                table_name=table,
                database=self.target_database,
                schema=schema,
                auto_create_table=True,
                overwrite=True
            )
            
            if success:
                logger.info(f"Successfully uploaded {nrows} rows to {schema}.{table}")
            else:
                raise Exception("Upload failed - no success flag returned")
                
        except Exception as e:
            logger.error(f"Failed to upload data to {schema}.{table}: {str(e)}")
            raise
    
    def migrate_table(self, schema: str, table: str, dry_run: bool = False) -> bool:
        """Migrate a single table from source to target."""
        try:
            # Extract data from source
            df = self.get_table_data(schema, table)
            
            if dry_run:
                logger.info(f"DRY RUN: Would migrate {len(df)} rows from {schema}.{table}")
                return True
            
            # Create target schema if needed
            self.create_target_schema(schema)
            
            # Upload to target
            self.upload_table_data(schema, table, df)
            
            logger.info(f"Successfully migrated {schema}.{table}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to migrate {schema}.{table}: {str(e)}")
            return False
    
    def migrate_schema(self, schema: str, specific_table: Optional[str] = None, dry_run: bool = False) -> Dict[str, bool]:
        """Migrate all tables in a schema or a specific table."""
        results = {}
        
        try:
            if specific_table:
                tables = [specific_table]
                logger.info(f"Migrating specific table: {schema}.{specific_table}")
            else:
                tables = self.get_static_tables(schema)
                logger.info(f"Migrating all tables in schema: {schema}")
            
            for table in tables:
                results[f"{schema}.{table}"] = self.migrate_table(schema, table, dry_run)
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to migrate schema {schema}: {str(e)}")
            return results
    
    def run_migration(self, specific_schema: Optional[str] = None, specific_table: Optional[str] = None, dry_run: bool = False) -> None:
        """Run the complete migration process."""
        logger.info("Starting Snowflake table migration")
        
        try:
            # Establish connections
            logger.info("Establishing source connection...")
            self.source_conn = self.create_connection(self.source_role)
            
            logger.info("Establishing target connection...")
            self.target_conn = self.create_connection(self.target_role)
            
            # Determine which schemas to migrate
            schemas_to_migrate = [specific_schema] if specific_schema else self.schemas
            
            all_results = {}
            
            for schema in schemas_to_migrate:
                logger.info(f"Processing schema: {schema}")
                schema_results = self.migrate_schema(schema, specific_table, dry_run)
                all_results.update(schema_results)
            
            # Summary
            successful = sum(1 for success in all_results.values() if success)
            total = len(all_results)
            
            logger.info(f"Migration complete: {successful}/{total} tables migrated successfully")
            
            if not dry_run and successful < total:
                failed_tables = [table for table, success in all_results.items() if not success]
                logger.warning(f"Failed migrations: {', '.join(failed_tables)}")
            
        except Exception as e:
            logger.error(f"Migration failed: {str(e)}")
            raise
        
        finally:
            # Close connections
            if self.source_conn:
                self.source_conn.close()
                logger.info("Source connection closed")
            
            if self.target_conn:
                self.target_conn.close()
                logger.info("Target connection closed")


def main():
    """Main entry point for the migration script."""
    parser = argparse.ArgumentParser(
        description="Migrate Snowflake tables between databases with role switching"
    )
    parser.add_argument(
        '--dry-run', 
        action='store_true', 
        help="Perform a dry run without actually migrating data"
    )
    parser.add_argument(
        '--schema', 
        choices=['CODESETS', 'RULESETS'], 
        help="Migrate only a specific schema"
    )
    parser.add_argument(
        '--table', 
        help="Migrate only a specific table (requires --schema)"
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.table and not args.schema:
        parser.error("--table requires --schema to be specified")
    
    # Ensure logs directory exists
    logs_dir = Path(__file__).parent.parent / 'logs'
    logs_dir.mkdir(exist_ok=True)
    
    try:
        migrator = SnowflakeMigrator()
        migrator.run_migration(
            specific_schema=args.schema,
            specific_table=args.table,
            dry_run=args.dry_run
        )
        
    except Exception as e:
        logger.error(f"Migration script failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main() 