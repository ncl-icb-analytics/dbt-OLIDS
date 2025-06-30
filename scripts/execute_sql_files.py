import os
import re
import subprocess
import logging
import json
from pathlib import Path
from typing import Dict, List, Set, Optional
from datetime import datetime
from build_dependency_graph import SQLDependencyBuilder

class SQLExecutor:
    def __init__(self, project_root: str, dry_run: bool = False,
                 connection: str = "data_lab_olids_uat",
                 test_table: Optional[str] = None):
        self.project_root = Path(project_root)
        self.dry_run = dry_run
        self.connection = connection
        self.test_table = test_table
        self.setup_logging()

        # SQL execution tracking
        self.execution_status = {}  # table_name -> (success, error_message)
        self.execution_order = []   # list of executed tables in order

        # Initialize dependency builder
        self.builder = SQLDependencyBuilder(project_root)

    def setup_logging(self):
        """Set up logging configuration"""
        log_dir = self.project_root / "logs"
        log_dir.mkdir(exist_ok=True)

        # Clean up old log files
        for old_log in log_dir.glob("sql_execution_*.log"):
            try:
                old_log.unlink()
            except Exception as e:
                print(f"Warning: Could not delete old log file {old_log}: {e}")

        # Create new log file
        log_file = log_dir / "sql_execution_latest.log"

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info("Starting new execution log")

    def execute_sql_file(self, table_name: str, file_path: Path) -> bool:
        """Execute a single SQL file using snow CLI"""
        try:
            if self.dry_run:
                self.logger.info(f"[DRY RUN] Would execute {table_name} from {file_path}")
                return True

            self.logger.info(f"Executing {table_name} from {file_path}")

            # Read the SQL file
            with open(file_path, 'r') as f:
                sql_content = f.read()

            # Create a temporary file with the SQL content
            temp_file = self.project_root / "temp" / f"{table_name}_temp.sql"
            temp_file.parent.mkdir(exist_ok=True)

            try:
                with open(temp_file, 'w') as f:
                    f.write(sql_content)

                # Execute using snow CLI with connection name
                cmd = [
                    "snow",
                    "sql",
                    "--connection", self.connection,  # Use --connection instead of --role
                    "--filename", str(temp_file),
                    "--silent"
                ]

                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    check=True
                )

                self.logger.info(f"Successfully executed {table_name}")
                self.execution_status[table_name] = (True, None)
                self.execution_order.append(table_name)
                return True

            finally:
                # Clean up temporary file
                if temp_file.exists():
                    temp_file.unlink()

        except subprocess.CalledProcessError as e:
            error_msg = f"Error executing {table_name}: {e.stderr}"
            self.logger.error(error_msg)
            self.execution_status[table_name] = (False, error_msg)
            return False
        except Exception as e:
            error_msg = f"Unexpected error executing {table_name}: {str(e)}"
            self.logger.error(error_msg)
            self.execution_status[table_name] = (False, error_msg)
            return False

    def execute_all(self) -> bool:
        """Execute SQL files in dependency order"""
        # Build dependency graph
        self.logger.info("Building dependency graph...")
        self.builder.find_sql_files()
        self.builder.build_dependency_graph()

        # Get execution order
        order = self.builder.get_execution_order()
        if not order:
            self.logger.error("Failed to determine execution order")
            return False

        # If in test mode, only execute the specified table and its dependencies
        if self.test_table:
            if self.test_table not in self.builder.sql_files:
                self.logger.error(f"Test table {self.test_table} not found in SQL files")
                return False

            # Get all dependencies for the test table
            test_deps = set()
            for table in order:
                if table == self.test_table or table in test_deps:
                    test_deps.add(table)
                    # Add all dependencies of this table
                    test_deps.update(self.builder.dependencies[table])

            # Filter order to only include test table and its dependencies
            order = [table for table in order if table in test_deps]
            self.logger.info(f"Test mode: Will execute {self.test_table} and its dependencies")
            self.logger.info(f"Dependencies to execute: {', '.join(sorted(test_deps))}")

        # Execute files in order
        self.logger.info(f"Found {len(order)} files to execute")
        success = True

        for table_name in order:
            file_path = self.builder.sql_files[table_name]
            if not self.execute_sql_file(table_name, self.project_root / file_path):
                success = False
                if not self.dry_run:
                    self.logger.error(f"Stopping execution due to error in {table_name}")
                    break

        # Print execution summary
        self.print_execution_summary()
        return success

    def print_execution_summary(self):
        """Print a summary of the execution results"""
        self.logger.info("\nExecution Summary:")
        self.logger.info("=================")

        # Count successes and failures
        success_count = sum(1 for status, _ in self.execution_status.values() if status)
        failure_count = len(self.execution_status) - success_count

        self.logger.info(f"Total files: {len(self.execution_status)}")
        self.logger.info(f"Successful: {success_count}")
        self.logger.info(f"Failed: {failure_count}")

        if failure_count > 0:
            self.logger.info("\nFailed executions:")
            for table_name, (success, error) in self.execution_status.items():
                if not success:
                    self.logger.info(f"- {table_name}: {error}")

        if self.dry_run:
            self.logger.info("\nThis was a dry run - no files were actually executed")

        if self.test_table:
            self.logger.info(f"\nTest mode execution for {self.test_table} completed")

def main():
    import argparse

    parser = argparse.ArgumentParser(description='Execute SQL files in dependency order')
    parser.add_argument('--dry-run', action='store_true',
                      help='Show what would be executed without actually running')
    parser.add_argument('--connection', default='data_lab_olids_uat',
                      help='Snow CLI connection to use (default: data_lab_olids_uat)')
    parser.add_argument('--test-table',
                      help='Execute only this table and its dependencies (e.g., DIM_PERSON_AGE)')
    args = parser.parse_args()

    # Get project root (assuming script is in scripts directory)
    project_root = Path(__file__).parent.parent

    # Execute SQL files
    executor = SQLExecutor(
        project_root,
        dry_run=args.dry_run,
        connection=args.connection,
        test_table=args.test_table
    )
    success = executor.execute_all()

    # Exit with appropriate status code
    exit(0 if success else 1)

if __name__ == "__main__":
    main()
