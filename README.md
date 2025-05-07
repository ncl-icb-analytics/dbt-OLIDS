# HealtheIntent Data Model Migration Project

## Overview

This project migrates data models from HealtheIntent (Vertica) to Snowflake. It contains SQL scripts to transform and adapt these models for Snowflake, creating intermediate, dimension, fact, and published tables.

## Project Structure

*   **`models/`**: Core SQL transformation scripts.
    *   **`codesets/`**: SQL scripts defining reference/lookup tables.
    *   **`dimensions/`**: SQL scripts for dimension tables.
    *   **`fact/`**: SQL scripts for fact tables.
    *   **`intermediate/`**: SQL scripts for intermediate transformation steps.
*   **`published/`**: Contains SQL scripts that create the final output tables for dashboards or reporting.
*   **`setup/`**: Scripts for initial database setup (e.g., DDL, schema creation). Contains `setup.sql`.
*   **`.git/`**: Git version control data.
*   **`.gitignore`**: Untracked files for Git to ignore.

## Setup & Execution (VS Code & Snowflake Extension)

1.  **Install Snowflake Extension**:
    *   In VS Code, go to the Extensions view (Ctrl+Shift+X).
    *   Search for "Snowflake" and install the official extension by Snowflake Inc.

2.  **Configure Connection**:
    *   Open the Snowflake extension.
    *   Add a new connection by providing your Snowflake account identifier, username, password (or set up key pair authentication), default role, and default warehouse.

3.  **Prepare Database**:
    *   Once connected, open the `setup/setup.sql` file.
    *   Execute this script using the Snowflake extension to prepare your database environment (e.g., create schemas, necessary base tables).

4.  **Run Models**:
    *   Execute the SQL scripts within the `models/` subdirectories and then the `published/` directory.
    *   Run scripts in a logical order if dependencies exist (e.g., intermediate models before facts/dimensions, and those before published models). You can execute them individually or as a batch if your setup allows, directly through the VS Code extension.

## Notes
*   Ensure your Snowflake user has the necessary permissions to create schemas, tables, and execute queries.
*   SQL dialect in the scripts should be compatible with Snowflake.

## Contributing

(Optional: Add guidelines for contributing to the project if applicable.)

## License

(Optional: Add license information if applicable.) 