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
*   **`.gitignore`**: Untracked files ignored by Git. Create a `playarea` folder in the project root to create untracked local SQL scripts.

## Setup & Execution (dbt)

### Prerequisites
1. **Python 3.8+** installed
2. **Snowflake account** with appropriate permissions

### Initial Setup
1. **Clone and setup environment**:
   ```bash
   git clone <repository-url>
   cd snowflake-hei-migration-dbt
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Configure dbt profiles**:
   ```bash
   # Copy the template file
   cp profiles.yml.template profiles.yml
   
   # Copy the environment variables template
   cp env.example .env
   
   # Edit .env with your Snowflake connection details
   ```

3. **Set environment variables** (in `.env` file):
   ```env
   SNOWFLAKE_ACCOUNT=your_account
   SNOWFLAKE_USER=your_username
   SNOWFLAKE_ROLE=your_role
   SNOWFLAKE_WAREHOUSE=your_warehouse
   ```

4. **Test connection**:
   ```bash
   dbt debug
   ```

### Running the Project
1. **Install dependencies**:
   ```bash
   dbt deps
   ```

2. **Run all models**:
   ```bash
   dbt run
   ```

3. **Run tests**:
   ```bash
   dbt test
   ```

4. **Generate documentation**:
   ```bash
   dbt docs generate
   dbt docs serve
   ```

## Legacy Setup (VS Code & Snowflake Extension)

For reference, the original setup method using VS Code:

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
    *   Run scripts in a logical order if dependencies exist (e.g., intermediate models before facts/dimensions, and those before published models). You can execute them individually or as a batch through the VS Code extension.

## Notes
*   Ensure your Snowflake user has the necessary permissions to create schemas, tables, and execute queries.
*   SQL dialect in the scripts should be compatible with Snowflake.

## License

This repository is dual licensed under the Open Government v3 & MIT. All code outputs are subject to Crown Copyright.
