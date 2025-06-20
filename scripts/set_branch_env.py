#!/usr/bin/env python3
"""
Simple script to set DBT_BRANCH environment variable from current Git branch.
Handles branch name sanitization for Snowflake schema names.
"""

import subprocess
import sys
import os
from pathlib import Path

def get_current_branch():
    """Get the current Git branch name."""
    try:
        result = subprocess.run(
            ['git', 'branch', '--show-current'],
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        print("❌ Error: Could not determine Git branch. Are you in a Git repository?")
        sys.exit(1)

def sanitize_branch_name(branch_name):
    """Convert branch name to valid Snowflake schema name."""
    # Replace common Git branch characters with underscores
    sanitized = (branch_name
                .replace('/', '_')
                .replace('-', '_')
                .replace('.', '_')
                .upper())
    return sanitized

def update_env_file(branch_name):
    """Update or create .env file with DBT_BRANCH."""
    env_file = Path('.env')
    
    # Read existing .env if it exists
    env_vars = {}
    if env_file.exists():
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    env_vars[key] = value
    
    # Update DBT_BRANCH
    sanitized_branch = sanitize_branch_name(branch_name)
    env_vars['DBT_BRANCH'] = sanitized_branch
    
    # Write back to .env
    with open(env_file, 'w') as f:
        f.write("# dbt Environment Configuration\n")
        f.write(f"# Current Git branch: {branch_name}\n")
        f.write(f"# Generated automatically by scripts/set_branch_env.py\n\n")
        for key, value in env_vars.items():
            f.write(f"{key}={value}\n")
    
    return sanitized_branch

def main():
    print("🌿 Setting dbt branch environment")
    print("=" * 40)
    
    # Get current branch
    branch = get_current_branch()
    sanitized = update_env_file(branch)
    
    print(f"📂 Git branch: {branch}")
    print(f"🏗️  Schema name: DBT_{sanitized}")
    print(f"✅ Updated .env with DBT_BRANCH={sanitized}")
    
    print(f"\n💡 Your dev schema will be: DBT_{sanitized}")
    print(f"🚀 You can now run: dbt run")

if __name__ == "__main__":
    main() 