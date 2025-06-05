import os
import re
from pathlib import Path
from typing import Dict, List, Set
import networkx as nx
from collections import defaultdict

class SQLDependencyBuilder:
    def __init__(self, project_root: str):
        self.project_root = Path(project_root)
        self.sql_files = {}  # filename -> full path
        self.dependencies = defaultdict(set)  # filename -> set of dependencies
        self.graph = nx.DiGraph()
        
    def find_sql_files(self) -> None:
        """Find all SQL files in the project"""
        for path in self.project_root.rglob("*.sql"):
            # Skip files in .git directory
            if ".git" in path.parts:
                continue
            # Get relative path from project root
            rel_path = path.relative_to(self.project_root)
            # Use the table name (filename without extension) as the key
            table_name = path.stem.upper()
            self.sql_files[table_name] = rel_path
            self.graph.add_node(table_name)
    
    def extract_dependencies(self, sql_content: str, current_table: str) -> Set[str]:
        """Extract table names from SQL content, being more precise about actual dependencies"""
        # Remove comments to avoid false positives
        sql_content = re.sub(r'--.*$', '', sql_content, flags=re.MULTILINE)  # Remove single line comments
        sql_content = re.sub(r'/\*.*?\*/', '', sql_content, flags=re.DOTALL)  # Remove multi-line comments
        
        # Look for actual table references in SQL patterns
        # Only consider FROM and JOIN clauses that are part of the main query or CTEs
        # Ignore references in comments, string literals, or subqueries that are just for filtering
        patterns = [
            # FROM clauses in main query or CTEs
            r'(?:^|\n)\s*(?:WITH\s+[^;]*?)?FROM\s+([A-Za-z0-9_\.]+)(?:\s+(?:AS\s+)?[A-Za-z0-9_]+)?(?:\s+(?:WHERE|JOIN|GROUP|ORDER|HAVING|LIMIT|$))',
            # JOIN clauses in main query or CTEs
            r'(?:^|\n)\s*(?:WITH\s+[^;]*?)?(?:INNER|LEFT|RIGHT|FULL|CROSS)?\s+JOIN\s+([A-Za-z0-9_\.]+)(?:\s+(?:AS\s+)?[A-Za-z0-9_]+)?(?:\s+(?:ON|USING|WHERE|GROUP|ORDER|HAVING|LIMIT|$))',
            # INSERT/UPDATE/MERGE target tables
            r'(?:INSERT\s+INTO|UPDATE|MERGE\s+INTO)\s+([A-Za-z0-9_\.]+)(?:\s+(?:AS\s+)?[A-Za-z0-9_]+)?(?:\s+(?:SELECT|SET|WHEN|$))',
        ]
        
        dependencies = set()
        for pattern in patterns:
            matches = re.finditer(pattern, sql_content, re.IGNORECASE | re.MULTILINE)
            for match in matches:
                # Extract table name (handle schema.table format)
                table_ref = match.group(1).split('.')[-1].upper()
                # Only add if it's a different table and exists in our files
                if table_ref != current_table and table_ref in self.sql_files:
                    # Additional validation: check if the table is actually used in a meaningful way
                    # by looking for it in SELECT, WHERE, or JOIN conditions
                    table_usage = re.search(
                        rf'(?:SELECT|WHERE|JOIN|ON|USING).*?{table_ref}',
                        sql_content,
                        re.IGNORECASE
                    )
                    if table_usage:
                        dependencies.add(table_ref)
        
        return dependencies
    
    def build_dependency_graph(self) -> None:
        """Build dependency graph by parsing all SQL files"""
        for table_name, file_path in self.sql_files.items():
            try:
                with open(self.project_root / file_path, 'r') as f:
                    content = f.read()
                    deps = self.extract_dependencies(content, table_name)
                    self.dependencies[table_name] = deps
                    # Add edges to graph (dependencies point to dependent)
                    for dep in deps:
                        self.graph.add_edge(dep, table_name)
            except Exception as e:
                print(f"Error processing {file_path}: {e}")
    
    def get_execution_order(self) -> List[str]:
        """Get files in correct execution order using topological sort"""
        try:
            # Check for cycles
            if not nx.is_directed_acyclic_graph(self.graph):
                cycles = list(nx.simple_cycles(self.graph))
                print("WARNING: Potential circular dependencies found:")
                for cycle in cycles:
                    print(" -> ".join(cycle))
                    # Print the actual dependencies for each table in the cycle
                    for table in cycle:
                        print(f"  {table} depends on: {', '.join(sorted(self.dependencies[table]))}")
                return []
            
            # Get topological sort
            return list(nx.topological_sort(self.graph))
        except nx.NetworkXUnfeasible:
            print("ERROR: Circular dependencies prevent determining execution order")
            return []
    
    def print_execution_plan(self) -> None:
        """Print the execution plan in a readable format"""
        order = self.get_execution_order()
        if not order:
            return
        
        print("\nExecution Order:")
        print("===============")
        for i, table_name in enumerate(order, 1):
            deps = self.dependencies[table_name]
            print(f"{i:3d}. {table_name}")
            if deps:
                print(f"     Dependencies: {', '.join(sorted(deps))}")
            print(f"     File: {self.sql_files[table_name]}")
            print()
    
    def get_table_dependencies(self, table_name: str) -> Set[str]:
        """Get all dependencies for a specific table"""
        if table_name not in self.sql_files:
            return set()
        
        # Get direct dependencies
        direct_deps = self.dependencies[table_name]
        
        # Get indirect dependencies
        indirect_deps = set()
        for dep in direct_deps:
            indirect_deps.update(self.get_table_dependencies(dep))
        
        return direct_deps | indirect_deps

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Build and analyze SQL file dependencies')
    parser.add_argument('--table', help='Show dependencies for a specific table')
    args = parser.parse_args()
    
    # Get project root (assuming script is in scripts directory)
    project_root = Path(__file__).parent.parent
    
    # Build dependency graph
    builder = SQLDependencyBuilder(project_root)
    print("Finding SQL files...")
    builder.find_sql_files()
    print(f"Found {len(builder.sql_files)} SQL files")
    
    print("\nBuilding dependency graph...")
    builder.build_dependency_graph()
    
    if args.table:
        # Show dependencies for specific table
        table_name = args.table.upper()
        if table_name not in builder.sql_files:
            print(f"Error: Table {table_name} not found in SQL files")
            exit(1)
        
        print(f"\nDependencies for {table_name}:")
        print("========================")
        deps = builder.get_table_dependencies(table_name)
        if deps:
            print("Direct and indirect dependencies:")
            for dep in sorted(deps):
                print(f"- {dep}")
        else:
            print("No dependencies found")
    else:
        # Show full execution plan
        print("\nGenerating execution plan...")
        builder.print_execution_plan()

if __name__ == "__main__":
    main() 