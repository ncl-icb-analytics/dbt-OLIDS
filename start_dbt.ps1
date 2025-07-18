  # start_dbt.ps1
  # Load project-specific environment variables
  Get-Content ".env" | ForEach-Object {
    if ($_ -match '^([^=]+)=(.*)$' -and -not $_.StartsWith('#')) {
        [System.Environment]::SetEnvironmentVariable($matches[1],
$matches[2], 'Process')
    }
}