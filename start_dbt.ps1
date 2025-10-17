# start_dbt.ps1

# Activate virtual environment
Write-Host "Activating Python virtual environment..." -ForegroundColor Cyan
$venvPath = "venv\Scripts\Activate.ps1"

if (Test-Path $venvPath) {
    & $venvPath
    Write-Host "[OK] Virtual environment activated" -ForegroundColor Green
    Write-Host "  Python: $(python --version 2>&1)" -ForegroundColor Gray
} else {
    Write-Host "[ERROR] Virtual environment not found" -ForegroundColor Red
    Write-Host "  Run 'python -m venv venv' to create it first" -ForegroundColor Yellow
    exit 1
}
Write-Host ""

# Disable AWS metadata service checks (prevents connection pool warnings on Azure)
[System.Environment]::SetEnvironmentVariable('AWS_EC2_METADATA_DISABLED', 'true', 'Process')

# Load project-specific environment variables
Write-Host "Loading environment variables from .env..." -ForegroundColor Cyan

$envPath = ".env"
if (Test-Path $envPath) {
    $envCount = 0
    Get-Content $envPath | ForEach-Object {
      if ($_ -match '^([^=]+)=(.*)$' -and -not $_.StartsWith('#')) {
          [System.Environment]::SetEnvironmentVariable($matches[1], $matches[2], 'Process')
          $envCount++
      }
    }
    Write-Host "[OK] Loaded $envCount environment variables" -ForegroundColor Green

    # Show configured variables
    if ($env:SNOWFLAKE_ACCOUNT) {
        Write-Host "  SNOWFLAKE_ACCOUNT: $env:SNOWFLAKE_ACCOUNT" -ForegroundColor Gray
    }
    if ($env:SNOWFLAKE_USER) {
        Write-Host "  SNOWFLAKE_USER: $env:SNOWFLAKE_USER" -ForegroundColor Gray
    }
    if ($env:SNOWFLAKE_ROLE) {
        Write-Host "  SNOWFLAKE_ROLE: $env:SNOWFLAKE_ROLE" -ForegroundColor Gray
    }
    if ($env:SNOWFLAKE_WAREHOUSE) {
        Write-Host "  SNOWFLAKE_WAREHOUSE: $env:SNOWFLAKE_WAREHOUSE" -ForegroundColor Gray
    }
    if ($env:SNOWFLAKE_TARGET_DATABASE) {
        Write-Host "  SNOWFLAKE_TARGET_DATABASE: $env:SNOWFLAKE_TARGET_DATABASE" -ForegroundColor Gray
    }
} else {
    Write-Host "[WARNING] No .env file found" -ForegroundColor Yellow
    Write-Host "  Copy env.example to .env and add your credentials" -ForegroundColor Gray
}
Write-Host ""

Write-Host "Ready! You can now run dbt commands." -ForegroundColor Green
Write-Host "Try: dbt debug (to test your connection)" -ForegroundColor Gray