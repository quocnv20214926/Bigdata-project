# Loads variables from .env (repository root) into the current PowerShell session.
# Usage, from the repository root:   . .\scripts\load-env.ps1
# (the leading dot is required so the variables stay in your session)
$envFile = Join-Path (Split-Path $PSScriptRoot -Parent) ".env"
if (-not (Test-Path $envFile)) {
    Write-Error ".env not found. Copy .env.example to .env first."
    return
}
$count = 0
foreach ($line in Get-Content $envFile) {
    $line = $line.Trim()
    if ($line -eq "" -or $line.StartsWith("#")) { continue }
    $name, $value = $line -split "=", 2
    $name = $name.Trim()
    $value = $value.Trim().Trim('"').Trim("'")
    Set-Item -Path "Env:$name" -Value $value
    $count++
}
Write-Host "Loaded $count variables from .env"

# Shortcut for the real-time stack (reads the same .env)
function global:dc { docker compose --env-file .env -f infra/compose.base.yml -f infra/compose.stream.yml @args }
Write-Host "Command 'dc' is ready, e.g.  dc up -d --build"
