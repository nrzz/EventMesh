$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Push-Location $root

try {
    Write-Host "Restoring EventMesh solution..."
    dotnet restore EventMesh.slnx

    Write-Host "Building EventMesh solution (Release)..."
    dotnet build EventMesh.slnx --configuration Release --no-restore

    Write-Host "Build completed successfully."
}
finally {
    Pop-Location
}
