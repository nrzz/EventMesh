$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Push-Location $root

try {
    Write-Host "Running EventMesh tests with coverage..."
    dotnet test EventMesh.slnx `
        --configuration Release `
        --verbosity normal `
        --collect:"XPlat Code Coverage" `
        --results-directory ./TestResults `
        /p:CollectCoverage=true `
        /p:CoverletOutputFormat=cobertura `
        /p:CoverletOutput=./TestResults/coverage/

    Write-Host "Tests completed successfully."
}
finally {
    Pop-Location
}
