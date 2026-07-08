$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Push-Location $root

try {
    Write-Host "Building benchmarks..."
    dotnet build benchmarks/EventMesh.Benchmarks/EventMesh.Benchmarks.csproj --configuration Release

    Write-Host "Running EventMesh benchmarks..."
    dotnet run --project benchmarks/EventMesh.Benchmarks/EventMesh.Benchmarks.csproj --configuration Release --no-build

    Write-Host "Benchmark run completed."
}
finally {
    Pop-Location
}
