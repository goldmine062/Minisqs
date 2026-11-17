# Builds minisqs.jar on Windows (PowerShell). Needs JDK 21+.
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
if (Test-Path out) { Remove-Item -Recurse -Force out }
if (Test-Path minisqs.jar) { Remove-Item -Force minisqs.jar }
New-Item -ItemType Directory out | Out-Null
$files = Get-ChildItem -Recurse src\main\java -Filter *.java | ForEach-Object { $_.FullName }
javac -d out $files
if ($LASTEXITCODE -ne 0) { exit 1 }
Set-Content -Path out\manifest.txt -Value "Main-Class: com.minisqs.Cli" -Encoding ascii
jar --create --file minisqs.jar --manifest out\manifest.txt -C out com
if ($LASTEXITCODE -ne 0) { exit 1 }
Write-Host "Built minisqs.jar"
