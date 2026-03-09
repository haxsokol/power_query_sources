param(
    [Parameter(Mandatory = $false)]
    [string]$IndexUrl,
    [Parameter(Mandatory = $false)]
    [string]$PythonBin
)

$ErrorActionPreference = "Stop"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$envFile = Join-Path $scriptDir ".env"

if (Test-Path $envFile) {
    Get-Content -Path $envFile | ForEach-Object {
        $line = $_.Trim()
        if (-not $line -or $line.StartsWith("#")) {
            return
        }

        if ($line.StartsWith("export ")) {
            $line = $line.Substring(7).Trim()
        }

        $parts = $line.Split("=", 2)
        if ($parts.Count -ne 2) {
            return
        }

        $key = $parts[0].Trim()
        $value = $parts[1].Trim()
        if ([string]::IsNullOrWhiteSpace($key)) {
            return
        }

        if ((($value.StartsWith('"') -and $value.EndsWith('"')) -or ($value.StartsWith("'") -and $value.EndsWith("'"))) -and $value.Length -ge 2) {
            $value = $value.Substring(1, $value.Length - 2)
        }

        if (-not (Test-Path "env:$key") -or [string]::IsNullOrWhiteSpace((Get-Item "env:$key").Value)) {
            Set-Item -Path "env:$key" -Value $value
        }
    }
}

if ([string]::IsNullOrWhiteSpace($PythonBin)) {
    $PythonBin = $env:PYTHON_BIN
}

$pythonExe = $null
$pythonPrefixArgs = @()

if (-not [string]::IsNullOrWhiteSpace($PythonBin)) {
    & $PythonBin -c "import sys" *> $null
    if ($LASTEXITCODE -ne 0) {
        Write-Error "PYTHON_BIN is set but does not work: $PythonBin"
    }
    $pythonExe = $PythonBin
}
else {
    $candidates = @(
        @{ Exe = "python"; Args = @() },
        @{ Exe = "py"; Args = @("-3") }
    )

    foreach ($candidate in $candidates) {
        if (-not (Get-Command $candidate.Exe -ErrorAction SilentlyContinue)) {
            continue
        }

        & $candidate.Exe @($candidate.Args) -c "import sys" *> $null
        if ($LASTEXITCODE -eq 0) {
            $pythonExe = $candidate.Exe
            $pythonPrefixArgs = @($candidate.Args)
            break
        }
    }

    if ($null -eq $pythonExe) {
        Write-Error "No working Python found. Install Python 3 or set PYTHON_BIN in .env."
    }
}

if ([string]::IsNullOrWhiteSpace($IndexUrl)) {
    $IndexUrl = if ([string]::IsNullOrWhiteSpace($env:CORP_PIP_INDEX_URL)) { $env:PIP_INDEX_URL } else { $env:CORP_PIP_INDEX_URL }
}

$venvDir = if ([string]::IsNullOrWhiteSpace($env:VENV_DIR)) { Join-Path $scriptDir ".venv" } else { $env:VENV_DIR }
if (-not [System.IO.Path]::IsPathRooted($venvDir)) {
    $venvDir = Join-Path $scriptDir $venvDir
}

$tomlDir = Join-Path $scriptDir "toml_files"
$outputDir = Join-Path $scriptDir "find_source_excel"

& $pythonExe @pythonPrefixArgs -m venv $venvDir
New-Item -ItemType Directory -Force -Path $tomlDir | Out-Null
New-Item -ItemType Directory -Force -Path $outputDir | Out-Null

$venvPython = Join-Path $venvDir "Scripts\python.exe"
$requirementsPath = Join-Path $scriptDir "requirements.txt"

& $venvPython -m pip install --upgrade pip

Write-Host "Trying to install dependencies from default PyPI..."
& $venvPython -m pip install -r $requirementsPath
if ($LASTEXITCODE -eq 0) {
    Write-Host "Dependencies installed from default PyPI."
}
else {
    if ([string]::IsNullOrWhiteSpace($IndexUrl)) {
        Write-Error "Default PyPI install failed and CORP_PIP_INDEX_URL is not set. Fill CORP_PIP_INDEX_URL in .env or pass -IndexUrl."
    }

    Write-Host "Default PyPI install failed. Retrying with corporate index..."
    & $venvPython -m pip install --index-url $IndexUrl -r $requirementsPath
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Failed to install dependencies from both default PyPI and corporate index."
    }
}

Write-Host "Environment is ready."
Write-Host "Created folders: $tomlDir and $outputDir"
Write-Host "Put TOML files into: $tomlDir"
Write-Host "Excel output will be created in: $outputDir"
Write-Host "Activate with: `"$venvDir\Scripts\Activate.ps1`""