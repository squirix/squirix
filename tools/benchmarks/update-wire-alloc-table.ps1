#Requires -Version 7.0
[CmdletBinding()]
param(
    [string] $ResultsPath,
    [string] $ScalarResultsPath,
    [string] $StructuredResultsPath,
    [string] $ArtifactsDir,
    [string] $GitSha = '',
    [string] $Branch = '',
    [string] $MarkdownPath = (Join-Path $PSScriptRoot '..\..\docs\benchmarks\wire-alloc-baseline.md'),
    [int] $OperationsPerInvoke = 512
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Resolve-ArtifactsDir {
    param([string] $Path)

    if ($Path) {
        if (-not (Test-Path -LiteralPath $Path)) {
            throw "Artifacts directory not found: $Path"
        }

        return (Resolve-Path -LiteralPath $Path).Path
    }

    $default = Join-Path (Get-Location) 'BenchmarkDotNet.Artifacts\results'
    if (-not (Test-Path -LiteralPath $default)) {
        throw 'No artifacts directory found. Run benchmarks with --exporters json first.'
    }

    return (Resolve-Path -LiteralPath $default).Path
}

function Resolve-TypeReportPath {
    param(
        [string] $Directory,
        [string] $TypeName,
        [string] $ExplicitPath
    )

    if ($ExplicitPath) {
        if (-not (Test-Path -LiteralPath $ExplicitPath)) {
            throw "Results file not found: $ExplicitPath"
        }

        return (Resolve-Path -LiteralPath $ExplicitPath).Path
    }

    $patterns = @(
        "*.$TypeName-report-full.json",
        "*$TypeName-report-full.json",
        "*.$TypeName-report.json",
        "*$TypeName-report.json"
    )

    foreach ($pattern in $patterns) {
        $match = Get-ChildItem -LiteralPath $Directory -Filter $pattern |
            Sort-Object LastWriteTime -Descending |
            Select-Object -First 1

        if ($match) {
            return $match.FullName
        }
    }

    throw "No BenchmarkDotNet JSON report found for $TypeName under $Directory"
}

function Get-ApiLabel {
    param([string] $MethodName)

    $map = [ordered]@{
        'SetAsync' = 'SetAsync'
        'GetValueAsync' = 'GetValueAsync'
        'GetEntryAsync' = 'GetEntryAsync'
        'TryAddAsync' = 'TryAddAsync'
        'AddAsync' = 'AddAsync'
        'UpdateAsync' = 'UpdateAsync'
        'RemoveAsync' = 'RemoveAsync'
        'GetOrAddAsyncHitAsync' = 'GetOrAddAsync (hit)'
        'GetOrAddAsyncMissAsync' = 'GetOrAddAsync (miss)'
        'GetExpirationAsync' = 'GetExpirationAsync'
        'RemoveExpirationAsync' = 'RemoveExpirationAsync'
        'TouchRelativeAsync' = 'TouchAsync (relative)'
        'TouchAbsoluteAsync' = 'TouchAsync (absolute)'
    }

    if ($map.Contains($MethodName)) {
        return $map[$MethodName]
    }

    return $MethodName
}

function Format-Number {
    param(
        [double] $Value,
        [int] $Digits = 2
    )

    return [Math]::Round($Value, $Digits, [MidpointRounding]::AwayFromZero).ToString("F$Digits", [Globalization.CultureInfo]::InvariantCulture)
}

function Build-TableRows {
    param(
        [object[]] $Benchmarks,
        [string] $TypeName
    )

    $order = @(
        'SetAsync',
        'GetValueAsync',
        'GetEntryAsync',
        'TryAddAsync',
        'AddAsync',
        'UpdateAsync',
        'RemoveAsync',
        'GetOrAddAsyncHitAsync',
        'GetOrAddAsyncMissAsync',
        'GetExpirationAsync',
        'RemoveExpirationAsync',
        'TouchRelativeAsync',
        'TouchAbsoluteAsync'
    )

    $byMethod = @{}
    foreach ($benchmark in $Benchmarks) {
        if ($benchmark.Type -ne $TypeName) {
            continue
        }

        $byMethod[$benchmark.Method] = $benchmark
    }

    $rows = New-Object System.Collections.Generic.List[string]
    foreach ($method in $order) {
        if (-not $byMethod.ContainsKey($method)) {
            throw "Missing benchmark row for $TypeName.$method"
        }

        $item = $byMethod[$method]
        if (-not $item.Statistics) {
            throw "Benchmark $TypeName.$method has no statistics. Re-run with --iterationCount 3 or higher."
        }

        $meanNsPerOp = [double]$item.Statistics.Mean / $OperationsPerInvoke
        $allocatedPerOp = [double]$item.Memory.BytesAllocatedPerOperation
        $gen0Metric = $item.Metrics | Where-Object { $_.Descriptor.Id -eq 'Gen0Collects' } | Select-Object -First 1
        $gen0PerOp = if ($gen0Metric) { [double]$gen0Metric.Value / 1000.0 } else { 0.0 }
        $label = Get-ApiLabel -MethodName $method

        $rows.Add("| $label | $(Format-Number $meanNsPerOp 0) | $(Format-Number $allocatedPerOp 2) | $(Format-Number $gen0PerOp 4) |")
    }

    return $rows
}

function Update-MarkerSection {
    param(
        [string] $Content,
        [string] $StartMarker,
        [string] $EndMarker,
        [string[]] $Rows
    )

    $header = @(
        '| ICache API | Mean (ns/op) | Allocated (bytes/op) | Gen0 |',
        '| ------------ | -------------: | ---------------------: | -----: |'
    )

    $section = @($header + $Rows) -join "`n"
    $pattern = "(?s)($([regex]::Escape($StartMarker)))\s*.*?\s*($([regex]::Escape($EndMarker)))"
    return [regex]::Replace($Content, $pattern, "`$1`n$section`n`$2")
}

$artifactsDirectory = Resolve-ArtifactsDir -Path $ArtifactsDir
$scalarReportPath = Resolve-TypeReportPath -Directory $artifactsDirectory -TypeName 'CacheWireScalarAllocBenchmarks' -ExplicitPath $(if ($ScalarResultsPath) { $ScalarResultsPath } else { $ResultsPath })
$structuredReportPath = Resolve-TypeReportPath -Directory $artifactsDirectory -TypeName 'CacheWireStructuredAllocBenchmarks' -ExplicitPath $StructuredResultsPath

$scalarReport = Get-Content -LiteralPath $scalarReportPath -Raw | ConvertFrom-Json
$structuredReport = Get-Content -LiteralPath $structuredReportPath -Raw | ConvertFrom-Json

if (-not $scalarReport.Benchmarks) {
    throw "No benchmarks found in $scalarReportPath"
}

if (-not $structuredReport.Benchmarks) {
    throw "No benchmarks found in $structuredReportPath"
}

$scalarRows = Build-TableRows -Benchmarks $scalarReport.Benchmarks -TypeName 'CacheWireScalarAllocBenchmarks'
$structuredRows = Build-TableRows -Benchmarks $structuredReport.Benchmarks -TypeName 'CacheWireStructuredAllocBenchmarks'

if (-not (Test-Path -LiteralPath $MarkdownPath)) {
    throw "Markdown file not found: $MarkdownPath"
}

$content = Get-Content -LiteralPath $MarkdownPath -Raw
$content = Update-MarkerSection -Content $content -StartMarker '<!-- wire-alloc-scalar-start -->' -EndMarker '<!-- wire-alloc-scalar-end -->' -Rows $scalarRows
$content = Update-MarkerSection -Content $content -StartMarker '<!-- wire-alloc-structured-start -->' -EndMarker '<!-- wire-alloc-structured-end -->' -Rows $structuredRows

if ($GitSha) {
    $content = $content -replace '\| Git SHA \| _pending_ \|', "| Git SHA | ``$GitSha`` |"
}

if ($Branch) {
    $content = $content -replace '\| Branch \| _pending_ \|', "| Branch | ``$Branch`` |"
}

$utcNow = [DateTime]::UtcNow.ToString('yyyy-MM-dd HH:mm:ss', [Globalization.CultureInfo]::InvariantCulture)
$content = $content -replace '\| Date \(UTC\) \| _pending_ \|', "| Date (UTC) | $utcNow |"

Set-Content -LiteralPath $MarkdownPath -Value $content -NoNewline -Encoding utf8
Write-Host "Updated $MarkdownPath"
Write-Host "  scalar: $($scalarReportPath)"
Write-Host "  structured: $($structuredReportPath)"
