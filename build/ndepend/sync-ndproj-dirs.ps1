# Back-compat wrapper. Prefer: pwsh build/ndepend/sync-ndproj.ps1
& (Join-Path $PSScriptRoot 'sync-ndproj.ps1') @args
