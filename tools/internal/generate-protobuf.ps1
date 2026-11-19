param(
    [string] $Protoc = 'protoc',
    [string] $IncludePath
)
$ErrorActionPreference = 'Stop'
$compiler = (Get-Command $Protoc -CommandType Application).Source
$version = & $compiler --version
if ($LASTEXITCODE -ne 0 -or $version -ne 'libprotoc 35.1') {
    throw "Expected libprotoc 35.1, found '$version'."
}
if (-not $IncludePath) {
    $IncludePath = Join-Path (Split-Path (Split-Path $compiler)) 'include'
}
$repo = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$schemas = @(
    @{ Source = 'src/shared/Squirix/Transport/Wire/Protos/SquirixCache.proto'; Output = 'src/shared/Squirix/Transport/Generated' },
    @{ Source = 'src/squirix.server/Adapters/Wire/Replication/SquirixReplication.proto'; Output = 'src/squirix.server/Adapters/Wire/Replication/Generated' }
)
foreach ($schema in $schemas) {
    $source = Join-Path $repo $schema.Source
    $output = Join-Path $repo $schema.Output
    & $compiler "-I$(Split-Path $source)" "-I$IncludePath" '--csharp_opt=internal_access' "--csharp_out=$output" $source
    if ($LASTEXITCODE -ne 0) { throw "Could not generate messages from $source." }
}
