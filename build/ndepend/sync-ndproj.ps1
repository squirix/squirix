# Restores squirix.ndproj settings after Visual NDepend rewrites them.
# Custom notmycode rules live in build/ndepend/squirix.ndrules and are inlined into
# squirix.ndproj <Queries>/Defining JustMyCode — Visual NDepend drops external RuleFiles references on save.
#
# Usage:
#   pwsh build/ndepend/sync-ndproj.ps1
#   pwsh build/ndepend/sync-ndproj.ps1 -RepoRoot C:\path\to\squirix
#
# Close Visual NDepend before running; reopen squirix.ndproj after.
# Prerequisite: dotnet build squirix.slnx -c Debug

param(
    [string]$RepoRoot = ''
)

$ErrorActionPreference = 'Stop'
if ([string]::IsNullOrWhiteSpace($RepoRoot)) {
    $repoRoot = Resolve-Path (Join-Path $PSScriptRoot '..\..')
} else {
    $repoRoot = Resolve-Path $RepoRoot
}
$ndprojPath = Join-Path $repoRoot 'squirix.ndproj'
$ndrulesPath = Join-Path $PSScriptRoot 'squirix.ndrules'
$justMyCodeGroupName = 'Defining JustMyCode'
$legacyCustomGroupName = 'squirix JustMyCode'
$customQueryMarker = '// <Name>'

[xml]$ndproj = Get-Content -LiteralPath $ndprojPath
[xml]$ndrules = Get-Content -LiteralPath $ndrulesPath

$runtimeProfileNode = $ndproj.NDepend.SelectSingleNode('RuntimeProfileDesc')
if ($null -eq $runtimeProfileNode) {
    throw 'Unexpected squirix.ndproj shape: RuntimeProfileDesc not found.'
}

$ideFilesNode = $ndproj.NDepend.SelectSingleNode('IDEFiles')
if ($null -ne $ideFilesNode) {
    $null = $ndproj.NDepend.RemoveChild($ideFilesNode)
}

$ideFilesNode = $ndproj.CreateElement('IDEFiles')
$ideFile = $ndproj.CreateElement('IDEFile')
$ideFile.SetAttribute('FilePath', '.\squirix.slnx')
$ideFile.SetAttribute('Filters', '')
$ideFile.SetAttribute('Configuration', 'DEBUG|AnyCPU')

$rootInfo = $ndproj.CreateElement('RootDirResolvingInfo')
$rootInfo.SetAttribute('Enabled', 'False')
$rootInfo.SetAttribute('Hints', 'Debug|bin|.bin|b|AnyCPU|x64|x86|v*.*|net*')
$rootInfo.SetAttribute('TimeOut', '10')

$rootDir = $ndproj.CreateElement('RootDir')
$rootDir.InnerText = '.'
$null = $rootInfo.AppendChild($rootDir)
$null = $ideFile.AppendChild($rootInfo)
$null = $ideFilesNode.AppendChild($ideFile)
$null = $ndproj.NDepend.InsertBefore($ideFilesNode, $runtimeProfileNode)

$queriesNode = $ndproj.NDepend.SelectSingleNode('Queries')
if ($null -eq $queriesNode) {
    throw 'Unexpected squirix.ndproj shape: Queries not found.'
}

$customQueriesNode = $ndrules.NDepend.Queries.CustomJustMyCodeQueries
if ($null -eq $customQueriesNode) {
    throw "Unexpected $ndrulesPath shape: Queries/CustomJustMyCodeQueries not found."
}

$legacyGroup = $queriesNode.SelectSingleNode("Group[@Name='$legacyCustomGroupName']")
if ($null -ne $legacyGroup) {
    $null = $queriesNode.RemoveChild($legacyGroup)
}

$justMyCodeGroup = $queriesNode.SelectSingleNode("Group[@Name='$justMyCodeGroupName']")
if ($null -eq $justMyCodeGroup) {
    throw "Unexpected squirix.ndproj shape: group '$justMyCodeGroupName' not found."
}

$existingCustomQueries = @(
    $justMyCodeGroup.SelectNodes('Query') |
        Where-Object { $_.InnerText -like "$customQueryMarker*" }
)
foreach ($query in $existingCustomQueries) {
    $null = $justMyCodeGroup.RemoveChild($query)
}

foreach ($query in $customQueriesNode.SelectNodes('Query')) {
    $importedQuery = $ndproj.ImportNode($query, $true)
    $null = $justMyCodeGroup.AppendChild($importedQuery)
}

$customRuleOverridesNode = $ndrules.NDepend.Queries.CustomRuleOverrides
if ($null -ne $customRuleOverridesNode) {
    foreach ($overrideQuery in $customRuleOverridesNode.SelectNodes('Query')) {
        $ruleToken = $overrideQuery.GetAttribute('RuleToken')
        $groupName = $overrideQuery.GetAttribute('Group')
        if ([string]::IsNullOrWhiteSpace($ruleToken) -or [string]::IsNullOrWhiteSpace($groupName)) {
            throw "CustomRuleOverrides Query must specify RuleToken and Group attributes."
        }

        $idTag = "// <Id>${ruleToken}:"
        if ($overrideQuery.InnerText -notlike "*$idTag*") {
            throw "Rule override '$ruleToken' must include '$idTag<ExplicitId></Id>' so NDepend shows the stock rule Id."
        }

        $idTag = "// <Id>${ruleToken}:"
        if ($overrideQuery.InnerText -notlike "*$idTag*") {
            throw "Rule override '$ruleToken' must include '$idTag<ExplicitId></Id>' so NDepend shows the stock rule Id."
        }

        $targetGroup = $queriesNode.SelectSingleNode(".//Group[@Name='$groupName']")
        if ($null -eq $targetGroup) {
            throw "Unexpected squirix.ndproj shape: group '$groupName' not found for rule override '$ruleToken'."
        }

        $placeholder = "`$${ruleToken}`$"
        $overrideMarker = "// ${ruleToken} squirix override:"
        $targetQuery = $targetGroup.SelectNodes('Query') | Where-Object {
            $_.InnerText -like "*$placeholder*" -or $_.InnerText -like "*$overrideMarker*"
        } | Select-Object -First 1
        if ($null -eq $targetQuery) {
            # Nested API groups may nest further; also search the whole Queries tree for the placeholder.
            $targetQuery = $queriesNode.SelectNodes('.//Query') | Where-Object {
                $_.InnerText -like "*$placeholder*" -or $_.InnerText -like "*$overrideMarker*"
            } | Select-Object -First 1
            if ($null -eq $targetQuery) {
                throw "Rule override '$ruleToken' placeholder '$placeholder' not found in group '$groupName'."
            }

            $targetGroup = $targetQuery.ParentNode
        }

        $importedQuery = $ndproj.ImportNode($overrideQuery, $true)
        $null = $importedQuery.RemoveAttribute('RuleToken')
        $null = $importedQuery.RemoveAttribute('Group')
        $null = $targetGroup.ReplaceChild($importedQuery, $targetQuery)
    }
}

$ruleFiles = $ndproj.NDepend.SelectSingleNode('RuleFiles')
if ($null -ne $ruleFiles) {
    $null = $ndproj.NDepend.RemoveChild($ruleFiles)
}

# Baseline / harness noise: keep inactive so rename/testkit churn does not gate quality.
# ND1412 is activated via squirix.ndrules CustomRuleOverrides (replication DAG gate).
$deactivateTokens = @(
    'ND1500', 'ND1501', 'ND1502', 'ND1503', 'ND1504', 'ND1505', # API Breaking Changes vs prior analysis
    'ND2201', # reserved exception types on compiler-generated collection helpers
    'ND1308', # namespace relational cohesion (Runtime.Contracts / TestKit bag namespaces)
    'ND1310', # testkit Hosting/Networking/Mtls sibling cycles through parent TestKit
    'ND1315'  # DisposableTypesMustUnsubscribeEvents - placeholder missing on NDepend 2026.1.2
)
foreach ($token in $deactivateTokens) {
    $placeholder = "`$${token}`$"
    $queries = $queriesNode.SelectNodes('.//Query') | Where-Object {
        $_.InnerText -like "*$placeholder*" -or $_.InnerText -like "*// <Id>${token}:*"
    }
    foreach ($query in $queries) {
        $query.SetAttribute('Active', 'False')
    }
}

$ndproj.Save($ndprojPath)
Write-Host "Updated $ndprojPath"
Write-Host "  IDEFile: .\squirix.slnx | Filters='' | Configuration=DEBUG|AnyCPU"
Write-Host "  Inlined $($customQueriesNode.SelectNodes('Query').Count) custom notmycode queries into '$justMyCodeGroupName'"
if ($null -ne $customRuleOverridesNode) {
    Write-Host "  Applied $($customRuleOverridesNode.SelectNodes('Query').Count) custom rule override(s) from squirix.ndrules"
}
Write-Host "  Deactivated noise rules: $($deactivateTokens -join ', ')"
Write-Host "  Removed legacy group '$legacyCustomGroupName' when present"
Write-Host "  Removed RuleFiles (Visual NDepend overwrites external rule file paths)"
