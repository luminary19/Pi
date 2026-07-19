[CmdletBinding()]
param(
    [string]$TargetTag,
    [switch]$Install,
    [switch]$Push
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version 2.0

$RepoRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$LumicityRoot = [System.IO.Path]::GetFullPath((Join-Path $RepoRoot "..\..\.."))
$SubagentsRoot = Join-Path $LumicityRoot "Infrastructure\.pi\agent\extensions\pi-subagents"
$Rebased = $false
$BackupRef = $null

function Invoke-Native {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter(ValueFromRemainingArguments = $true)][string[]]$Arguments
    )
    & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$FilePath exited with code $LASTEXITCODE"
    }
}

function Invoke-NativeCapture {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter(ValueFromRemainingArguments = $true)][string[]]$Arguments
    )
    $output = & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$FilePath exited with code $LASTEXITCODE"
    }
    return (($output | Out-String).Trim())
}

function Get-LatestVersionTag {
    $tags = Invoke-NativeCapture git tag --list "v*"
    $parsed = @()
    foreach ($tag in ($tags -split "`r?`n")) {
        if ($tag -match '^v(\d+\.\d+\.\d+)$') {
            $parsed += [PSCustomObject]@{ Tag = $tag; Version = [version]$Matches[1] }
        }
    }
    if ($parsed.Count -eq 0) {
        throw "No semantic vX.Y.Z upstream tags were found."
    }
    return ($parsed | Sort-Object Version -Descending | Select-Object -First 1).Tag
}

function Restore-SourceBranch {
    if (-not $script:Rebased -or -not $script:BackupRef) { return }
    Write-Warning "Restoring the pre-update source branch from $script:BackupRef"
    & git rebase --abort 2>$null
    & git reset --hard $script:BackupRef
    if ($LASTEXITCODE -ne 0) {
        throw "Automatic source rollback failed. The backup ref is $script:BackupRef"
    }
}

Set-Location $RepoRoot

$dirty = Invoke-NativeCapture git status --porcelain
if ($dirty) {
    throw "Refusing to update a dirty Pi checkout. Commit or stash all changes first."
}

$branch = Invoke-NativeCapture git branch --show-current
if (-not $branch.StartsWith("lumicity/")) {
    throw "Refusing to update branch '$branch'. Check out a Lumicity patch-stack branch first."
}

Invoke-Native git fetch upstream --tags
if (-not $TargetTag) {
    $TargetTag = Get-LatestVersionTag
}
Invoke-Native git rev-parse --verify "$TargetTag^{commit}" | Out-Null

$currentBase = Invoke-NativeCapture git merge-base HEAD upstream/main
$targetCommit = Invoke-NativeCapture git rev-parse "$TargetTag^{commit}"
if ($currentBase -ne $targetCommit) {
    $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $BackupRef = "lumicity/pre-update-$stamp"
    Invoke-Native git branch $BackupRef HEAD
    try {
        Invoke-Native git rebase --onto $TargetTag $currentBase
        $Rebased = $true
    }
    catch {
        & git rebase --abort
        throw "Upstream rebase failed. The checkout was restored; backup ref: $BackupRef"
    }
}

try {
    Invoke-Native npm ci
    Invoke-Native npm run check
    Invoke-Native npm --workspace "@earendil-works/pi-agent-core" test -- --run test/cancellation.test.ts test/agent.test.ts test/agent-loop.test.ts
    Invoke-Native npm --workspace "@earendil-works/pi-coding-agent" test -- test/suite/regressions/6363-agent-settled-event.test.ts test/interactive-mode-status.test.ts test/bash-close-hang-windows.test.ts
    Invoke-Native npm --workspace "@earendil-works/pi-coding-agent" test -- test/package-command-paths.test.ts -t "refuses .* self-update"
    Invoke-Native npm run build

    if (-not (Test-Path $SubagentsRoot)) {
        throw "Required Lumicity subagent extension not found: $SubagentsRoot"
    }
    Invoke-Native npm --prefix $SubagentsRoot ci
    Invoke-Native npm --prefix $SubagentsRoot run lint
    Invoke-Native npm --prefix $SubagentsRoot run typecheck
    Invoke-Native npm --prefix $SubagentsRoot test
    Invoke-Native npm --prefix $SubagentsRoot run build

    Invoke-Native node (Join-Path $RepoRoot "scripts\build-lumicity-artifacts.mjs")

    $commit = Invoke-NativeCapture git rev-parse HEAD
    $artifactDir = Join-Path $RepoRoot (".artifacts\lumicity\" + $commit.Substring(0, 12))
    $provenancePath = Join-Path $artifactDir "provenance.json"
    if (-not (Test-Path $provenancePath)) {
        throw "Artifact builder did not produce provenance: $provenancePath"
    }
    $provenance = Get-Content -Raw $provenancePath | ConvertFrom-Json

    if ($Push) {
        Invoke-Native git push --force-with-lease origin ("HEAD:" + $branch)
    }

    if (-not $Install) {
        Write-Host "Candidate verified. Live Pi was not changed." -ForegroundColor Green
        Write-Host "Provenance: $provenancePath"
        Write-Host "Re-run with -Install only after review approval."
        exit 0
    }

    $installedRecordPath = Join-Path $RepoRoot ".artifacts\lumicity\installed.json"
    $previousRecord = $null
    if (Test-Path $installedRecordPath) {
        $previousRecord = Get-Content -Raw $installedRecordPath | ConvertFrom-Json
    }
    $globalRoot = Invoke-NativeCapture npm root --global
    $installedManifest = Join-Path $globalRoot "@earendil-works\pi-coding-agent\package.json"
    $previousVersion = $null
    if (Test-Path $installedManifest) {
        $previousVersion = (Get-Content -Raw $installedManifest | ConvertFrom-Json).version
    }
    $artifactPaths = @($provenance.artifacts | ForEach-Object { $_.path })

    try {
        Invoke-Native npm install --global @artifactPaths
        $newGlobalRoot = Invoke-NativeCapture npm root --global
        Invoke-Native node (Join-Path $RepoRoot "scripts\probe-installed-abort.mjs") --root $newGlobalRoot
        $cliPath = Join-Path $newGlobalRoot "@earendil-works\pi-coding-agent\dist\cli.js"
        Invoke-Native node $cliPath --version
    }
    catch {
        Write-Warning "Candidate installation failed verification; attempting live rollback."
        if ($previousRecord -and $previousRecord.artifacts) {
            $rollbackArtifacts = @($previousRecord.artifacts | ForEach-Object { $_.path } | Where-Object { Test-Path $_ })
            if ($rollbackArtifacts.Count -ne $previousRecord.artifacts.Count) {
                throw "Rollback artifacts are incomplete. Previous record: $installedRecordPath"
            }
            Invoke-Native npm install --global @rollbackArtifacts
        }
        elseif ($previousVersion) {
            Invoke-Native npm install --global ("@earendil-works/pi-coding-agent@" + $previousVersion)
        }
        else {
            throw "No previous Pi installation metadata is available for rollback."
        }
        throw
    }

    $installedRecord = [ordered]@{
        installedAt = (Get-Date).ToUniversalTime().ToString("o")
        provenancePath = $provenancePath
        forkCommit = $provenance.forkCommit
        upstreamTag = $provenance.upstreamTag
        artifacts = $provenance.artifacts
    }
    $installedRecord | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 $installedRecordPath
    Write-Host "Lumicity Pi installed and verified." -ForegroundColor Green
    Write-Host "Installed record: $installedRecordPath"
}
catch {
    Restore-SourceBranch
    throw
}
