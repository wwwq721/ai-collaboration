param(
    [ValidateSet('Check','Install','Uninstall','Pack')][string]$Mode = 'Check',
    [string]$PackPython = ''
)
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [Text.UTF8Encoding]::new($false)
$root = [IO.Path]::GetFullPath($PSScriptRoot)
$folder = Get-Item -LiteralPath $root -Force
if ($folder.Attributes -band [IO.FileAttributes]::ReparsePoint) {
    $target = [string]@($folder.Target)[0]
    if (![IO.Path]::IsPathRooted($target)) { $target = Join-Path $folder.Parent.FullName $target }
    $root = [IO.Path]::GetFullPath($target)
}
$links = @(
    (Join-Path $env:USERPROFILE '.codex/skills/ai-collaboration'),
    (Join-Path $env:USERPROFILE '.workbuddy/skills/ai-collaboration')
)

function Test-OurLink([string]$path) {
    if (!(Test-Path -LiteralPath $path)) { return $false }
    $item = Get-Item -LiteralPath $path -Force
    if (!($item.Attributes -band [IO.FileAttributes]::ReparsePoint)) {
        throw "技能路径已有真实文件夹，停止以避免覆盖：$path"
    }
    $target = [IO.Path]::GetFullPath([string]@($item.Target)[0]).TrimEnd([char[]]@('\','/'))
    if ($target -ne $root.TrimEnd([char[]]@('\','/'))) {
        throw "技能路径指向其他目录，停止：$path"
    }
    return $true
}

if ($Mode -eq 'Pack') {
    # Pack must not open this device's local.json, peers.json or runtime.
    if (!$PackPython) { $PackPython = $env:COLLAB_PACK_PYTHON }
    if (!$PackPython) {
        $pythonCommand = Get-Command python.exe -ErrorAction SilentlyContinue
        if ($pythonCommand) { $PackPython = $pythonCommand.Source }
    }
    if (!$PackPython) { throw '请通过 -PackPython 或 COLLAB_PACK_PYTHON 指定已有 Python；不会自动安装依赖。' }
    $zipPath = Join-Path (Split-Path -Parent $root) 'ai-collaboration-portable.zip'
    & $PackPython -X utf8 -B (Join-Path $root 'scripts/manage_upgrade.py') portable --source $root --output $zipPath --replace
    if ($LASTEXITCODE -ne 0) { throw '公共迁移包构建失败；未读取本机身份或凭据。' }
    $zipPath
    exit 0
}

if ($Mode -eq 'Uninstall') {
    if (Test-Path -LiteralPath (Join-Path $root 'runtime/a2a-service.json')) {
        & powershell.exe -NoProfile -File (Join-Path $root 'a2a.ps1') -Mode Stop
        if ($LASTEXITCODE -ne 0) { throw 'A2A 服务未能停止，技能入口尚未移除' }
    }
    foreach ($link in $links) {
        if (Test-OurLink $link) {
            # Delete the verified directory junction only. Keep its target and configuration.
            [IO.Directory]::Delete($link,$false)
            "已移除技能入口：$link"
        }
    }
    exit 0
}

$local = Get-Content -LiteralPath (Join-Path $root 'local.json') -Raw -Encoding UTF8 | ConvertFrom-Json
& $local.python -X utf8 -B (Join-Path $root 'scripts/send.py') --check
if ($LASTEXITCODE -ne 0) { throw '本机配置检查未通过，未安装' }
& $local.node --check (Join-Path $root 'scripts/workbuddy_sync.cjs')
if ($LASTEXITCODE -ne 0) { throw 'Node 运行环境检查未通过，未安装' }
if ($Mode -eq 'Check') { exit 0 }

# Preflight every destination before creating either reference.
foreach ($link in $links) { $null = Test-OurLink $link }
foreach ($link in $links) {
    if (!(Test-OurLink $link)) {
        New-Item -ItemType Directory -Path (Split-Path -Parent $link) -Force | Out-Null
        New-Item -ItemType Junction -Path $link -Target $root | Out-Null
    }
    "技能入口：$link → $root"
}
