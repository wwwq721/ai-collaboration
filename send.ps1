param(
    [Parameter(Position=0)][string]$To,
    [Parameter(Position=1)][string]$Text,
    [switch]$List,
    [switch]$Status,
    [int]$WaitIdle = 0,
    [switch]$DebugReceipt
)
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [Text.UTF8Encoding]::new($false)
try {
    $local = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'local.json') -Raw -Encoding UTF8 | ConvertFrom-Json
    if (!(Test-Path -LiteralPath $local.python -PathType Leaf)) { throw '本机 Python 路径不可用，请填写 local.json' }
    $mode = if ($List) { 'list' } elseif ($Status) { 'status' } else { 'send' }
    $request = @{mode=$mode; to=$To; text=$Text; wait_idle=$WaitIdle; debug=[bool]$DebugReceipt}
    $OutputEncoding = [Text.UTF8Encoding]::new($false)
    $request | ConvertTo-Json -Compress | & $local.python -X utf8 -B (Join-Path $PSScriptRoot 'scripts/send.py') --stdin
    exit $LASTEXITCODE
} catch {
    @{state='not_sent'; error=$_.Exception.Message} | ConvertTo-Json -Compress
    exit 2
}
