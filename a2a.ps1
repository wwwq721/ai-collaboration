param([ValidateSet('Install','Start','Stop','Status')][string]$Mode = 'Status')
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [Text.UTF8Encoding]::new($false)
$root = [IO.Path]::GetFullPath($PSScriptRoot)
$folder = Get-Item -LiteralPath $root -Force
if ($folder.Attributes -band [IO.FileAttributes]::ReparsePoint) {
    $root = [IO.Path]::GetFullPath([string]@($folder.Target)[0])
}
$hashAlgorithm = [Security.Cryptography.SHA256]::Create()
try { $lockKey = [BitConverter]::ToString($hashAlgorithm.ComputeHash([Text.Encoding]::UTF8.GetBytes($root.ToLowerInvariant()))).Replace('-','') }
finally { $hashAlgorithm.Dispose() }
$managementMutex = [Threading.Mutex]::new($false,('Local\AiCollabA2A-'+$lockKey))
$hasLock = $false
try {
try { $hasLock = $managementMutex.WaitOne(15000) }
catch [Threading.AbandonedMutexException] { $hasLock = $true }
if (!$hasLock) { throw '另一个 A2A 管理操作尚未结束，请稍后检查状态' }
$runtime = Join-Path $root 'runtime'
$statePath = Join-Path $runtime 'a2a-service.json'
$localPath = Join-Path $root 'local.json'
$local = Get-Content -LiteralPath $localPath -Raw -Encoding UTF8 | ConvertFrom-Json
$python = Join-Path $root '.a2a-env/Scripts/python.exe'
$server = Join-Path $root 'scripts/a2a_server.py'

function Write-Result($value) { $value | ConvertTo-Json -Compress -Depth 8 }
function Read-State {
    if (Test-Path -LiteralPath $statePath) {
        return Get-Content -LiteralPath $statePath -Raw -Encoding UTF8 | ConvertFrom-Json
    }
    return $null
}
function Read-Health($port) {
    try { return Invoke-RestMethod -Uri "http://127.0.0.1:$port/health" -Headers @{Authorization=('Bearer '+$local.a2a.tokens.local)} -TimeoutSec 2 -Proxy $null }
    catch { return $null }
}
function Find-OurProcess($state) {
    if (!$state) { return $null }
    $servicePid = [int]$state.pid
    $process = Get-Process -Id $servicePid -ErrorAction SilentlyContinue
    if (!$process) { return $null }
    $sameTime = $process.StartTime.ToUniversalTime().Ticks.ToString() -eq [string]$state.started
    if (!$sameTime -or $process.Path -ne $python) {
        # The old PID may have been reused after a crash; never stop its new owner.
        return $null
    }
    return $process
}

if ($Mode -eq 'Install') {
    if (!(Test-Path -LiteralPath $python -PathType Leaf)) {
        & $local.python -m venv (Join-Path $root '.a2a-env')
        if ($LASTEXITCODE -ne 0) { throw '无法建立 A2A 独立环境' }
    }
    & $python -m pip install --disable-pip-version-check -r (Join-Path $root 'requirements-a2a.txt')
    if ($LASTEXITCODE -ne 0) { throw 'A2A 依赖安装失败' }
    if (!$local.a2a) {
        $bytes = New-Object byte[] 32
        $rng = [Security.Cryptography.RandomNumberGenerator]::Create()
        try { $rng.GetBytes($bytes) } finally { $rng.Dispose() }
        $settings = [pscustomobject]@{
            host='127.0.0.1'; port=8788; idle_wait_seconds=300; task_timeout=900
            tokens=@{local=[Convert]::ToBase64String($bytes)}
        }
        $local | Add-Member -MemberType NoteProperty -Name a2a -Value $settings
        [IO.File]::WriteAllText($localPath,($local | ConvertTo-Json -Depth 12),[Text.UTF8Encoding]::new($false))
    }
    Write-Result @{state='ready'; action='installed'; environment='.a2a-env'}
    exit 0
}

if (!$local.a2a -or $local.a2a.host -ne '127.0.0.1') { throw '先运行 a2a.ps1 -Mode Install；当前仅支持本机监听' }
$port = [int]$local.a2a.port
if ($port -lt 1 -or $port -gt 65535) { throw 'A2A 监听端口无效' }
$state = Read-State
$process = Find-OurProcess $state

if ($Mode -eq 'Stop') {
    if ($process) {
        try {
            $stopHeaders = @{Authorization=('Bearer '+$local.a2a.tokens.local)}
            $stopBody = @{instance=$state.instance} | ConvertTo-Json -Compress
            $null = Invoke-RestMethod -Uri ("http://127.0.0.1:"+[int]$state.port+"/shutdown") -Method Post `
                -Headers $stopHeaders -Body $stopBody -ContentType 'application/json' -TimeoutSec 3 -Proxy $null
            $process.WaitForExit(10000) | Out-Null
        } catch { }
        if (!$process.HasExited) {
            Stop-Process -Id $process.Id
            $process.WaitForExit(5000) | Out-Null
        }
        if (!$process.HasExited) { throw 'A2A 进程尚未停止，保留状态记录' }
    }
    if (Test-Path -LiteralPath $statePath) { Remove-Item -LiteralPath $statePath }
    Write-Result @{state='stopped'}
    exit 0
}

$health = Read-Health $port
if ($Mode -eq 'Status') {
    if ($process -and $health -and $health.service -eq 'ai-collaboration-a2a' -and $health.instance -eq $state.instance) {
        Write-Result @{state='running'; url="http://127.0.0.1:$port"; agents=$health.agents}
    } else {
        Write-Result @{state= $(if ($process) {'unhealthy'} else {'stopped'})}
    }
    exit 0
}

if ($process) {
    if (!$health -or $health.instance -ne $state.instance) { throw '既有 A2A 进程未就绪，未重复启动' }
    Write-Result @{state='running'; url="http://127.0.0.1:$port"; agents=$health.agents}
    exit 0
}
if (!(Test-Path -LiteralPath $python -PathType Leaf)) { throw 'A2A 依赖未准备，请运行 a2a.ps1 -Mode Install' }
$probe = New-Object Net.Sockets.TcpListener([Net.IPAddress]::Parse('127.0.0.1'),$port)
try { $probe.Start() } catch { throw 'A2A 端口已占用，未更动占用端口的程序' }
finally { $probe.Stop() }
New-Item -ItemType Directory -Path $runtime -Force | Out-Null
$instance = [Guid]::NewGuid().ToString('N')
# Windows PowerShell Start-Process can leak the caller's capture handles to a
# detached child. Python's explicit handle allowlist keeps tool calls finite.
$launched = & $local.python -X utf8 -B (Join-Path $root 'scripts/a2a_launch.py') --root $root --instance $instance
if ($LASTEXITCODE -ne 0) { throw '无法创建 A2A 后台进程' }
$launchResult = $launched | ConvertFrom-Json
$process = Get-Process -Id ([int]$launchResult.pid)
$record = @{pid=$process.Id; instance=$instance; port=$port; started=$process.StartTime.ToUniversalTime().Ticks.ToString()}
[IO.File]::WriteAllText($statePath,($record | ConvertTo-Json),[Text.UTF8Encoding]::new($false))
for ($attempt=0; $attempt -lt 20; $attempt++) {
    Start-Sleep -Milliseconds 500
    $health = Read-Health $port
    if ($health -and $health.instance -eq $instance) {
        Write-Result @{state='running'; url="http://127.0.0.1:$port"; agents=$health.agents}
        exit 0
    }
    if ($process.HasExited) { throw 'A2A 启动失败，见 runtime/a2a.log 或 a2a-stderr.log' }
}
throw 'A2A 启动尚未确认，请检查状态；未重复启动'
} finally {
    if ($hasLock) { $managementMutex.ReleaseMutex() }
    $managementMutex.Dispose()
}
