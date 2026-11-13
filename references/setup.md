# 配置、维护与修改指南

程序版本为 2026.11.13.3-simple。同一协作包，日常只填收件人和正文。电脑 Codex、WorkBuddy 默认经各自的 Skill 调用 CLI；Minis 沿用原 Skill、CLI 和网络收件器。已实现三软件原聊天适配、官方 A2A SDK 1.3.0 收发、程序自动收件回执及私网 HTTPS。备用 MCP 以未安装方案保存在 references/mcp-fallback.zip，活动代码和日常接入不依赖 MCP SDK。宿主配置撤除与旧进程实际释放，以项目 tasks.md 的部署验收为准。ACP 连接器尚未实现。唯一使用说明为 SKILL.md，唯一程序修改说明为本文，新增细节继续整理到这两处。

使用见 [SKILL.md](../SKILL.md)。修改程序直接看本文后半部“程序修改指南”；这是随包迁移的维护入口，不依赖旧聊天或另一台电脑的绝对路径。项目方案记录目标、部署和已验事实，本文记录具体修改步骤。

## 根本目的与确认规则

不同软件、不同设备的 AI 能自行向指定原聊天发消息、唤醒对方并继续交流，用户无需转话。每个 AI 接入同一协作包，并且减少ai对话的成本，比如日常只填“目标对话＋正文”。

收件回执由接收方程序依据宿主的实际状态自动生成，发件程序核对并更新发送结果后，本次确认结束。确认链路在程序内完成，双方 AI 的发言由业务消息本身决定，可以继续提问、回答或联系第三个 AI。

接收 AI 的业务来信统一为正文、来源软件和可用于后续发送的来源对话地址。收件状态分别表示宿主已接纳、宿主已排队或送达未确认。Minis 创建 follow-up 当前只证明调度已排队，进一步的原聊天接收证据须实测核验。

本版实现按对话交流与程序自动收件回执，消息模式依已核验的中央能力启用；任意不同的已接入原聊天自动互通，不再逐软件、逐方向配置权限。旧任务、临时结果地址和后台结果通知仅为升级前记录及标准任务服务保留兼容。普通消息的收件确认只更新工具结果，AI 自主决定后续业务发言。

消息模式的发送结果为 waiting、accepted、queued、uncertain 或 not_sent，分别表示等待目标确认、宿主已接纳、宿主/调度器已排队、送达未确认及明确未送出。失败文字直接关联目标和原消息短摘。消息编号在发送前由程序登记，身份隔离的收据查询使用该编号，独立于 SDK 短期任务缓存。SDK task_id 与发送 message_id 各有用途。

消息元数据继续沿用原有有界数据库。每个源的 message_id 幂等绑定目标及正文摘要，正文只在投递或短期结果阶段保留。设备确认和总收件记录同事务更新，晚到合法回执只确认原记录。外部标准服务的实际正文按原请求隔离短存十分钟，进入另一个 AI 原聊天前核对实际来源和目标登记，使用确定性消息键去重。各结果在现有在途上限内独立处理，不被另一台慢设备串行阻塞。本机同一原聊天的原生提交依序进入，跨聊天及手机设备队列仍可并行；等候占用原空闲等待上限，锁内复核接入身份。原生转交最多尝试一次，重启或结果不明时不重发；原请求仍可在短存期限内查询正文。纯标准任务型调用者从原 SDK 任务或带 include_result=1 的本人收据查询读取结果，不从 A2A URL 推定支持原聊天主动收件。

## 使用与进程

Windows 在 PowerShell 中运行 send.ps1；宿主默认终端不是 PowerShell 时，显式用 PowerShell 调用该文件。Minis/Linux 运行 sh send.sh。已知目标直接提供“目标对话＋正文”，无需在每次发送前列伙伴或查状态。来源由本次宿主命令环境和本机登记核对，无法识别则拒绝猜测。人工检查可用 scripts/send.py --as 已登记本端名；这不是日常填写项，不能用来冒用别的原聊天。

Minis 的命令执行环境由宿主为当前聊天建立，发给脚本的 MINIS_CHAT_SESSION_ID 用于识别来源；本包 minis.py 再用原生 minis-sessions-cli status 核对当前会话及父会话链，找到已登记的来源原聊天。send.sh 优先使用手机现有 .a2a-env/bin/python，缺失时才用可用的 python3。手机 MCP 客户端当前调用链没有向本包逐次转发这份来源上下文；这是该客户端当前实现的接入边界，不能据此说 MCP 协议不支持来源元数据，也不能把服务器启动时的某个聊天号当成所有调用的来源。手机当前沿用已验证的 CLI 与收件器，本次电脑入口调整不要求手机安装 MCP SDK 或额外更新。

旧任务兼容中的 accepted/background=true 表示已取得任务号，后台查询同一任务，把完成、失败、过期或需输入状态送回发件原聊天，AI 可以结束本轮。立即返回的最终结果不再后台播报。uncertain 先核对，勿重复派题。

电脑用 a2a.ps1 -Mode Start / Status / Stop；手机配对后用 sh scripts/receiver.sh start / status / stop。后台查询和空轮询不调用模型。A2A 进程使用包内 .a2a-env；Windows send.ps1 及原生投递辅助进程使用 local.python，后者需要现有的 Windows 原生依赖。不要把这两个 Python 环境当成一个。

Codex 须有桌面持有目标原聊天；WorkBuddy 须运行、账号匹配且远程通道可用，消息经过其云服务；原生 idle/error/failed/terminated 均可接新输入，忙碌与未知状态保留实际等待语义；Minis 用指定 session 的 follow-up。接纳不等于模型完成。应用/子进程回收后的无人值守恢复尚未保证，锁屏、换网、重启分别验收。

## 新软件和设备

1. 确认设备、软件、原聊天，先找已有适配器或标准连接器。
2. 在本机 local.json 登记 device_id、运行路径，在 peers.json 登记伙伴及本端原聊天。现有软件在新设备上复用原适配器。
3. A2A 伙伴合并到 peers.json.peers；凭据只放 local.json.a2a_credentials.研究伙伴.token：

~~~json
{
  "研究伙伴": {
    "software": "newapp", "transport": "a2a", "device_id": "pc-2",
    "conversation_id": "核对后的原聊天号",
    "url": "https://对方私网域名:端口/agents/研究伙伴"
  }
}
~~~

兼容 software=a2a 旧格式。仅处理独立任务的服务不能标成“已验证原聊天唤醒”。新宿主自身要发信，还需实现可信来源识别；只登记对方 URL 不等于双向接入。

有原生 API/CLI 的软件按下文补充适配器支持、来源识别和适配。网络来信不能加载任意模块/命令。MCP 工具能调用不等于能唤醒原聊天；ACP 尚未接入，不能直接运行规划模板。

4. 验收程序收件回执、指定原聊天唤醒、连续消息、可选双向发言、忙/拒绝/未知和来源身份。接收 AI 保持沉默时，本次发送也应根据原生回执结束。新软件逐项核对真实原聊天能力。

## 备用 MCP 的恢复与撤除

本阶段日常采用 CLI，电脑两宿主的 MCP 注册和独立 .mcp-env 按项目记录撤除。Codex、Minis 保留原 Skill，WorkBuddy 恢复指向同一正式包的 Skill 联接。已有 MCP 子进程是否随宿主重读或退出而释放，必须查看本次实际验收，不能仅凭配置已删、文件已删或宿主界面关闭作结论。

本次 WorkBuddy 原聊天分别通过 MCP 和 PowerShell/send.ps1 各发送一次，均以自动来源进入同一目标原聊天、新开处理轮次，取得 accepted/native_host。两次都没有先列伙伴或查状态。此前连续 CLI 测试曾因目标忙而等满300秒，结果为 not_sent，且投递许可未签发；这属于共同的目标收件等待限制。上述少量实测不构成 CLI/MCP 的普遍优劣或 token 数量结论。

需要恢复 MCP 时，仅使用 references/mcp-fallback.zip 这一份备用。电脑迁移包携带该 ZIP，手机公共更新包排除它。ZIP 不含说明副本、环境、宿主配置、凭据、运行状态或数据库，内容为：

- scripts/mcp_actions.py、scripts/mcp_server.py、scripts/setup_mcp.py
- tests/test_mcp_actions.py、tests/test_mcp_stdio.py、tests/test_setup_mcp.py
- requirements-mcp.txt、requirements-mcp-windows-lock.txt
- mcp-fallback-manifest.json，格式为 format=1、kind=mcp-fallback、files={相对路径:SHA256}，只列上面8个公共文件

恢复步骤集中在此，日常发送无需执行。

1. 核对项目当前授权与对应发布验证记录中的整包 SHA256。在隔离目录检查 ZIP，拒绝绝对路径、..、链接、重复项或超出上述集合的文件。按 mcp-fallback-manifest.json 核对恰好8个公共文件及各自 SHA256 后再解出；不得直接覆盖正式包。ZIP 内部清单不能替代可信发布记录中的整包校验。
2. 在隔离候选恢复这8个原路径。用现有 SDK Python 跑不依赖 MCP SDK 的 actions/context/setup 检查。需要实际恢复 MCP 时，在包内独立 .mcp-env 安装官方 mcp==1.30.0，使用 requirements-mcp-windows-lock.txt 的完整版本与下载哈希；原锁针对 Windows CPython 3.13，其他系统或 Python 版本先重新预览依赖。保留现有 .a2a-env、local.python 和手机环境各自用途。依赖安装须已在本次授权范围内。
3. 用独立 .mcp-env 运行 test_mcp_stdio.py 真正启动协议进程。缺 SDK 导致跳过不算成功。核对 send_message(to,text)、list_peers()、send_status(to) 及逐次来源，再将已验证的公共文件发布到正式包。
4. 按需执行 scripts/setup_mcp.py preview --root 包目录 --host workbuddy或codex。预览仅显示本项目条目、路径和比较指纹。核对后执行 install --root 包目录 --host 对应宿主 --confirm --expected 本次预览指纹，不覆盖同名不同条目；仅在安装脚本给出的本机私密目录保留单条回退依据。
5. 让真正持有目标原聊天的同一个宿主重读本条配置。外部改文件或刷新另一个后端不证明当前原聊天已经连上。按宿主当前支持的刷新方式处理；没有已核实入口时，先核对其他聊天与进程状态，再安排正常退出与重开。不能把关窗口留托盘算作退出，也不能承诺旧进程已释放。
6. 在已登记原聊天真实调用一次，并核对来源、目标原聊天、程序收件回执和业务正文。验收通过后才将该宿主切换为 MCP 日常入口。手机及其他宿主保持已验证的入口。
7. 若 POST、宿主重读或原聊天验收失败，先查原发送状态，结果不明不重投。使用 setup_mcp.py rollback --root 包目录 --host 对应宿主 --confirm --installation 本次安装编号，只撤回仍匹配的本项目入口，恢复该宿主原 Skill 联接。保留后来其他设置、凭据、身份和数据库。禁止整体恢复旧宿主配置，也不运行影响共享服务的 setup.ps1 Uninstall。

备用 MCP 的调用仍为 send_message → mcp_actions → send.run → 现有 A2A/身份核验/回执 → 目标软件原生收件。工具参数不提供来源、令牌、任意命令或旧任务临时地址。WorkBuddy 5.9.3 使用每次 tools/call 的 _meta["workbuddy.ai/conversationId"]；Codex 使用每次调用的 _meta.threadId。两者都要匹配本机唯一、已核验的原聊天，不取启动环境、不猜父聊天、不接受 AI 手填来源替代。元数据只在受配置宿主控制的本地 stdio 管道上作身份声明，不能直接信任任意 HTTP 客户端自报字段。同一连接的并发调用保持独立上下文，取消不保证撤回已投递消息，也不会自动重发。

宿主配置只改本条：WorkBuddy 使用 mcp.json 的 mcpServers.ai-collaboration，Codex 使用当前 CODEX_HOME/config.toml 的 mcp_servers.ai-collaboration。安装脚本保留其他字节与解析值，检查原文指纹、同名冲突和本机文件权限。Codex 的独立管理块在回退时须逐字匹配；本人改过管理块则停止自动回退。包内锁不能协调其他编辑器，不能把检查与写入之间的窗口称为全局原子操作。新增 MCP 宿主先核验逐次可信来源，能调用工具仍须另验指定原聊天收件。

## 手机私网

电脑后端保持 127.0.0.1:8788，由 Tailscale Serve 转接专用 HTTPS 端口。不发布 Funnel，不为发信修改全局代理、DNS、Clash 或其他项目端口。申请公信证书会公开登记完整设备域名，访问仍受私网及应用认证限制。

电脑 local.a2a.public_base_url 登记 HTTPS 根地址；device_tokens 为每个设备分配独立令牌。完成接入的原聊天之间自动互通，旧 device_permissions、message_permissions 和网页策略不再限制软件或方向。手机发件 metadata 的设备/软件/原聊天须与登记相符。设备领取、任务查询和回件分别检查归属；回件还须对应实际投递及当前服务实例。

手机保留自身身份及 local.python，在 network 增量合并下列字段；这里只是格式示例，不能把占位值写进正在使用的配置：

~~~json
{
  "mode": "paired_ip",
  "base_url": "https://配对电脑完整域名.ts.net:18443",
  "server_dns_name": "配对电脑完整域名.ts.net",
  "device_dns_name": "配对电脑完整域名.ts.net",
  "server_port": 18443,
  "token": "本机私密设备令牌"
}
~~~

程序读取 minis-config get network.tailscale.status，兼容 value 为字符串化 JSON，按完整 dnsName 唯一匹配在线节点并筛选唯一 Tailscale IPv4。SDK URL、Host、SNI 和证书保持完整域名；禁止关闭验签或猜第一台设备。connect_ip 仅用于明确选择的静态配对。本版 paired_ip 绑定一个上游来源；多个独立上游尚需扩展映射，不能直接放宽来源校验。

凭据仅供程序在本机内存使用。AI 不得读取或展示真实密钥值、完整私密配置或批量环境变量，不把值写进提示词、回件、日志或迁移包。POSIX 私密配置权限为 0600；Windows 保持本机私密文件访问权限。发现历史暴露仅报类别/位置，由用户手动换，不重读、不自行轮换。

## 保存、迁移与验证边界

迁移新设备使用 setup.ps1 -Mode Pack。ZIP 只含公开程序、说明、电脑测试和虚构的配置模板。新设备按自己的原聊天填写 peers.json、local.json，重新核验身份和配对。原设备的聊天登记、令牌、近期消息和运行状态留在原设备。已有手机的原位升级使用下文的设备更新包。

runtime 不另存输入消息历史：

| 文件 | 默认规则 |
|---|---|
| a2a.log、.1、.2 | 每份 1MiB，最多三份，仅元数据，无正文/凭据 |
| a2a-tasks.db | 结果缓存十分钟，结束项最多 200 |
| a2a-mailbox.db | 地址/状态；SDK 保存终态后清掉重复明文，去重 24 小时最多 5000 项 |
| pending-results.db | 来源/任务号/通知状态；通知接纳后清结果，临时结果十分钟，有界元数据 |
| device-receipts.db | 投递/原生调度编号及确认，无正文；严格核对归属后清本包自建调度 |
| 服务状态、锁与备份 | 实例、进程、最近轮询时间；备份可能含私密配置，不公开、不迁移 |

正文/回件各 64KiB，在途默认 16 项；无法安全清理时拒新任务。SQLite 复用已清空页面，文件保留峰值不等于保留明文。原软件的聊天历史由原软件管理。

2026-11-08 两端原聊天双向自动唤醒、回件已真实通过；具体依据保留在电脑 validation/deployment-1108.json。自动测试、本机 TLS 成功不代表锁屏、换网、多日运行或 App 强停恢复成功。原聊天忙超过结果缓存期限时可能收到 expired，应只读核对已完成成果，不重做原任务。

## 程序修改指南

使用说明集中在包内 [SKILL.md](../SKILL.md)，程序修改说明集中在本文。本包迁移时一并携带和维护这两处说明。每次修改先对照上文根本目的，核对自主交流、指定原聊天唤醒及 AI 对话成本。

用户级维护要求如下。

> 写大型程序（超过大致10个代码文件）时，应该在程序文件内维护唯一一个使用说明和唯一一个程序修改说明，以便后续想要修改某些功能，或者使用时，造成偏差（程序修改和使用说明里都要加上这句话）

以下步骤供后续 AI 或开发者接手。文档里的命令是操作说明，不表示本轮已执行。仅改文档不授权更新运行代码、安装依赖、改权限或网络；先依据用户当前授权确定范围。

### 1. 接手与准备：先隔离，确认改动边界

1. 读 SKILL.md、本文及项目 tasks.md 对应段落；在迁移设备上以本包说明为入口，不依赖旧机路径。
2. 明确本次触发条件、预期结果、受影响模块、是否影响两端/任务状态/配置，以及验收方法。仅查公开源码、状态和字段存在性；不输出私密配置。
3. 在工作区建隔离候选，只复制公共代码、说明和 tests；不要复制正式 local.json、peers.json、runtime、.a2a-env、validation 或现有密钥。测试用临时目录和虚构身份/凭据。
4. 可以用既有 SDK Python 跑候选测试，不在正式环境随意升级依赖。Windows 原生辅助 Python 的路径以本机既有配置为准，不把旧机路径硬编码进公共代码。
5. 只改选中的模块。另存修改前的公共文件和哈希清单；新发布前还要留一份上一可用版本。凭据备份与公共发布包严格分开。

当前维护机的正式根目录为 C:/path/to/ai-collaboration；手机为 /var/minis/skills/ai-collaboration。下面命令中的 candidateRoot 指你刚建立的隔离候选，不是正式根目录。

### 2. 功能在哪里，修改会影响什么

| 文件/入口 | 主要职责与修改位置 | 修改时要连同检查 |
|---|---|---|
| send.ps1、send.sh；scripts/send.py | 两项调用、严格来源识别、中央消息路由、原生辅助和旧临时结果兼容 | Windows 入口用 local.python，手机优先 SDK 环境；完整来源和错误分类不能丢 |
| references/mcp-fallback.zip | 唯一备用 MCP 源码、测试与锁定依赖；日常活动路径不加载 | 恢复前按本节前面的备用流程核对整包及逐文件哈希，恢复后再跑隔离协议测试 |
| host_adapters.py | ChatRef、NATIVE_SOFTWARE、get_adapter、NativeAdapter 的 status/deliver/invoke、deliver_local | 本机原聊天唯一登记、未知软件拒绝；异步 Windows 投递经原生 Python 辅助进程 |
| codex_ipc.py | 桌面持有者发现、状态、空闲 start/忙时 steer、外部消息包装及轮次回执 | 不另起写者抢原聊天；已发 RPC 结果不明不可自动重发 |
| workbuddy.py、workbuddy_sync.cjs | 复用已登录 broker 和 sendPrompt，以远程 requests 的唯一原生标记及完整正文确认接纳；处理宿主特殊 @skill 语法 | Python/Node 两层均需检查，不复制登录凭据 |
| minis.py | 当前/父会话识别、指定原聊天 follow-up、自建调度归属与清理 | 调度接纳不等于模型完成；只删标签/会话/记录均匹配的本包任务 |
| a2a_transport.py、a2a_client.py | SDK 子进程、exchange、任务/上下文连续性、即时回执、同源卡片检查 | task-status 查询指定 task，不查错最新任务，不重新派题；标准 transport=a2a 保留宿主软件身份 |
| a2a_server.py | BearerAccess 设备认证、Agent Card、原生/远端队列、lifespan、恢复；health/package/device 路由 | 未认证拒绝、可信来源、owner 隔离、当前实例、固定文件下载；无网页或登录路由 |
| a2a_state.py；a2a_runtime.py | Mailbox 地址/回件/连续性；Limits、SDK 临时结果、日志轮换、清理 | SQLite 兼容、TTL/数量上限、原文擦除、并发、停止释放 |
| device_queue.py | 设备投递、领取/确认、sender_for_reply、回件授权和过期清理 | 按设备隔离；回件由实际投递派生身份，不能只相信请求体 |
| device_receiver.py | 网络长轮询、原生投递、ACK、去重、远端回信、生命周期与本包调度清理 | 新题不重复触发；收件与后台查询并行；确认失败可重试 ACK，不能重派题 |
| result_watcher.py | 新消息发送前登记及静默收件查询，旧任务结果通知兼容 | prepared→waiting→ready→dispatching→notified；inline/uncertain 另行终止，不重播不明通知 |
| network_transport.py | 同步/异步 HTTPX 传输、节点解析、域名→配对 IP 拨号 | Host/SNI/证书/逻辑 URL 同源、正文上限、关闭内层连接池；不改系统网络 |
| a2a.ps1、a2a_launch.py | 电脑服务 start/status/stop、进程实例核对与隐藏启动 | 健康检查在程序内带 local 认证；不按复用 PID 误停其他程序 |
| receiver.sh、receiver_manager.py | 手机单实例 start/status/协作式 stop | stop 返回 stopping 时尚未停止；不能仅看 PID 或命令退出码 |
| setup.ps1；setup.sh、setup_mobile.py | 桌面 Skill 引用/检查/迁移打包；手机初始化/依赖检查 | Install/init 会改变环境或身份，普通升级不能重跑初装或覆盖配置 |
| access_policy.py、managed_bridge.py、delivery_ticket.py、collaboration_client.py | 核对已接入原聊天、来源身份及实际投递前一次性许可 | 所有已接入聊天互通；仍核对来源、设备归属、实际投递、一次性许可与停用接入 |
| managed_egress.py、a2a_client.py | 从中央服务向已核验的标准 A2A 伙伴发原始正文，使用该伙伴自己的凭据，保存远端任务号以查询回件和延续追问 | 真实 SDK 交互、原聊天与 HTTPS 地址绑定、提交前撤权、不同来源及不同任务隔离、不明结果不重派 |
| managed_setup.py、manage_upgrade.py | 安全盘点、核验登记及发件身份绑定；公共包构建、校验、备份、两端升级和回退 | 接入核对原聊天及设备配对，不配置方向；备份只还原对应版本和文件 |

新增原生软件并非只改一个名称：同时检查 send.py 的 SUPPORTED、sender_at（自动/人工身份）、check，host_adapters.py 的允许名单、来源变量/能力/调用分支，以及 a2a_server.py 暴露入口和 device_receiver.py 本地收件允许名单。可用 rg 查 codex、workbuddy、minis 现有分支，再决定最小改动；不能改成从网络指定 Python 模块。

已有 A2A 服务通常只需登记，不改公共路由。电脑与手机默认沿 CLI 发件，ACP 尚未实现。备用 MCP 来源适配已覆盖 WorkBuddy、Codex；将来恢复或增加其他 MCP 宿主时，先核对逐次真实来源及原聊天收件，不能把启动进程时的聊天号套给全部调用。

### 3. 必须保留的约定

- ChatRef 用设备＋软件＋原聊天，任务号和协议上下文不冒充聊天号。收件人不能靠正文伪造发件身份。
- accepted 只是接纳；最终结果与通知状态分开。等待不调用模型，不循环互发空确认。
- prepared 在发送前登记；通知前先落 dispatching。uncertain 或中断的 dispatching 不自动重投；有证据确定 not_sent 才能按现有规则重试同一通知。
- 未认证设备拒绝；已认证设备仍只能领自己的投递，回件绑定实际投递/当前实例；owner 是发起方，不是收件方。
- 未知/不明状态如实返回，不能改成 completed。缓存过期核对成果，不重复有副作用任务。
- TLS 正常验签，不通过去掉同源检查、换成明文、自动改代理/路由来掩盖连接问题。
- 改数据库先做旧表升级、恢复/去重和回退兼容检查。未来若引入破坏性迁移，必须在发布前明确备份、停机和还原策略，不能删除 runtime 试修。
- 日志只记事件、编号和错误类型；真实凭据、完整请求/回件不进入模型或日志。保留清理期限和容量限制。

### 4. 怎样测试

测试主要使用临时数据库、虚构令牌、模拟最后一步原生投递，以及本机 HTTP/ASGI（不打开浏览器的服务接口）/TLS。部分测试会启动临时本机服务或子进程，不会因此证明真实软件已接通。

在 PowerShell 中先设置候选路径和已有 SDK 解释器：

~~~powershell
$installedRoot = 'C:\path\to\ai-collaboration'
$candidateRoot = 'C:\待替换\隔离候选目录'
$sdkPython = Join-Path $installedRoot '.a2a-env\Scripts\python.exe'
Set-Location -LiteralPath $candidateRoot
if (!(Test-Path -LiteralPath $sdkPython -PathType Leaf)) { throw '先核对已有SDK环境；不要自动安装' }
~~~

先跑与修改对应的测试；下面单文件命令中的 -p 可按表替换。退出码非零立即停，不继续发布：

~~~powershell
& $sdkPython -X utf8 -B -m unittest discover -s tests -p 'test_result_watcher.py' -v
if ($LASTEXITCODE -ne 0) { throw '相关检查失败，未发布' }
~~~

| 修改范围 | 至少选择的现有测试文件 |
|---|---|
| 发信/来源/路由 | test_send.py、test_mobile_routing.py、test_console.py |
| 原生适配/软件升级 | test_host_adapters.py，加 test_codex_ipc.py 或 test_minis.py；WorkBuddy 另核对 test_send.py 的 Node broker 回执用例 |
| A2A 协议、任务状态、数据库/清理/日志 | test_a2a.py、test_background_delivery.py；相关字段再加 result_watcher/device_queue 用例 |
| 后台查询/原聊天通知 | test_result_watcher.py、test_background_delivery.py、test_host_adapters.py |
| 设备身份/队列 | test_device_permissions.py、test_device_queue.py、test_a2a.py |
| 收件器/ACK/手机调度 | test_device_receiver.py、test_receiver_manager.py、test_minis.py、test_result_watcher.py |
| HTTPS/节点解析 | test_network_transport.py；集成再加 test_device_queue.py、test_device_receiver.py |
| 启停/安装/解释器/终端 | test_manager.py、test_receiver_manager.py、test_setup_mobile.py、test_console.py |
| CLI逐次来源 | test_send_context.py、test_send.py；身份上下文与实际 A2A 子进程交叉核验 |
| 恢复备用MCP | 先从唯一备用ZIP恢复，再跑 test_mcp_actions.py、test_send_context.py、test_setup_mcp.py；用独立SDK环境跑 test_mcp_stdio.py，来源与最终投递全部虚构 |
| 接入身份和真实调用链 | test_access_policy.py、test_managed_bridge.py、test_managed_client.py、test_workbuddy_managed.py、test_managed_setup.py、test_manage_upgrade.py。桥接联测使用真正的 A2A SDK、任务数据库和设备领取，最后一步宿主操作用虚构回执。覆盖任意方向与新伙伴自动互通、未知来源/越权领取拒绝及网页路由消失；上线还须验真实原聊天 |

跨模块、接口、共享状态、依赖或准备发布时，再跑全套：

~~~powershell
& $sdkPython -X utf8 -B -m unittest discover -s tests -v
if ($LASTEXITCODE -ne 0) { throw '整套检查失败，未发布' }
~~~

Windows 管理器测试在非 Windows 跳过；部分 Node 测试要求 Node 在 PATH。test_network_transport.py 的真实 TLS 用例需要测试环境已有 cryptography，缺少会跳过。跳过不算已覆盖，不因此自动安装新依赖。test_manager.py 会在临时目录造环境和本机服务，允许它正常结束，勿中途删测试目录。

整套 SDK 服务端测试放电脑跑；手机仅装客户端 requirements-mobile.txt，不为跑全套往手机补 SQLAlchemy/Uvicorn/MCP。Windows 原生依赖与 Node 的真机可用性，使用既有 setup.ps1 -Mode Check 和伙伴状态核对；它们不是单元测试替代品。日常 CLI 版没有活动 MCP 专用测试；只有恢复备用方案时才从 ZIP 恢复后，用独立 .mcp-env 运行其协议测试。缺 SDK 导致跳过不算协议验证成功。

仅文档更改核对链接、路径、命令语法和打包内容即可，不唤醒AI或重跑整套业务测试。测试数量随文件变化，以当前输出为准，不照抄历史总数。

真实验收在取得部署/发消息授权后进行：两边各自发一次带唯一标记的简单题；发件方收到 background 后结束，让目标原聊天在上一轮结束后自动开始，结果自动回来源原聊天。核对 ChatRef、任务号、原生回执/实际消息、答案/标记；再做一次同任务追问和来源冒认拒绝。不要一直开着等待轮次把 steer 混当 idle 唤醒。缓存默认十分钟，及时结束发件轮次；长时间忙碌后 expired 只能只读核对。

### 5. 发布、同步两端

先按改动区分，避免每次整包重装：

| 改动 | 同步/重启范围 |
|---|---|
| SKILL.md、references/setup.md | 同步公共文档，更新迁移包；不重启服务、不为文档派测试题 |
| 仅电脑原生/服务代码 | 更新电脑公共文件并重启受影响的长驻进程；手机若有该公共模块也同步版本，但不因此改配对 |
| 手机收件、后台回件、共享网络模块 | 更新两端相关公共文件；手机收件器和电脑服务按实际加载范围重启 |
| 配置/依赖/数据库接口 | 先核对授权、兼容顺序和私密备份；新依赖先报名称/版本/安装位置，未获授权不安装 |
| CLI入口与Skill联接 | 核对本次执行使用的来源环境、登记及目标；文档同步不等于运行进程已释放 |
| 备用MCP ZIP或恢复 | ZIP只进电脑迁移包；恢复时先隔离测协议，再按需安装单条宿主入口，不给手机增装依赖 |

运行中的 Python 不会可靠自动重载已导入模块；配置和地址簿也有启动时读取的部分。仅替换文件不能当作升级完成。

1. **保存上一可用版本。** 在各自设备本地备份即将覆盖的公共文件，列出新增文件及哈希。若要改配置，程序只备份/修改选定键，AI 不看真实值；私密备份不得放共享目录、聊天或发布 ZIP。运行数据库需一致性备份时先停组件或用 SQLite 备份接口，不在写入中随意复制主文件。
2. **确认没有在途任务。** 看 a2a-mailbox.db 的 replies（waiting）、device_deliveries 的 queued/尚待回件、pending-results.db 的 prepared/waiting/ready/dispatching，以及手机 receipts 的待确认项。只查这些状态/编号，不能 SELECT * 或输出正文/凭据；服务 online/HTTP200 不表示可以安全重启。已有不明投递必须先核对，不能把状态重置重派。维护期间暂停发新题。
3. **准备公共更新包。** 从已检查的候选按明确文件清单生成 ZIP，只含选中的 scripts、说明、入口和需求清单；列出每文件 SHA256、包 SHA256、修改前版本和两端适用范围，随同一条部署任务交接，不另造一堆说明文档。不包含 local.json、peers.json、runtime、validation、.a2a-env、备份或任何密钥。迁移 ZIP 和手机更新 ZIP 分开使用。
4. **用现有网络分发。** 本版手机清单由38项缩为36项，先通过既有 HTTPS 下载并核对本版 scripts/manage_upgrade.py 的 SHA256；随后统一使用这份新版维护脚本执行 inventory/verify/prepare/apply，不用旧版脚本验证新清单。用新版 scripts/manage_upgrade.py inventory --root 手机包目录 --target mobile 取得旧公共文件的哈希，build 选择 --baseline-manifest 或对应设备的 --baseline，并指定 --source、--target、--version、--output。将电脑生成的手机公共包放在 runtime/public-update.zip，手机通过 fetch --root 已安装包 --output 更新.zip --sha256 已核对的摘要 下载。首次升级时，接收方可先用既有配对 HTTPS 程序下载、核对整包及公共维护脚本的 SHA256，再运行脚本。凭据只供程序在本机使用。两端分别运行 verify、prepare、apply；用 --help 查看参数，--quiescent 表示维护者已确认没有在途业务并安全停止对应进程。
5. **设备侧先验后改。** 核对设备和目标包目录、完整 ZIP 哈希及文件清单；拒绝绝对路径、..、链接、越界目标和未允许文件。更新包不能覆盖配置/数据库/环境。用同目录临时文件＋原子替换逐文件更新，手机 sh 文件保留可执行权限；校验全部通过才替换，不边猜边覆盖。
6. **按需停/起组件。** 不兼容协议或数据库变化必须协调停机；兼容升级可先由旧收件器接下更新，再在其任务回件完成、没有其他待办后安排独立更新进程。不能让负责回件的收件器先杀掉自己再声称成功。若任务尚未完成且必须重启，明确报告中断/失败并核对成果，不静默重派。

以下管理命令仅在维护边界和授权已确认后执行；每一步分别看回执，失败停止：

~~~powershell
# 电脑：均在正式包根目录；Stop前已确认无在途任务
.\a2a.ps1 -Mode Stop
.\a2a.ps1 -Mode Start
.\a2a.ps1 -Mode Status
~~~

~~~sh
# 手机：确认独立维护进程可继续、回件已完成后
cd /var/minis/skills/ai-collaboration
PYTHON="$PWD/.a2a-env/bin/python" sh scripts/receiver.sh stop
PYTHON="$PWD/.a2a-env/bin/python" sh scripts/receiver.sh start
PYTHON="$PWD/.a2a-env/bin/python" sh scripts/receiver.sh status
~~~

替换代码安排在相应组件确认停止与重新启动之间。手机 stop 若仍返回 running/stopping，等该实例真正释放锁；不能按PID强杀。电脑由管理器核对进程路径、启动时间和实例，不手工批量杀 Python。

7. **验证与清理。** 比较两端应同步公共文件的 SHA256。电脑 Status 必须确认本项目实例；手机除 running 外，还要看到最近成功轮询持续推进。真 HTTPS health 只验固定service；完整实例由本地鉴权管理器查。涉及行为的更改再按上一节做真实原聊天验收。已开始的任务不能用停止服务“取消”。
8. **更新交付。** 已授权发布完成后在电脑正式根目录运行下面命令更新迁移包，再检查 ZIP 不含私密配置、环境和runtime；包中 peers.json 只作登记资料，新设备必须重建本端身份。必要事实写进原有 validation 记录和项目 tasks.md，不另存消息全文。设备确认下载后删临时公共ZIP和维护缓存，保留一份私密回退依据。

~~~powershell
.\setup.ps1 -Mode Pack
~~~

这只打包，不安装或重启；它默认输出正式根目录同级的 ai-collaboration-portable.zip。不要把文档里的历史哈希当作下次发布的新哈希。

### 6. 回退怎么做

本版公共文件按本次设备本地 prepare 留下的备份回退。先按维护顺序确认在途并停服务，再执行 scripts/manage_upgrade.py restore --root 包目录 --backup 本次公共备份目录 --quiescent。本机私密配置需单独处理。最初的 runtime/deployment-1108-backup.zip 仅对应首次部署。

本版沿用 CLI、A2A 服务、原生收件、网络、身份及数据库，删除网页和软件/方向授权。备用 MCP 的恢复或再次撤除只走上文单条配置流程；本机历史升级备份位置与适用版本从对应验证记录核对，不在本指南重复列过期版本。恢复公共代码不能顺带整体还原配置或密钥。

1. 停止发新任务，记录失败发生在哪个阶段、任务/通知号及两端当前版本；确认目标进程和在途状态，再按上一节安全停受影响组件。不能通过删数据库、锁或状态文件跳过未解决投递。
2. 从本次发布前的备份中按文件清单恢复上一个可用的公共文件；本次新增文件只在确认归属且旧版不需要时移除。不要删除整个正式包，也不要把电脑配置覆盖手机。
3. 配置只恢复本次确实变动的键，并保留之后用户手动更换的密钥、当前设备身份和无关配置；AI不读取/回显旧秘密、不自动轮换。普通代码回退通常不需动配置。
4. 默认保留当前runtime。若新代码做了旧版不兼容的数据库迁移，只能按发布前已验证的迁移回退方案处理，并核对备份后新任务是否已有实际执行；直接还原旧库可能丢去重状态并重做任务。没有安全方案就停止自动回退，报告缺口，不能编造成功。
5. 按相反兼容顺序恢复两端代码/依赖（仅在本次确实改过且有可用备份时），正常 Start/Status，比较哈希与最近轮询，再做必要最小验收。相同任务查询原task，不能为验证恢复重新执行原操作。
6. 本次只改代码时保留现有网络。确实需要撤销本次新增入口，只用 `tailscale serve --https=18443 off`；不 reset 全部 Serve，不碰另一项目443、DNS、Clash或路由。恢复入口按已批准记录操作，不临时拓宽权限。
7. 更新现有任务段落：恢复到哪一版、哪些事实已验证、哪些任务需人工核对，清理无用候选和临时包，保留必要备份。不要将“回退命令退出0”当作两端恢复成功。

### 7. 给下一位 AI 留什么

**新增聊天。** 当前设备级令牌需要唯一映射来源原聊天；同设备多个发件聊天要逐聊天来源凭据绑定，不能只改正文或发送参数来冒认另一条聊天。先把实际设备、软件和原聊天登记到本机 peers.json，再核对该设备配对和该软件实际收件接口。在同包目录运行 `scripts/managed_setup.py prepare --root . --verify 设备 软件 原聊天编号`，由程序保留已有来源凭据绑定或生成缺少的绑定；不显示或轮换现有密钥。随后按维护顺序重载服务。核对身份的动作是接入，不是软件/方向授权：完成接入的聊天可直接联系所有其他已接入聊天，未来伙伴同样适用。尚无真实收件接口的软件仍不能假装已支持自动唤醒。

手机通讯录使用 `phone-sources --bind 别名 设备 软件 原聊天` 核对已有配对中央目标的实际宿主字段，不复制电脑私密配置。手机从中央 Agent Card 的 conversation-message-v1 能力选择消息模式，领取携带 message_version=1。新增目标也须在发件设备通讯录登记对应的真实来源/目标地址；这是聊天寻址，不是按软件限制联系。

**核查回执。** 已认证来源可查 `GET /collaboration/receipts/{message_id}`，返回目标必须与发前记录一致。查不到保持待确认，到期限仍未知才显示送达未确认。原请求外部结果可按同一所有者查询 `?include_result=1`，缓存十分钟。收据确认不会启动接收 AI 的额外轮次。

**网页已删除。** 程序没有 /ui/、网页密码、权限编辑器、正文预览缓存或网页模式切换命令。`send.ps1 -List`、`send.ps1 目标 -Status`、`a2a.ps1 -Mode Status` 和 `managed_setup.py inspect` 提供必要的本机查看。旧 managed_ui 字段仅兼容旧版本的来源/票据保护，不能恢复网页；旧 device_permissions、message_permissions、ui-policy.json 均不决定联系方向。未开启消息模式的旧任务服务只保留直接本机、无转发头的保留令牌调用；设备或网络来源不能借旧模式绕过原聊天绑定，新业务交流须走已绑定的消息入口。不要用恢复旧网页密码文件来解决收发问题。

**本版退役与回退。** 当前公共清单为电脑66项、手机36项。网页退役7项：scripts/ui_api.py、scripts/activity.py、ui/index.html、ui/app.js、ui/style.css、tests/test_ui_api.py、tests/test_activity.py。手机公共清单只涉及前两个模块。更新工具只替换白名单公共文件，不自动递归删除任何路径；升级时另按这份固定退役清单与本次本机公共备份逐文件核对哈希，拒绝链接及越界，再移除同版旧文件。若旧 runtime/ui-policy.json 存在，先由程序只输出待核对登记和明确停用聊天的数量；必要申请迁移到通讯录，明确停用状态保留到对应 peers.enabled=false，不能顺带重新接入停用聊天或盲删策略。口令文件不存在则不创建；已有文件停用时不读取或显示口令、salt、hash。

回退先还原本次 prepare 的66项（手机36项），再从同一次公共退役备份恢复上述7项（手机2项），最后重新启动旧服务。只还原公共文件，不整体覆盖当前身份、密钥或数据库。旧政策材料不随迁移包分发；部署事实及唯一公共回退ZIP位置记在原有 validation 和项目 tasks.md。

在现有任务记录中只保留：目标与本次改动、已发布版本/公共文件哈希、相关测试命令和实际结果（含跳过）、两端状态/原聊天验收、尚未解决项，以及回退文件位置和兼容性。正式程序说明集中维护本文，项目目标与网络历史分别沿用已有文件。

当前限制仍包括：单上游paired_ip映射；自动来源识别限已实现的软件；ACP尚未实现；备用MCP来源适配仅覆盖已核验的WorkBuddy、Codex，Minis当前客户端没有逐次转发所需上下文；未配置电脑开机启动；手机锁屏/换网/强停恢复未验；源聊天久忙可能超过等待或结果缓存。本文不扩大这些能力，也不授权自动部署。


## 独立仓库与 Git 流程

本仓只维护 AI 协作程序，与手机操控项目的 mobile-ops、phone-browser 分别保存代码、提交记录、分支、版本和发布。程序的日常发件与 Git 无关，不给其他 AI 额外安装 Git 工具。

1. **分开目录。** 本仓保存公共源码；实际设备的安装目录保存身份、路径、凭据和 runtime。源码修改先在本仓隔离验证，部署时才按公共清单选择性同步，不在安装目录直接提交全部文件。
2. **新机器模板。** 从本仓取得源码后，在实际安装目录把 local.example.json 复制为 local.json、peers.example.json 复制为 peers.json，再按上文核对实际设备、宿主路径、原聊天和配对。模板没有真实身份或凭据。生成的迁移 ZIP 中 peers.json 也只是虚构模板，不能覆盖已有安装配置。
3. **独立改动。** 从本仓当前 main 建自己的工作分支；多个 AI 同时修改本仓时，各自使用独立分支及工作目录。手机操控仓的分支和 Git 配置保持原状。不要修改全局 Git 配置。
4. **核对后暂存。** 用 git status --short 查看本仓，检查具体差异和相关测试，只 git add 明确核对过的文件。git diff --cached --name-only 与 git diff --cached --check 用来核对暂存范围和空白错误；.gitignore 不能阻止强制加入或已追踪的秘密，因此每次仍需检查暂存正文、文件名单及所有压缩包内部。
5. **隐私与作者。** 真实用户路径、设备名、私网域名、聊天编号和凭据不进源码；测试使用明显虚构值。公开提交使用该 GitHub 账号的 noreply 邮箱，配置仅作用于本仓。不把 token 放在命令、远端 URL、Git 配置、提交信息或日志里。
6. **提交与远端分开。** 本地检查和提交完成后才准备远端；用户尚未允许上传时，不创建远端仓库、分支、PR、标签或发布。取得明确指示后只推送本仓，不自动合并其他项目的改动。遇到并发更新先核对差异，不用强推覆盖。
7. **发布与部署分开。** 在已核对的提交生成公共迁移 ZIP，检查清单、模板及逐文件哈希，排除真实配置、runtime、validation、环境和回退备份。源码发布不触发设备安装；需要部署时再按本文既有维护顺序同步和回退。

本仓不复制其他项目的开发记录。阶段结论保留目标、当前提交、测试/隐私检查和未完成事项，清理临时导出与重复记录；使用说明和修改说明继续只维护 SKILL.md 与本文。
