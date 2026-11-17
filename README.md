# Telecapture：环境遥测离线录制入仓

环境遥测接入中心的接入员导入网关提供的 UTF-8 JSONL 录制；质量分析员读取完整测量、质量标记和原消息证据。V1 处理已经解码的有序 SCTP I-DATA 与 I-FORWARD-TSN，不连接网络或物理设备。需要 Python 3.11 标准库，无安装依赖。

## 一天的工作

1. 接收录制，读取唯一首行固定头；头后 EOF 是合法空录制。
2. 登记录制、网关、会话和流的固定初始条件。同身份同头幂等，冲突拒绝。
3. 逐行验证事件，保存文件、行号与原行 SHA-256；坏尾行必须报错。
4. 原子接纳 TSN、核对分片身份；冲突拒绝整个 push。
5. 按流、逻辑 MID、FSN 还原正文，首尾齐备且 FSN 连续才完整。
6. 来源放弃终结指定消息，保留后继原文并释放流内阻塞。
7. 每次 push 后 drain 全部当前合格消息；不等待 EOF，各流独立推进。
8. 全批解码为整数微单位，在同一个 SQLite 事务中发布测量和证据。
9. 按网关、流、半开时间范围查询；重放固定录制核账，不重复入仓。

## 运行

从项目根目录执行：

    sh verify.sh
    sh demo.sh
    python3.11 -B -m telecapture.measurements.ingest --db /tmp/telecapture-example.sqlite import samples/normal.jsonl
    python3.11 -B -m telecapture.measurements.ingest --db /tmp/telecapture-example.sqlite read --stream 7 --start-us 100 --end-us 103

verify.sh 执行完整 unittest discover 并传播退出码。demo.sh 在独立临时目录实际导入正常、缺片放弃样例，重放、查询、断言并重开核账，结束清理数据库。两份运行清单使用相同真实 argv。CLI 成功退出 0，错误向 stderr 输出 JSON error 并退出 2。数据库由用户指定，不清空其他录制。

## 公开 Python API

- telecapture.recording.reader.open_recording(path) 返回上下文管理器 RecordingInput。header 是不可变 RecordingHeader，events() 是单次迭代器；EOF、异常或 close 都关闭文件。末行可没有换行符，但必须是完整 JSON。
- telecapture.recording.header.parse_header(dict) 和 telecapture.recording.events.decode_event(dict, evidence=None) 提供离线端口。也可直接使用不可变 IDataEvent、ForwardEvent、ForwardEntry。
- telecapture.association.lifecycle.open_session(header, registry=None) 创建独立运行状态。push(event) 原子接纳，drain() 返回 DeliveredMessage 列表。finish_input() 返回剩余不完整的 (stream_id, logical_mid) 元组。结束后仍可 drain，不能再 push；EOF 不等于放弃。
- telecapture.measurements.archive.Archive(path=':memory:', fault=None) 是上下文管理器。register_recording(header) 核对固定头，commit(messages) 返回新建证据数，read(**filters) 返回 MeasurementView，evidence() 可查询没有测量行的合法空消息证据。
- telecapture.measurements.ingest.import_recording(path, archive) 返回含 events、delivered、inserted、incomplete 的 IngestReport。需要同进程存储失败重试时，保持 open_recording 上下文，创建 IngestRun(recording, archive) 并调用 run()；失败后 pending 保存原批次，retry_pending() 成功后再次 run() 继续。解码失败应修正原录制并新建运行，不得将坏批次冒充已提交。V1 不提供进程中断续采。

DeliveredMessage 含录制、网关、会话、流、逻辑 MID、原始 MID、body 和 fragments。Fragment 保留 TSN、FSN、标记、字节及来源位置；归档保存原文、摘要和片段来源。来源路径、行号与运行时间不进入稳定身份。跨流 drain 按数字流号遍历，不声明跨流网络顺序。

## JSONL 输入

首行示例：

    {"version":1,"recording_id":"day","gateway_id":"gw","session_id":"s","initial_cumulative_tsn":99,"streams":[{"stream_id":7,"initial_mid":0}]}

后续每行严格采用一种字段集合：

    {"type":"I-DATA","session_id":"s","tsn":100,"stream_id":7,"mid":0,"fsn":0,"begin":true,"end":true,"payload_b64":"Qw=="}
    {"type":"I-FORWARD-TSN","session_id":"s","cumulative_tsn":102,"entries":[{"stream_id":7,"mid":0}]}

拒绝未知字段、重复 JSON 键、非法数字、布尔代整数、不支持的版本或事件类型。身份是非空字符串；流号 0..65535，声明中不可重复。payload_b64 严格 base64 解码。上面的 C 只是原字节传输示例，不能当作合法测量；samples 中正文是合法测量 JSON。

TSN/MID 为 uint32，按半范围串行规则比较；活动范围须小于 2^31，恰半范围歧义报 CaptureConflict。逻辑 MID 从 initial_mid 展开，回绕不重用归档身份。FSN 是非负普通整数，FSN0 必须带 begin，其他 FSN 不得带 begin；唯一 end 确定末片，0..end 每个位置都到达才完整。

同 TSN 的会话、流、逻辑 MID、FSN、标记或正文变化报 CaptureConflict；同消息 FSN 的标记或正文变化也冲突。来源行号变化不构成冲突。同内容分片用其他 TSN 重传不重复交付。已交付或放弃的迟到事件仍须检查已知身份冲突。

累计接收 TSN、来源通知位置和每流 MID 游标分别维护。放弃条目终结指定流从初始位置至所指逻辑 MID 的尚未交付消息；完整但尚未 drain 的消息也可被覆盖。未被放弃消息的原文保留。严格早于最后已处理来源通知累计 TSN 的旧通知整次无效；相同位置可幂等合并各流范围，范围不回退。数据接收可令累计位置超过来源通知，两者不混作锚点。

push 在工作副本中完成全部计算后一次提交；失败不改变接收、片段、终结、可交付集合和游标。drain 为不可撤回交付，与 SQLite 提交成功不同，协调器保存未提交批次。

## 测量计算与查询

正文为 UTF-8 JSON 对象：

    {"version":1,"measurements":[{"sensor_id":"t","time_us":100,"quantity":"temperature","unit":"mC","value":-1250,"quality":"good"}]}

时间和数值使用任意精度整数。temperature 接受 mC（乘 1000）或 uC（乘 1），输出 uC；pressure 接受 Pa（乘 1000000）或 uPa（乘 1），输出 uPa。value 是有符号整数或 null，全程整数运算；SQLite 将时间和规范值存为十进制文本，API 返回 Python int，不经浮点数。quality 为 good、suspect、missing，仅 missing 对应 null，其他质量必须对应整数。sensor_id 为非空字符串。空 measurements 数组只保存证据。

任一消息或行不合法，整个 commit 批次失败，不发布合法前缀。同稳定身份同原文幂等，同身份异原文拒绝。身份、原文、摘要、来源及测量在同事务发布。可选 fault(stage) 在 after_evidence、after_measurement、before_commit 调用；回调异常使真实事务回滚。

read 参数为 gateway_id、stream_id、start_us、end_us。时间范围 [start_us,end_us)，端点可省略，相等端点为空；逆序和错误类型报 ArchiveError(query)。按 time_us、gateway_id、session_id、stream_id、logical_mid、载荷内行号排序；完全同序时用 recording_id 决胜。MeasurementView 带原 body 和 delivery_key，as_dict() 将 UTF-8 body 转为字符串。

## 初始化与失败边界

只创建缺失的受支持表，重复打开和登记不改变成果。用户其他合法录制及无关表保留。没有迁移、清库或自动重建接口。结构、外键、原文摘要或测量与原文不一致时拒绝读取/重开，不覆盖损坏内容。V1 为单工作者离线导入，不提供并发写入或会话迁移。

RecordingError 带源行号，区分坏输入和 EOF；AssociationError 拒绝错会话、未知流及冲突头；CaptureConflict 拒绝接收身份及片段矛盾。ArchiveError.reason 分 payload、identity_conflict、schema、storage，查询错误为 query。坏行前成功提交的完整消息保留，整份录制不是一个事务。存储失败不发布半份成果。

## 确定性回归

- tests/test_capture.py：两流交错、精确单位、质量空值、过滤查询、尾先首后、FSN 缺口、标记冲突、坏尾行及关联拒绝；CLI 坏录制须退出 2。
- tests/test_abandonment.py：逐 push 核对 A/C/放弃/迟到 B、未覆盖流、双序号回绕、重复及旧通知，与独立小域排列模型比较。
- tests/test_replay.py：重放、独立身份、空录制、完整状态快照及冲突后续接。
- tests/test_archive.py：坏批次无前缀、事务故障、重开、重试、初始化保留及损坏拒绝。

预期采用固定整数与独立模型，不用待验证函数生成。场景标记在真实断言后打印实测结果。相同头、录制顺序及初始归档产生确定性结果。
