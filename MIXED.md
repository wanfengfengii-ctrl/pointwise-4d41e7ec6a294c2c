# 混合有序／无序遥测录制增量

## 现有能力与本次目标
README.md 描述已可运行的有序 V1：严格 JSONL、关联登记、共享 TSN 判重、FSN 重组、有序交付和来源放弃、精确测量、原文证据与 SQLite 原子提交。本次增加同一关联内的无序消息；这些能力尚未实现。复用原入口、全部原测试、两个原样例及运行清单。下面仅覆盖混合模式增量；其他 V1 规则继续适用。

共同结果：有序缺片不阻塞完整无序测量，同流同 MID 的两种模式分别交付和入仓，模式定向放弃不伤及另一模式。不限扫描算法，不新增并发、内存、网络或进程恢复目标。

## 公开入口与兼容
open_recording(path)、open_session(header)、push、drain、finish_input、Archive、IngestRun、import_recording 及现有 import/read 命令接通混合录制。
decode_event(value, evidence=None) 保持 V1 严格字段校验；新增关键字 mixed=True 时才接受下面的模式字段。JSONL 读取器使用扩展解码入口，无需额外命令行选项。旧测试中拒绝 unordered 的直接调用必须继续通过，不能修改原测试或按调用栈区别行为。
IDataEvent、ForwardEntry、DeliveredMessage 增加布尔 unordered，缺省 false；保持所有原位置参数含义，包括 evidence 和 fragments。新参数可用关键字传入，整数 0/1 不是布尔值。

## 输入
录制仍为 version=1。流声明允许额外 uint32 initial_unordered_mid，缺省 0；initial_mid 只定义有序空间。RecordingHeader 的 streams 仍支持原有二元项读取及旧构造调用。显式无序初值 0 与省略表示相同固定配置，原有头的规范串不得改变；非零初值参与头冲突核对、存储和重开。
I-DATA 允许额外 unordered，I-FORWARD-TSN 的每个 entries 项允许额外 unordered，均为布尔且缺省 false。其他字段、未知字段拒绝、重复 JSON 键、base64、关联和流校验沿用 V1。ForwardEvent 本身没有单一消息模式。

例：流声明 {"stream_id":7,"initial_mid":0,"initial_unordered_mid":0}；数据项在 README 原格式上加 "unordered":true；放弃项 {"stream_id":7,"mid":0,"unordered":true} 只针对无序空间。完整可导入输入是 samples/mixed.jsonl；mixed_check.py 中的 body/header/data/forward 仅构造有限夹具，不实现采集状态。

## 身份、展开与交付
同一关联共享 TSN 接收账本；相同 TSN 改变 unordered 也属于身份冲突。不同 TSN 的同流、同 MID、同 FSN 可以属于不同模式，不能混合正文或相互冲突。FSN 首尾、连续性、相同消息分片冲突及迟到已知身份核对保持 V1。
消息身份为流、模式及逻辑 MID。有序 MID 沿用原规则。无序 MID 以该流 initial_unordered_mid 为初始展开锚点，此后锚点只前进到已接纳分片或有效放弃项的最大逻辑 MID；交付不会把未终结低 MID 关闭。沿用 unwrap32 的半范围解释，恰半范围拒绝，早于本空间初始值的消息不得交付。同一 push 内放弃项均按 push 前锚点展开，再合并同流同模式的最大有效范围，条目次序不影响结果。
无序完整消息不等待低 MID 或有序消息。一次 drain 按流号、模式 false 在前、逻辑 MID 排序。已经 drain 的身份永不重复交付，但高 MID 已交付不能阻止尚未终结的低 MID 随后完成。完整但未 drain 仍不算已交付。
放弃项仅终结指定流和模式从初始值至所指逻辑 MID 的未交付消息，包括完整但未 drain 的消息；不撤回已 drain 成果，迟到分片不能复活。累计 TSN、通知位置、新旧与同位置通知合并仍遵守 V1；旧通知不得推进任一模式的放弃或展开锚点，TSN 前进本身不表示消息完成。
失败 push 不得改变任一模式或共享接收事实。drain 仍不代表归档成功，原 pending 批次及 retry_pending 继续适用。
finish_input 对有序未完整消息保留 (stream_id,logical_mid)，无序返回 (stream_id,logical_mid,true)；按流号、模式、逻辑 MID 排序。完整但受有序前驱阻塞的消息与 V1 一样不计入未完整项。

## 归档交接
DeliveredMessage.unordered、MeasurementView.unordered、as_dict() 和 evidence() 项的 unordered 均能辨认模式，旧有序成果显示 false。
有序 key 保持原字符串值，无序 key 使用不与任何有序 key 相交的稳定命名空间。无序键的具体编码不限定，但只由原身份字段、模式和逻辑 MID 决定，不能依赖到达次序、路径或正文。make_delivery_key 的旧调用含义保留，可增加缺省 false 的 unordered 参数。
Archive 必须独立接受通过公共构造器创建的合法 DeliveredMessage，不得依赖接收器私有状态。按对应模式的初始 MID 核对逻辑身份和 raw_mid。两种模式同流同逻辑 MID 分别保存原文、摘要、来源与测量；重放各自幂等，同一模式同身份异正文报 ArchiveError(identity_conflict)。
沿用整批载荷验证、证据与测量同事务发布、故障注入及失败重试。任何一份坏载荷或提交失败均不得发布本批另一模式的合法前缀。
旧 V1 数据库仍可读取、重放和追加，原有序 key、原文及测量不改写；保留无关表，损坏拒绝语义不减弱。内部存储表示由实现选择。
read 的原过滤参数不变；排序为 time_us、gateway_id、session_id、stream_id、unordered、logical_mid、row_number、recording_id。读写及重开核验使用同一模式身份规则。

## 联合样例和交付核验
samples/mixed.jsonl 的流7两种初始 MID 均为0。逐事件 drain 的 (unordered,logical_mid) 应为：
[]；[(true,1)]；[(true,0)]；[]；[(true,2)]；[]；[]。
有序0只有首片，期间无序1、0依次完整并分别交付；随后仅放弃有序0，再交付无序2，迟到有序尾片和无序重复不交付。三份原文分别属于 u1、u0、u2，查询按时间得到 10000、11000、12000 uC。导入新增证据3份，重放新增0份，重开仍保留这三份完整结果。
mixed_check.py verify 还检查两种模式 MID0 的独立直接入仓、模式变更冲突、完整未 drain 放弃、真实事务回滚及 pending 重试。它是有限公开回归，不能替代独立小域参照。补充按显式逻辑身份生成预期的小域对照，不调用生产展开或队列函数制造答案；覆盖高 MID 先交付、低 MID 晚完成，两种空间各自回绕，旧／同位置通知、迟到冲突、失败 push 原子性及混合归档失败重试。
保留全部原测试字节和断言。完整旧回归仍由 sh verify.sh 运行；混合公开回归运行 python3.11 -B mixed_check.py verify，夹具自检运行 python3.11 -B mixed_check.py self-check。夹具自检只核对材料，不能当作目标功能通过。
交付时将混合业务核验加入运行清单的 verify，并让 demo 实际展示样例交付、入仓、重放及重开结果，保留旧演示。沿用 .pointwise-runtime.json 和 RUN_MANIFEST.json 的原结构及 argv 格式，两份保持一致；失败传播非零退出码，原 CLI 业务失败仍退出2。补充简短状态归属与不变量说明，不提供网络服务、UI、流重置或跨进程续采。
