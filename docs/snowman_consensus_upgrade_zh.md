# Quorum Snowman++ 共识升级完整实施方案

本文档记录将 Quorum 当前未完成的 multi-producers 出块实现，整体替换为 Avalanche Snowman++ 共识协议的实施方案。

本升级不兼容现有 HBBFT、PTBFT、RBC、ACS、Molasses BFT 实现，不迁移旧链数据，不支持双协议并行。升级后的网络需要使用新 genesis、新数据库和新协议版本启动。

## 目标

1. 使用 Snowman++ 作为唯一线性链共识协议。
2. 移除现有半成品 HBBFT/PTBFT/RBC/ACS 出块路径。
3. producer set 作为 permissioned validator set。
4. 支持多 producer 对同一高度 block candidate 进行随机子采样投票并最终收敛。
5. 普通节点只跟随 Snowman++ accepted chain，不再只信任 group owner 出块。
6. 同步逻辑基于 accepted frontier 和 ancestors 拉取，不再基于单 owner provider。

## 非目标

1. 不兼容旧数据。
2. 不保留旧 HBBFT/PTBFT/RBC/ACS 消息。
3. 不实现 Avalanche X-Chain 的 DAG Avalanche engine。
4. 不做 stake 权重，初版所有 producer 等权。
5. 不保留旧 producer channel 与 user channel 的共识语义。

## 协议选择

采用 permissioned Snowman++。

Snowman 是 Avalanche 家族中的线性链协议。它通过重复随机子采样 query/chits 让节点对某个 block preference 收敛，并在连续多轮达到阈值后 accept。Snowman++ 在 Snowman 上增加 proposer window，以减少同高度并发冲突。

本项目的数据结构是线性 block chain，因此选择 Snowman++，而不是 Avalanche DAG 协议。

## 关键术语

- Producer：当前 group 中被批准参与出块和投票的节点。
- Validator：Snowman++ 共识视角下的 producer。
- Preference：节点当前偏好的 block。
- Processing：已验证但尚未 accepted/rejected 的 block。
- Accepted：已最终确认的 block。
- Rejected：与 accepted 分支冲突或无效的 block。
- Poll：一次随机子采样投票请求。
- Chits：被采样节点返回的偏好投票。
- Accepted frontier：节点已接受链的前沿 block。

## 共识参数

新增 Snowman++ 参数：

- `K`：每轮随机采样 validator 数量。
- `AlphaPreference`：更新 preference 的投票阈值。
- `AlphaConfidence`：增加 confidence 的投票阈值。
- `BetaVirtuous`：无冲突 block 的连续成功轮数阈值。
- `BetaRogue`：存在冲突 block 的连续成功轮数阈值。
- `ConcurrentRepolls`：并发 repoll 数。
- `OptimalProcessing`：目标 processing block 数。
- `MaxOutstandingPolls`：最大未完成 poll 数。
- `RoundTimeoutMs`：poll 超时。
- `ProposerWindowMs`：单个 proposer window 长度。
- `ProposerWindowCount`：优先 proposer window 个数。

初始建议值在测试网中调优，不写死在业务代码里。

## 数据模型

升级后的 block 必须包含：

- `GroupId`
- `BlockId`
- `Epoch`
- `ParentBlockId`
- `ParentBlockHash`
- `BlockHash`
- `ProducerPubkey`
- `ProducerSign`
- `TrxRoot`
- `StateRoot`
- `ProducerSetVersion`
- `ProposerIndex`
- `ProtocolVersion`

其中 `ProtocolVersion` 固定为 `snowman++`。

新增 block 状态：

- `UNKNOWN`
- `PROCESSING`
- `ACCEPTED`
- `REJECTED`

新增持久化索引：

- block hash 到 block。
- block hash 到 status。
- parent hash 到 children。
- height 到 accepted block hash。
- last accepted block hash。
- producer set version 到 producer set。
- pending block dependency。
- peer accepted frontier cache。

## Producer Set

现有 producer 管理 API 保留业务入口，但语义升级为 validator set 管理。

规则：

1. group owner 仍是初始 validator。
2. producer set 更新必须通过 accepted block 生效。
3. producer set 更新交易必须带 `EffectiveBlockId`。
4. 当前 block 的验证使用 parent block 对应的 active producer set。
5. 新 producer set 不影响正在 processing 的旧高度候选。

## P2P 消息

移除旧 `HBMsgv1`、`RBCMsg`、`BBAMsg`。

新增 Snowman++ 消息：

- `PutBlock`：广播或发送完整 block。
- `GetBlock`：按 block hash 请求 block。
- `GetAncestors`：请求 ancestor blocks。
- `Ancestors`：返回 ancestor blocks。
- `PushQuery`：携带 block 的投票请求。
- `PullQuery`：只携带 block hash 的投票请求。
- `Chits`：返回当前 preference。
- `GetAcceptedFrontier`：请求 accepted frontier。
- `AcceptedFrontier`：返回 accepted frontier。
- `GetAccepted`：询问一组 block 是否 accepted。
- `Accepted`：返回 accepted block 列表。

所有共识消息必须包含：

- `GroupId`
- `RequestId`
- `SenderPubkey`
- `Timestamp`
- `Payload`
- `Signature`

签名范围必须覆盖 `GroupId`、`RequestId`、消息类型、payload hash 和 timestamp。

## 出块流程

1. producer 从 mempool 选取交易。
2. 根据 parent hash、height、producer set version 计算 deterministic proposer order。
3. 如果当前节点处于自己的 proposer window，创建 block candidate。
4. 对 block 进行签名。
5. 本地验证 block。
6. 将 block issue 到 Snowman engine。
7. 广播 `PutBlock` 或 `PushQuery`。

如果所有 proposer window 超时，进入开放窗口，任意 validator 可提议 block，以保证 liveness。

## 投票流程

1. engine 对 processing block 启动 poll。
2. 从 active validator set 中随机采样 `K` 个 validator。
3. 发送 `PullQuery` 或 `PushQuery`。
4. validator 只对已验证、parent 已知且 producer 合法的 block 投票。
5. poll manager 汇总 `Chits`。
6. 若某 preference 达到 `AlphaPreference`，更新本地 preference。
7. 若达到 `AlphaConfidence`，增加 confidence。
8. 连续达到 `BetaVirtuous` 或 `BetaRogue` 后 accept。

## Accept/Reject 流程

Accept block 时：

1. 确保 parent 已 accepted。
2. 写入 accepted block index。
3. 将同 parent 的冲突 sibling 标记为 rejected。
4. apply block transactions。
5. 更新 last accepted。
6. 清理 mempool 已上链交易。
7. 广播 accepted frontier。

Reject block 时：

1. 标记 block rejected。
2. 递归 reject descendant。
3. 保留交易回 mempool，除非交易已在 accepted chain 中出现。

## 同步流程

新节点启动：

1. 向 peers 请求 `GetAcceptedFrontier`。
2. 对 frontier 做采样确认，选择多数 accepted frontier。
3. 使用 `GetAncestors` 拉取祖先链。
4. 从 genesis 到 frontier 验证 hash、签名、producer set 和交易。
5. 写入 accepted chain。
6. 切换到在线 Snowman++ 共识。

落后节点：

1. 收到未知 parent block 时缓存为 dependency。
2. 发起 `GetAncestors`。
3. 补齐 parent 后再 issue block。

不再使用 “只从 owner 接收 BLOCK_NOT_FOUND” 的逻辑。

## 需要删除或替换的旧模块

删除或停止编译：

- `pkg/consensus/trxbft.go`
- `pkg/consensus/trxacs.go`
- `pkg/consensus/trxrbc.go`
- `pkg/consensus/rbc.go`
- `pkg/consensus/bba.go`
- `pkg/consensus/msgsender.go` 中旧 HB/RBC/BBA 发送逻辑

替换：

- `pkg/consensus/molassesproducer.go`
- `pkg/consensus/molassesuser.go`
- `pkg/consensus/molasses.go`
- `pkg/consensus/def/*`
- `internal/pkg/chainsdk/core/chain.go` 中 owner-only block 验证逻辑
- `internal/pkg/chainsdk/core/rexsyncer.go` 中 owner-only sync 逻辑
- `internal/pkg/conn/connmgr.go` 中 producer channel 共识语义

## 新模块规划

新增：

- `pkg/consensus/snowman/params.go`
- `pkg/consensus/snowman/block.go`
- `pkg/consensus/snowman/engine.go`
- `pkg/consensus/snowman/poll.go`
- `pkg/consensus/snowman/proposer.go`
- `pkg/consensus/snowman/bootstrap.go`
- `pkg/consensus/snowman/validator_set.go`
- `pkg/consensus/snowman/signer.go`
- `pkg/consensus/snowman/metrics.go`

新增 protobuf：

- `pkg/pb/snowman.proto`

## 安全要求

1. 所有共识消息必须签名。
2. 所有 block 必须校验 producer set、parent hash、block hash 和 producer signature。
3. query/chits 必须绑定 request id。
4. poll 结果只接受采样名单中的 validator 响应。
5. 同一 request 中每个 validator 只能投一次票。
6. 节点不得为未知 parent 或无效 block 投票。
7. producer set 变更只在 accepted block 后按 effective height 生效。

## 测试计划

必须覆盖：

1. 单 producer 正常出块。
2. 3 producers 正常收敛。
3. 4 producers，1 个离线。
4. 10 producers，3 个离线或慢响应。
5. 同高度多个 block candidate，最终只 accept 一个。
6. proposer window 超时后开放窗口出块。
7. 恶意节点返回错误 chits。
8. 节点重启后从 last accepted 恢复。
9. 新节点通过 accepted frontier bootstrap。
10. producer set update 在指定高度生效。
11. 大交易量下 block size、mempool 去重和交易回收。

## 实施阶段

1. 增加文档、Snowman++ proto、参数与接口定义。
2. 实现 Snowman++ engine 与单元测试。
3. 实现 block 状态存储和 accepted chain 索引。
4. 替换 producer 出块路径。
5. 替换 block 验证和 apply 逻辑。
6. 替换 P2P 共识消息。
7. 替换 sync/bootstrap。
8. 改造 producer API 为 validator set API。
9. 删除旧 HBBFT/PTBFT/RBC/ACS 编译路径。
10. 增加多节点仿真测试。

## 完成标准

1. 项目不再依赖 HBBFT/PTBFT/RBC/ACS 出块模块。
2. `go test ./pkg/consensus/...` 通过。
3. `go test ./internal/pkg/chainsdk/...` 通过。
4. 至少一个本地多节点测试能证明多个 producer 对同一高度冲突块收敛到单一 accepted block。
5. 普通节点不再只接受 owner 出块。
6. 同步逻辑不再只信任 owner。
