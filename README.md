# astrbot_plugin_dcqq_relay

在 **Discord** 与 **QQ / OneBot**（以及 AstrBot 支持的其它平台）之间双向同步消息的 AstrBot 插件。

本项目是 [koishi-plugin-dcqq-relay](https://github.com/koishijs/koishi-plugin-dcqq-relay)（Apache-2.0，作者 [LTC](https://github.com/shigma)）的 AstrBot 移植版，并在此基础上增加了：

- **一条配置即双向**：一条关系同时负责 Discord → QQ 和 QQ → Discord，不再需要分别配置两个方向
- **聊天内绑定**：管理员在任一频道发 `/绑定` 拿到绑定码，到另一端发 `/完成绑定 <码>` 即可互通，无需手抄频道 ID
- **忽略 @全体成员** 开关（`@everyone` / `@here` / `@全体成员`）
- **LLM 自动翻译**：可分别为两个方向设置目标语言，使用 AstrBot 已配置的聊天模型
- **Discord Webhook 身份发送**：让 QQ 用户的**昵称和头像**在 Discord 中直接显示
- 撤回 / 删除同步、Discord 编辑同步、历史消息恢复
- 消息 ID 映射持久化到 SQLite，重启不丢

---

## 安装

### 方式一：AstrBot WebUI 插件市场

在「插件」页搜索 `dcqq_relay` 或直接粘贴仓库地址安装。

### 方式二：手动安装

```bash
cd AstrBot/data/plugins
git clone https://github.com/xZhongjie/astrbot_plugin_dcqq_relay.git
```

然后在 WebUI「插件」页重载插件（或重启 AstrBot）。

依赖：`aiohttp`（AstrBot 本体已自带）。若要使用 Discord，需要已启用 AstrBot 的 `discord` 平台适配器。

---

## 聊天内绑定（推荐）

不用手抄任何 ID，两步完成：

1. 在 **Discord 频道** 或 **QQ 群** 里（需要管理员）发送：

   ```
   /绑定
   ```

   机器人会返回一个 6 位绑定码，例如 `483920`，有效期 5 分钟。  
   也可以带名称：`/绑定 主群互通`。

2. 到 **要互通的另一端** 发送：

   ```
   /完成绑定 483920
   ```

   绑定成功后立即开始双向同步，并自动写入插件配置（重启不丢）。

```
QQ 群  /绑定                    → 绑定码 483920
Discord 频道  /完成绑定 483920   → ✅ 绑定完成，双向同步生效
```

说明：

- 两端都必须是 **AstrBot 管理员**（或在 `admin_ids` 里）。
- 其中一端必须是 **Discord 频道**，另一端可以是 QQ 群/私聊，也可以是 AstrBot 支持的其它平台会话。
- 绑定码 5 分钟内有效、一次性使用；重复绑定同一组合会提示已绑定。
- 解除绑定：`/解绑 1`（序号见 `/relay_status`），或到 WebUI 删除对应关系。
- 如果配置保存失败（例如配置目录只读），绑定仍会在本次运行生效，但会提示你到 WebUI 手动补上。

也可以继续在 WebUI 的「互通关系」里手动配置，两种方式等价。

---

## 配置

配置位于 WebUI「插件 → DC-QQ 互通 → 配置」。

### 互通关系（`relations`）

**一条关系 = 一个 Discord 频道 ⇄ 一个 QQ 群 / 私聊，两个方向同时生效。**

| 字段 | 说明 |
| --- | --- |
| `name` | 关系名称，仅用于日志和 `/relay_status` |
| `discord_guild_id` | Discord 服务器 ID（可选） |
| `discord_channel_id` | Discord 频道 ID（必填，右键频道 → 复制频道 ID） |
| `forward_platform` | QQ 侧平台适配器 ID（WebUI「消息平台」里该实例的 ID，通常为 `aiocqhttp`） |
| `forward_channel` | QQ 群号 / 私聊 QQ 号（必填） |
| `forward_message_type` | `GroupMessage`（群聊）或 `FriendMessage`（私聊） |
| `show_forward_id` | QQ → Discord 时是否在消息里带上 `[QQ:123456]` |
| `download_asset` | Discord → QQ 时先把资源下载为 base64 再发送（Discord CDN 被墙时用） |
| `reverse_name` | 只有图片/视频时，把用户名放到内容之后（更符合 QQ 显示习惯） |

等价的 JSON 形态（例如直接编辑 `data/config/astrbot_plugin_dcqq_relay_config.json`）：

```json
{
  "relations": [
    {
      "name": "主群互通",
      "discord_guild_id": "123456789012345678",
      "discord_channel_id": "123456789012345678",
      "forward_platform": "aiocqhttp",
      "forward_channel": "987654321",
      "forward_message_type": "GroupMessage",
      "show_forward_id": true,
      "download_asset": false,
      "reverse_name": false
    }
  ]
}
```

### 其它配置

| 配置项 | 默认 | 说明 |
| --- | --- | --- |
| `enabled` | `true` | 插件总开关 |
| `discord_platform_id` | 空 | 多个 Discord 机器人时指定实例 ID，留空自动查找 |
| `ignore_at_all` | `true` | 转发时丢弃 `@全体成员` / `@everyone` / `@here` |
| `suppress_llm` | `true` | 被转发的消息不触发 AstrBot 的默认 AI 回复（指令仍可用） |
| `admin_ids` | `[]` | 额外允许执行管理指令的用户 ID |
| `recovery_platforms` | `[]` | 启动后先积压这些平台的消息，执行 `/relay_recovery` 后再放行 |
| `discord_webhook.enabled` | `true` | 用 Webhook 身份发送，显示 QQ 昵称与头像 |
| `discord_webhook.name` | `DCQQ Relay` | Webhook 名称（自动复用） |
| `discord_webhook.nickname_template` | `{name}` | 昵称模板，可用 `{name}` `{id}` `{platform}` |
| `discord_webhook.show_avatar` | `true` | 是否显示 QQ 头像 |
| `translation.enabled` | `false` | 启用自动翻译 |
| `translation.provider_id` | 空 | 翻译用的模型提供商，留空用默认聊天模型 |
| `translation.to_discord_language` | `English` | QQ → Discord 目标语言，留空则该方向不翻译 |
| `translation.to_qq_language` | `简体中文` | Discord → QQ 目标语言，留空则该方向不翻译 |
| `translation.show_original` | `false` | 译文下方保留原文 |
| `translation.max_chars` | `1500` | 超过长度的文本不翻译 |
| `translation.timeout` | `30` | 单次翻译超时（秒） |
| `advanced.max_file_mb` | `20` | 单个媒体文件大小上限 |
| `advanced.max_discord_chars` | `1900` | Discord 单条消息字符上限 |
| `advanced.delete_sync` | `true` | 同步撤回 / 删除 |
| `advanced.edit_sync` | `true` | 同步 Discord 消息编辑 |
| `advanced.emoji_index_url` | QFace 索引 | QQ 表情名称索引地址 |

---

## 昵称和头像会在 Discord 显示吗？

- **Koishi 原版**：是通过 `<author name avatar>` 元素实现的——Koishi 的 Discord 适配器会把这类消息用 Webhook 发出去，所以昵称和头像天然显示。原版本身不带这个能力，是适配器提供的。
- **AstrBot 内置 Discord 适配器**：没有 author/webhook 模拟能力，消息永远以机器人身份发送，昵称只能写成文本，头像无法显示。
- **本插件**：在 Discord 侧**自己实现 Webhook 身份发送**。默认开启（`discord_webhook.enabled`），效果等同于 Koishi 原版：
  - 昵称 = QQ 群名片 / 昵称（可用 `nickname_template` 定制）
  - 头像 = QQ 头像（`q1.qlogo.cn`，仅 aiocqhttp 等 QQ 平台）
  - 需要机器人在目标频道拥有 **管理 Webhook（Manage Webhooks）** 权限
  - 权限不足或 Webhook 创建失败时，会**自动回退**为机器人身份发送，并把昵称写进消息前缀，不会丢消息

> 注意：Webhook 消息在 Discord 端属于「机器人消息」，AstrBot 的 Discord 适配器默认忽略机器人消息，因此不会造成转发回环。

---

## 指令

| 指令 | 说明 |
| --- | --- |
| `/绑定 [名称]` | 当前会话生成 6 位绑定码（管理员） |
| `/完成绑定 <绑定码>` | 在另一端完成绑定（管理员） |
| `/解绑 <序号>` | 解除一条互通关系（管理员） |
| `/relay` | 查看帮助 |
| `/relay_status` | 查看互通关系、映射数量、翻译与 Webhook 状态 |
| `/relay_recovery <开始时间> [平台ID\|all] [--dry]` | 恢复历史消息（管理员） |

恢复示例：

```
/relay_recovery 2026-11-01 12:00 aiocqhttp
/relay_recovery 2026-11-01 --dry
```

时间支持 `2026-11-01`、`2026-11-01 12:00`、`2026-11-01 12:00:00`、`2026-11-01T12:00`、`11-01 12:00`。恢复过程中新消息会先排队，历史同步完成后再按顺序补发，避免重复。

---

## 工作原理

```
Discord 频道 ──► 适配器事件 ──► 解析昵称/文本/附件/表情/嵌入 ──► 目标平台发送 ──┐
      ▲                                                                        │
      └──── Webhook / channel.send ◄── 解析 QQ 消息链 ◄── OneBot 群消息 ◄──────┘

每条消息的 ID 映射写入 data/plugin_data/astrbot_plugin_dcqq_relay/relay.db
```

- Discord → QQ：`discord.Message` 被解析为 AstrBot 消息链（文本、图片、语音、视频、文件、贴纸、Embed），并加 `昵称 (@用户名):` 前缀
- QQ → Discord：OneBot 消息链被解析为文本 / 图片 / 文件，@ 转成文字，QQ 表情转成 `[微笑]` 等可读名称
- 引用回复：通过映射表转换成对方平台的消息引用
- 撤回：QQ 的 `group_recall` / `friend_recall` 通知、Discord 的 `on_raw_message_delete` 都会触发对侧删除
- 编辑：Discord 的 `on_raw_message_edit` 会删除旧消息并重发（QQ 侧无编辑事件）
- 防回环：自己触发的删除会在一小时内被忽略；机器人自己的消息不会转发

---

## 已知限制与与原版的差异

- **QQ 消息编辑**：OneBot v11 没有消息编辑事件，无法同步 QQ → Discord 的编辑。
- **历史恢复的 QQ 侧**：依赖协议端的 `get_group_msg_history`（NapCat / Lagrange 等支持）。协议端不支持时只同步 Discord 侧历史，并在日志中提示。
- **文件大小**：超过 `advanced.max_file_mb` 的附件会跳过并留下占位文本。
- **QQ 语音**：Discord 只能以文件形式发送（如 `.amr`），不做格式转码。
- **Discord 表情**：自定义表情转成 `[表情:名称]` 文本，不下载成图片。
- **`/relay_recovery` 需要管理员**：AstrBot 管理员或 `admin_ids` 中的用户。
- 原版的 `recovery` / `qqEmojiIndex` / `downloadAsset` / `showQQId` / `reverseName` 均已对应实现。

---

## 常见问题

### QQ 图片在 Discord 显示成 `[图片]`

说明插件没能拿到可下载的图片地址。插件会依次尝试：

1. AstrBot 自带的 `Image.convert_to_base64()`（支持 URL / base64 / 本地路径）
2. 组件里的 `url`、`file`、`path` 等所有候选地址（自动还原 `&amp;` 转义，使用浏览器 UA 下载）
3. 若只有裸文件名，调用协议端的 `get_image` 换取真实链接
4. 仍然失败则发送原始链接（Discord 会自动展开预览），最后才退化成 `[图片]`

排查步骤：

- 在 AstrBot 日志里搜 `[dcqq-relay] 图片下载失败`，会打印失败原因和候选地址
- 确认 AstrBot 所在机器能访问 `gchat.qpic.cn` / `multimedia.nt.qq.com.cn` 等 QQ 图片域名
- 确认协议端（NapCat / Lagrange）版本支持 `get_image`
- Discord → QQ 方向图片发不出去时，打开该关系的 `download_asset`，插件会先下载再用 base64 发送

### 消息发出去了但昵称/头像是机器人

说明 Webhook 不可用（缺少「管理 Webhook」权限，或频道是公告/论坛等特殊类型）。插件会自动回退，并在 `/relay_status` 里显示「已尝试但不可用」。

---

## 许可证

本项目遵循 **Apache License 2.0**，与上游 `koishi-plugin-dcqq-relay` 保持一致。

- 原始项目：https://github.com/koishijs/koishi-plugin-dcqq-relay
- 原项目作者：LTC `<i@ltlec.com>`
- AstrBot 移植：xZhongjie

移植过程中参考了原项目的消息转换、撤回同步、恢复模式等设计，感谢原作者与 Koishi / AstrBot 社区。
