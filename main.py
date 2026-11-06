# -*- coding: utf-8 -*-
"""astrbot_plugin_dcqq_relay — Discord ⇄ QQ 双向消息互通插件。

移植自 koishi-plugin-dcqq-relay（Apache-2.0，作者 LTC <i@ltlec.com>），
在 AstrBot 上重新实现，并额外提供：

* 一条配置即双向的关系模型（不再是"分别单向设置"）
* 忽略 @全体成员 / @everyone / @here 的开关
* 基于 AstrBot 已配置聊天模型的自动翻译（双向可分别设置目标语言）
* Discord Webhook 身份发送，让 QQ 用户的昵称和头像在 Discord 中显示
* 撤回 / 删除同步、Discord 编辑同步、历史消息恢复

设计说明：
* 每条 Relation 描述一个 "Discord 频道 ⇄ QQ 群/私聊"，两个方向共用同一份配置。
* 消息 ID 映射保存在 data/plugin_data/<插件名>/relay.db（SQLite），重启不丢。
* 所有平台能力都通过 AstrBot 的平台适配器实例访问，不依赖 Koishi 的 Session 模型。
"""

from __future__ import annotations

import asyncio
import base64
import datetime as dt
import json
import os
import re
import sqlite3
import time
from dataclasses import dataclass, field
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import aiohttp

import astrbot.api.message_components as Comp
from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.platform import AstrBotMessage, MessageMember
from astrbot.api.star import Context, Star, StarTools

try:
    # GreedyStr 让指令把剩余全部文本当作一个参数，便于解析带空格的时间
    from astrbot.core.star.filter.command import GreedyStr
except Exception:  # pragma: no cover - 兼容旧版本 AstrBot

    class GreedyStr(str):
        """旧版本回退：退化为普通字符串。"""

try:  # Discord 适配器未安装时插件仍然可以加载（只是无法转发到 Discord）
    import discord  # type: ignore
except Exception:  # pragma: no cover - 可选依赖
    discord = None  # type: ignore


PLUGIN_NAME = "astrbot_plugin_dcqq_relay"
RECOVERY_PREFIX = "[RECOVERY]"
SELF_DELETE_TTL = 3600  # 自己触发的删除事件在 1 小时内忽略，避免撤回互相反弹
MAX_TRANSLATE_CACHE = 512


# ---------------------------------------------------------------------------
# 关系模型
# ---------------------------------------------------------------------------


@dataclass
class Relation:
    """一条双向互通关系：Discord 频道 ⇄ 目标平台会话。"""

    name: str
    discord_channel_id: str
    forward_platform: str
    forward_channel: str
    discord_guild_id: str = ""
    forward_message_type: str = "GroupMessage"
    show_forward_id: bool = True
    reverse_name: bool = False
    download_asset: bool = False

    @property
    def key(self) -> str:
        return self.name or f"{self.discord_channel_id}>{self.forward_channel}"

    @property
    def is_group(self) -> bool:
        return self.forward_message_type != "FriendMessage"

    @property
    def description(self) -> str:
        return (
            f"{self.name or self.key}: "
            f"discord:{self.discord_channel_id} ⇄ "
            f"{self.forward_platform}:{self.forward_channel}"
        )


def parse_relations(raw: Any) -> list[Relation]:
    """把 _conf_schema.json 里的 template_list 解析成 Relation 列表。"""
    relations: list[Relation] = []
    if not isinstance(raw, list):
        return relations

    for index, item in enumerate(raw):
        if not isinstance(item, dict):
            continue
        discord_channel = str(item.get("discord_channel_id") or "").strip()
        forward_channel = str(item.get("forward_channel") or "").strip()
        forward_platform = str(item.get("forward_platform") or "").strip()
        if not (discord_channel and forward_channel and forward_platform):
            logger.warning(
                "[dcqq-relay] 忽略第 %s 条关系：discord_channel_id / forward_platform / forward_channel 不能为空",
                index + 1,
            )
            continue

        relations.append(
            Relation(
                name=str(item.get("name") or f"relation-{index + 1}").strip(),
                discord_channel_id=discord_channel,
                forward_platform=forward_platform,
                forward_channel=forward_channel,
                discord_guild_id=str(item.get("discord_guild_id") or "").strip(),
                forward_message_type=(
                    str(item.get("forward_message_type") or "GroupMessage").strip()
                    or "GroupMessage"
                ),
                show_forward_id=bool(item.get("show_forward_id", True)),
                reverse_name=bool(item.get("reverse_name", False)),
                download_asset=bool(item.get("download_asset", False)),
            )
        )

    return relations


# ---------------------------------------------------------------------------
# 消息映射存储
# ---------------------------------------------------------------------------


@dataclass
class RelayRow:
    """一条消息映射：Discord 消息 ⇄ 目标平台消息。"""

    id: int
    relation: str
    dc_id: str
    dc_channel: str
    forward_platform: str
    forward_channel: str
    forward_id: str
    deleted: int
    created_at: float

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "RelayRow":
        return cls(
            id=int(row["id"]),
            relation=str(row["relation"] or ""),
            dc_id=str(row["dc_id"] or ""),
            dc_channel=str(row["dc_channel"] or ""),
            forward_platform=str(row["forward_platform"] or ""),
            forward_channel=str(row["forward_channel"] or ""),
            forward_id=str(row["forward_id"] or ""),
            deleted=int(row["deleted"] or 0),
            created_at=float(row["created_at"] or 0),
        )


class RelayStore:
    """轻量 SQLite 存储，所有磁盘操作都放到线程池，避免阻塞事件循环。"""

    def __init__(self, db_path: Path) -> None:
        self._db_path = Path(db_path)
        self._conn: sqlite3.Connection | None = None
        self._lock = asyncio.Lock()

    async def init(self) -> None:
        await asyncio.to_thread(self._init_sync)

    def _init_sync(self) -> None:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL")
        except sqlite3.DatabaseError:
            pass
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS relay_map (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                relation TEXT NOT NULL DEFAULT '',
                dc_id TEXT NOT NULL DEFAULT '',
                dc_channel TEXT NOT NULL DEFAULT '',
                forward_platform TEXT NOT NULL DEFAULT '',
                forward_channel TEXT NOT NULL DEFAULT '',
                forward_id TEXT NOT NULL DEFAULT '',
                deleted INTEGER NOT NULL DEFAULT 0,
                created_at REAL NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_relay_dc ON relay_map (dc_id)")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_relay_forward "
            "ON relay_map (forward_platform, forward_channel, forward_id)"
        )
        conn.commit()
        self._conn = conn

    async def close(self) -> None:
        if self._conn is None:
            return
        conn = self._conn
        self._conn = None
        await asyncio.to_thread(conn.close)

    async def _run(self, func):
        async with self._lock:
            if self._conn is None:
                raise RuntimeError("RelayStore 尚未初始化")
            return await asyncio.to_thread(func)

    # -- 写入 ---------------------------------------------------------------
    async def add(
        self,
        *,
        relation: str,
        dc_id: str,
        dc_channel: str,
        forward_platform: str,
        forward_channel: str,
        forward_id: str,
    ) -> int:
        def _do() -> int:
            cur = self._conn.execute(
                "INSERT INTO relay_map "
                "(relation, dc_id, dc_channel, forward_platform, forward_channel, "
                " forward_id, deleted, created_at) VALUES (?, ?, ?, ?, ?, ?, 0, ?)",
                (
                    relation,
                    dc_id,
                    dc_channel,
                    forward_platform,
                    forward_channel,
                    forward_id,
                    time.time(),
                ),
            )
            self._conn.commit()
            return int(cur.lastrowid or 0)

        return await self._run(_do)

    async def mark_deleted(self, row_ids: list[int]) -> None:
        if not row_ids:
            return

        def _do() -> None:
            placeholders = ",".join("?" for _ in row_ids)
            self._conn.execute(
                f"UPDATE relay_map SET deleted = 1 WHERE id IN ({placeholders})",
                row_ids,
            )
            self._conn.commit()

        await self._run(_do)

    async def update_forward_id(self, row_id: int, forward_id: str) -> None:
        def _do() -> None:
            self._conn.execute(
                "UPDATE relay_map SET forward_id = ? WHERE id = ?", (forward_id, row_id)
            )
            self._conn.commit()

        await self._run(_do)

    # -- 查询 ---------------------------------------------------------------
    async def by_dc(
        self, dc_id: str, *, include_deleted: bool = False
    ) -> list[RelayRow]:
        def _do() -> list[RelayRow]:
            sql = "SELECT * FROM relay_map WHERE dc_id = ?"
            if not include_deleted:
                sql += " AND deleted = 0"
            rows = self._conn.execute(sql, (dc_id,)).fetchall()
            return [RelayRow.from_row(row) for row in rows]

        return await self._run(_do)

    async def by_forward(
        self,
        platform: str,
        channel: str,
        forward_id: str,
        *,
        include_deleted: bool = False,
    ) -> list[RelayRow]:
        def _do() -> list[RelayRow]:
            sql = (
                "SELECT * FROM relay_map "
                "WHERE forward_platform = ? AND forward_channel = ? AND forward_id = ?"
            )
            if not include_deleted:
                sql += " AND deleted = 0"
            rows = self._conn.execute(sql, (platform, channel, forward_id)).fetchall()
            return [RelayRow.from_row(row) for row in rows]

        return await self._run(_do)

    async def exists_dc(self, dc_id: str) -> bool:
        return bool(await self.by_dc(dc_id))

    async def exists_forward(self, platform: str, channel: str, forward_id: str) -> bool:
        return bool(await self.by_forward(platform, channel, forward_id))

    async def count(self) -> int:
        def _do() -> int:
            row = self._conn.execute(
                "SELECT COUNT(*) AS c FROM relay_map WHERE deleted = 0"
            ).fetchone()
            return int(row["c"] if row else 0)

        return await self._run(_do)


# ---------------------------------------------------------------------------
# 通用工具
# ---------------------------------------------------------------------------


def uri_to_path(uri: str) -> str:
    """把 file:// URI 还原成本地路径（Windows 盘符也正确处理）。"""
    if not uri:
        return ""
    if uri.startswith("file://"):
        parsed = urlparse(uri)
        path = unquote(parsed.path)
        if os.name == "nt" and len(path) > 2 and path[0] == "/" and path[2] == ":":
            path = path[1:]
        return path
    return uri


def guess_filename(url: str, default: str = "file") -> str:
    """从 URL 猜测一个安全的文件名。"""
    try:
        name = os.path.basename(unquote(urlparse(url).path)) if url else ""
    except Exception:
        name = ""
    name = name.strip() or default
    return re.sub(r"[^0-9A-Za-z._\-\u4e00-\u9fff]", "_", name)[:100]


def sanitize_webhook_username(name: str) -> str:
    """Discord 不允许 webhook 昵称包含 clyde/discord 等保留词。"""
    name = (name or "").strip() or "QQ User"
    for word in ("clyde", "discord"):
        if word in name.lower():
            name = re.sub(word, word.capitalize() + "_", name, flags=re.IGNORECASE)
    return name[:80]


class HttpClient:
    """复用 aiohttp session 的小封装，带体积上限。"""

    def __init__(self, max_bytes: int) -> None:
        self.max_bytes = max_bytes
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=90)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def get_bytes(self, url: str) -> bytes | None:
        if not url:
            return None
        if url.startswith("base64://"):
            try:
                data = base64.b64decode(url[len("base64://") :])
            except Exception:
                return None
            return data if len(data) <= self.max_bytes else None
        if url.startswith("file://"):
            path = Path(uri_to_path(url))
            if path.is_file() and path.stat().st_size <= self.max_bytes:
                return await asyncio.to_thread(path.read_bytes)
            return None
        if not url.startswith(("http://", "https://")):
            return None

        session = await self._get_session()
        try:
            async with session.get(url) as resp:
                resp.raise_for_status()
                if resp.content_length and resp.content_length > self.max_bytes:
                    logger.warning(
                        "[dcqq-relay] 跳过超大文件（%s 字节）：%s",
                        resp.content_length,
                        url,
                    )
                    return None
                data = await resp.content.read(self.max_bytes + 1)
                if len(data) > self.max_bytes:
                    return None
                return data
        except Exception as exc:
            logger.warning("[dcqq-relay] 下载资源失败：%s（%s）", url, exc)
            return None

    async def get_json(self, url: str) -> Any:
        session = await self._get_session()
        try:
            async with session.get(url) as resp:
                resp.raise_for_status()
                return await resp.json(content_type=None)
        except Exception as exc:
            logger.warning("[dcqq-relay] 拉取 %s 失败：%s", url, exc)
            return None

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None


# ---------------------------------------------------------------------------
# QQ 表情索引
# ---------------------------------------------------------------------------


class QQEmojiIndex:
    """把 QQ 的 face id 转成可读文字。

    优先使用 QFace 的在线索引（可配置），失败时退回内置常用表情表。
    """

    FALLBACK: dict[int, str] = {
        0: "微笑", 1: "撇嘴", 2: "色", 3: "发呆", 4: "得意", 5: "流泪",
        6: "害羞", 7: "闭嘴", 8: "睡", 9: "大哭", 10: "尴尬", 11: "发怒",
        12: "调皮", 13: "呲牙", 14: "惊讶", 15: "难过", 16: "酷", 17: "冷汗",
        18: "抓狂", 19: "吐", 20: "偷笑", 21: "愉快", 22: "白眼", 23: "傲慢",
        24: "饥饿", 25: "困", 26: "惊恐", 27: "流汗", 28: "憨笑", 29: "大兵",
        30: "奋斗", 31: "咒骂", 32: "疑问", 33: "嘘", 34: "晕", 35: "折磨",
        36: "衰", 37: "骷髅", 38: "敲打", 39: "再见", 40: "擦汗", 41: "抠鼻",
        42: "鼓掌", 43: "糗大了", 44: "坏笑", 45: "左哼哼", 46: "右哼哼",
        47: "哈欠", 48: "鄙视", 49: "委屈", 50: "快哭了", 51: "阴险",
        52: "亲亲", 53: "吓", 54: "可怜", 55: "菜刀", 56: "西瓜", 57: "啤酒",
        58: "篮球", 59: "乒乓", 60: "咖啡", 61: "饭", 62: "猪头", 63: "玫瑰",
        64: "凋谢", 65: "示爱", 66: "爱心", 67: "心碎", 68: "蛋糕", 69: "闪电",
        70: "炸弹", 71: "刀", 72: "足球", 73: "瓢虫", 74: "便便", 75: "月亮",
        76: "太阳", 77: "礼物", 78: "拥抱", 79: "强", 80: "弱", 81: "握手",
        82: "胜利", 83: "抱拳", 84: "勾引", 85: "拳头", 86: "差劲", 87: "爱你",
        88: "NO", 89: "OK", 90: "爱情", 91: "飞吻", 92: "跳跳", 93: "发抖",
        94: "怄火", 95: "转圈", 96: "磕头", 97: "回头", 98: "跳绳", 99: "挥手",
        100: "激动", 101: "街舞", 102: "献吻", 103: "左太极", 104: "右太极",
        105: "双喜", 106: "鞭炮", 107: "灯笼", 108: "K歌", 109: "购物",
        110: "邮件", 111: "帅", 112: "喝彩", 113: "祈祷", 114: "爆筋",
        115: "棒棒糖", 116: "喝奶", 117: "下面", 118: "香蕉", 119: "飞机",
        120: "开车", 121: "高铁左车头", 122: "车厢", 123: "高铁右车头",
        124: "多云", 125: "下雨", 126: "钞票", 127: "熊猫", 128: "灯泡",
        129: "风车", 130: "闹钟", 131: "打伞", 132: "彩球", 133: "钻戒",
        134: "沙发", 135: "纸巾", 136: "药", 137: "手枪", 138: "青蛙",
        139: "茶", 140: "眨眼睛", 141: "泪奔", 142: "无奈", 143: "卖萌",
        144: "小纠结", 145: "喷血", 146: "斜眼笑", 147: "doge", 148: "惊喜",
        149: "骚扰", 150: "笑哭", 151: "我最美", 152: "河蟹", 153: "羊驼",
        154: "栗子", 155: "幽灵", 156: "蛋", 157: "马赛克", 158: "菊花",
        159: "梅开三度", 160: "梅开二度", 161: "梅开一度", 162: "猪头",
        163: "狗头", 164: "喵喵", 165: "泡面", 166: "摸头", 167: "支持",
        168: "比心", 169: "庆祝", 170: "红包", 171: "发红包", 172: "收红包",
        173: "开会", 174: "暗中观察", 175: "忙", 176: "摸鱼", 177: "记录",
        178: "哦", 179: "偷看", 180: "旺柴", 181: "哇", 182: "哇哦",
        183: "裂开", 184: "叹气", 185: "六六六", 186: "666", 187: "赞",
        188: "鼓掌", 189: "握手", 190: "抱拳", 191: "胜利", 192: "拳头",
        193: "打脸", 194: "加油", 195: "汗", 196: "天啊", 197: "Emm",
        198: "社会社会", 199: "旺柴", 200: "打call", 201: "收到", 202: "OK",
        203: "在吗", 204: "嗯哼", 205: "晚安", 206: "早", 207: "爱你",
        208: "偷笑", 209: "憨笑", 210: "嘿哈", 211: "捂脸", 212: "奸笑",
        213: "机智", 214: "皱眉", 215: "耶", 216: "吃瓜", 217: "加油",
        218: "汗", 219: "天啊", 220: "Emm", 221: "社会社会", 222: "旺柴",
    }

    def __init__(self, url: str, cache_file: Path, http: "HttpClient") -> None:
        self._url = url
        self._cache_file = Path(cache_file)
        self._http = http
        self._emojis: dict[str, str] = {}

    async def load(self) -> None:
        # 1) 先读 7 天内的本地缓存
        try:
            if self._cache_file.is_file():
                raw = json.loads(self._cache_file.read_text(encoding="utf-8"))
                if time.time() - float(raw.get("ts", 0)) < 7 * 86400:
                    self._apply(raw.get("data") or {})
                    if self._emojis:
                        logger.info(
                            "[dcqq-relay] 已从缓存加载 %d 个 QQ 表情", len(self._emojis)
                        )
                        return
        except Exception:
            pass

        # 2) 在线拉取
        if not self._url:
            return
        try:
            data = await asyncio.wait_for(self._http.get_json(self._url), timeout=30)
        except Exception:
            data = None
        if not data:
            logger.warning("[dcqq-relay] 表情索引不可用，使用内置表情表")
            return
        self._apply(data)
        try:
            self._cache_file.parent.mkdir(parents=True, exist_ok=True)
            self._cache_file.write_text(
                json.dumps({"ts": time.time(), "data": data}, ensure_ascii=False),
                encoding="utf-8",
            )
        except Exception:
            pass
        logger.info("[dcqq-relay] 已加载 %d 个 QQ 表情", len(self._emojis))

    def _apply(self, data: Any) -> None:
        if not isinstance(data, dict):
            return
        emojis = data.get("emojis", data)
        if not isinstance(emojis, dict):
            return
        parsed: dict[str, str] = {}
        for key, value in emojis.items():
            describe = ""
            if isinstance(value, dict):
                describe = str(value.get("describe") or value.get("name") or "")
            elif isinstance(value, str):
                describe = value
            describe = describe.strip().lstrip("/[")
            if describe:
                parsed[str(key)] = describe
        if parsed:
            self._emojis = parsed

    def describe(self, face_id: Any) -> str:
        key = str(face_id)
        if key in self._emojis:
            return self._emojis[key]
        try:
            number = int(face_id)
        except (TypeError, ValueError):
            number = -1
        return self.FALLBACK.get(number) or f"表情:{face_id}"


# ---------------------------------------------------------------------------
# 翻译
# ---------------------------------------------------------------------------


class Translator:
    """使用 AstrBot 已配置的聊天模型做翻译，带缓存与超时。"""

    def __init__(self, plugin: "DCQQRelayPlugin") -> None:
        self.plugin = plugin
        self._cache: dict[tuple[str, str], str] = {}

    # -- 配置读取 -----------------------------------------------------------
    @property
    def _cfg(self) -> dict:
        cfg = self.plugin.config.get("translation") or {}
        return cfg if isinstance(cfg, dict) else {}

    def target_language(self, direction: str) -> str:
        if not self._cfg.get("enabled"):
            return ""
        key = "to_qq_language" if direction == "dc2qq" else "to_discord_language"
        return str(self._cfg.get(key) or "").strip()

    def _provider(self, umo: str = ""):
        provider_id = str(self._cfg.get("provider_id") or "").strip()
        if provider_id:
            try:
                provider = self.plugin.context.get_provider_by_id(provider_id)
                if provider is not None:
                    return provider
            except Exception:
                logger.warning(
                    "[dcqq-relay] 找不到翻译模型 %s，回退到默认模型", provider_id
                )
        try:
            return self.plugin.context.get_using_provider(umo or None)
        except Exception:
            return None

    # -- 翻译 ---------------------------------------------------------------
    async def translate(self, text: str, direction: str, umo: str = "") -> str | None:
        text = (text or "").strip()
        if not text:
            return None
        language = self.target_language(direction)
        if not language:
            return None
        max_chars = int(self._cfg.get("max_chars") or 1500)
        if len(text) > max_chars:
            return None
        # 纯数字 / emoji / 链接等没有翻译价值
        if not re.search(r"[A-Za-z\u4e00-\u9fff]", text):
            return None
        if len(text) < 2:
            return None

        cache_key = (language, text)
        if cache_key in self._cache:
            return self._cache[cache_key]

        provider = self._provider(umo)
        if provider is None:
            logger.warning("[dcqq-relay] 没有可用的聊天模型，跳过翻译")
            return None

        system_prompt = (
            f"你是一个精准的即时通讯翻译器。请把用户给出的消息翻译成{language}。"
            "要求：只输出译文本身，不要解释、不要加引号或前缀；"
            "保留原文的语气、换行、@提及、链接、表情和代码内容；"
            f"如果原文已经主要是{language}，原样输出。"
        )
        timeout = int(self._cfg.get("timeout") or 30)
        try:
            response = await asyncio.wait_for(
                provider.text_chat(
                    prompt=text,
                    system_prompt=system_prompt,
                    contexts=[],
                    tool_choice="auto",
                ),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            logger.warning("[dcqq-relay] 翻译超时（%ss），本条消息使用原文", timeout)
            return None
        except Exception as exc:
            logger.warning("[dcqq-relay] 翻译失败：%s", exc)
            return None

        result = (getattr(response, "completion_text", "") or "").strip()
        result = result.strip().strip('"').strip("“”").strip()
        if not result or result == text:
            return None

        if len(self._cache) >= MAX_TRANSLATE_CACHE:
            # 简单淘汰：清掉最早的一半
            for key in list(self._cache.keys())[: MAX_TRANSLATE_CACHE // 2]:
                self._cache.pop(key, None)
        self._cache[cache_key] = result
        return result


# ---------------------------------------------------------------------------
# Discord 侧
# ---------------------------------------------------------------------------


@dataclass
class _DiscordPayload:
    content: str = ""
    files: list[Any] = field(default_factory=list)
    reference_id: str | None = None


class DiscordSide:
    """封装 Discord 侧的发送 / 删除 / 历史读取，并负责 Webhook 身份伪装。"""

    def __init__(self, plugin: "DCQQRelayPlugin") -> None:
        self.plugin = plugin
        self._channels: dict[int, Any] = {}
        self._webhooks: dict[int, Any] = {}
        self._webhook_unavailable = False

    # -- 基础访问 -----------------------------------------------------------
    @property
    def platform(self):
        return self.plugin.discord_platform()

    @property
    def client(self):
        inst = self.platform
        return getattr(inst, "client", None) if inst is not None else None

    @property
    def available(self) -> bool:
        return discord is not None and self.client is not None

    async def get_channel(self, channel_id: Any):
        client = self.client
        if client is None:
            return None
        try:
            cid = int(str(channel_id))
        except (TypeError, ValueError):
            return None
        channel = self._channels.get(cid) or client.get_channel(cid)
        if channel is None:
            try:
                channel = await client.fetch_channel(cid)
            except Exception as exc:
                logger.warning("[dcqq-relay] 找不到 Discord 频道 %s：%s", cid, exc)
                return None
        self._channels[cid] = channel
        return channel

    async def history(self, channel_id: Any, after: dt.datetime) -> list[Any]:
        channel = await self.get_channel(channel_id)
        if channel is None:
            return []
        messages: list[Any] = []
        try:
            async for message in channel.history(
                limit=None, after=after, oldest_first=True
            ):
                messages.append(message)
        except Exception as exc:
            logger.warning("[dcqq-relay] 读取 Discord 历史消息失败：%s", exc)
        return messages

    # -- 发送 ---------------------------------------------------------------
    async def send(
        self,
        relation: Relation,
        chain: list[Any],
        *,
        username: str,
        avatar_url: str | None = None,
        fallback_prefix: str = "",
        reference_id: str | None = None,
    ) -> str | None:
        if discord is None:
            logger.error("[dcqq-relay] 当前环境未安装 discord.py，无法发送到 Discord")
            return None

        channel = await self.get_channel(relation.discord_channel_id)
        if channel is None:
            return None

        payload = await self._build_payload(chain)
        if payload is None:
            return None

        max_chars = int(
            (self.plugin.config.get("advanced") or {}).get("max_discord_chars") or 1900
        )
        plain_content = payload.content
        with_prefix = (fallback_prefix + plain_content).strip()
        if len(with_prefix) > max_chars:
            with_prefix = with_prefix[:max_chars].rstrip() + "\n…（消息过长已截断）"
        if len(plain_content) > max_chars:
            plain_content = plain_content[:max_chars].rstrip() + "\n…（消息过长已截断）"

        webhook = await self._get_webhook(channel)
        content = plain_content if webhook is not None else with_prefix
        if not content and not payload.files:
            return None

        kwargs: dict[str, Any] = {"allowed_mentions": discord.AllowedMentions.none()}
        if content:
            kwargs["content"] = content
        if payload.files:
            kwargs["files"] = payload.files[:10]

        ref = reference_id or payload.reference_id
        if ref:
            try:
                kwargs["reference"] = discord.PartialMessage(
                    channel=channel, id=int(str(ref))
                )
            except (TypeError, ValueError):
                pass

        if webhook is not None:
            kwargs["username"] = sanitize_webhook_username(username)
            show_avatar = bool(
                (self.plugin.config.get("discord_webhook") or {}).get(
                    "show_avatar", True
                )
            )
            if avatar_url and show_avatar:
                kwargs["avatar_url"] = avatar_url
            try:
                message = await webhook.send(wait=True, **kwargs)
                return str(getattr(message, "id", "")) or None
            except Exception as exc:
                logger.warning(
                    "[dcqq-relay] Webhook 发送失败，回退为机器人身份：%s", exc
                )
                self._webhooks.pop(getattr(channel, "id", 0), None)
                self._webhook_unavailable = True
                kwargs.pop("username", None)
                kwargs.pop("avatar_url", None)
                kwargs["content"] = with_prefix
                if not with_prefix and not payload.files:
                    return None

        try:
            message = await channel.send(**kwargs)
            return str(getattr(message, "id", "")) or None
        except Exception as exc:
            logger.error("[dcqq-relay] 发送消息到 Discord 失败：%s", exc)
            return None

    async def delete(self, channel_id: Any, message_id: Any) -> bool:
        channel = await self.get_channel(channel_id)
        if channel is None:
            return False
        try:
            mid = int(str(message_id))
        except (TypeError, ValueError):
            return False
        try:
            await channel.get_partial_message(mid).delete()
            return True
        except Exception as exc:
            logger.info("[dcqq-relay] 删除 Discord 消息 %s 失败：%s", message_id, exc)
            return False

    # -- 内部 ---------------------------------------------------------------
    async def _build_payload(self, chain: list[Any]) -> _DiscordPayload | None:
        if discord is None:
            return None
        payload = _DiscordPayload()
        parts: list[str] = []
        for seg in chain:
            if isinstance(seg, Comp.Plain):
                if seg.text:
                    parts.append(seg.text)
            elif isinstance(seg, Comp.Reply):
                payload.reference_id = str(seg.id)
            elif isinstance(seg, Comp.Image):
                ref = seg.url or seg.file or ""
                if not await self._add_media(
                    ref, self._media_name(ref, "image.png"), parts, payload.files
                ):
                    parts.append("[图片]")
            elif isinstance(seg, Comp.Record):
                ref = seg.url or seg.file or ""
                if not await self._add_media(
                    ref, self._media_name(ref, "voice.wav"), parts, payload.files
                ):
                    parts.append("[语音]")
            elif isinstance(seg, Comp.Video):
                ref = seg.url or seg.file or ""
                if not await self._add_media(
                    ref, self._media_name(ref, "video.mp4"), parts, payload.files
                ):
                    parts.append("[视频]")
            elif isinstance(seg, Comp.File):
                ref = seg.url or getattr(seg, "file_", "") or ""
                if not await self._add_media(
                    ref,
                    seg.name or self._media_name(ref, "file.bin"),
                    parts,
                    payload.files,
                ):
                    parts.append(f"[文件: {seg.name or '未知'}]")
        payload.content = "".join(parts).strip()
        if not payload.content and not payload.files:
            return None
        return payload

    @staticmethod
    def _media_name(url: str, default: str) -> str:
        if not url:
            return default
        name = guess_filename(url, default)
        if "." not in os.path.basename(name):
            return default
        return name

    async def _add_media(
        self, url: str, filename: str, parts: list[str], files: list[Any]
    ) -> bool:
        if not url:
            return False
        if len(files) >= 10:
            parts.append("\n[附件数量超过 Discord 单条上限，已省略]")
            return False
        data = await self.plugin.http.get_bytes(url)
        if data is None:
            if url.startswith("http"):
                parts.append(f"\n[附件]({url})")
            return False
        files.append(discord.File(BytesIO(data), filename=filename))
        return True

    async def _get_webhook(self, channel):
        if discord is None or self._webhook_unavailable:
            return None
        cfg = self.plugin.config.get("discord_webhook") or {}
        if not cfg.get("enabled", True):
            return None
        cid = int(getattr(channel, "id", 0))
        if cid in self._webhooks:
            return self._webhooks[cid]
        name = str(cfg.get("name") or "DCQQ Relay")
        try:
            hooks = await channel.webhooks()
            webhook = next(
                (h for h in hooks if h.name == name and getattr(h, "token", None)), None
            )
            if webhook is None:
                webhook = await channel.create_webhook(name=name)
            self._webhooks[cid] = webhook
            logger.info(
                "[dcqq-relay] 已启用 Discord Webhook「%s」（频道 %s）", name, cid
            )
            return webhook
        except Exception as exc:
            logger.warning(
                "[dcqq-relay] 无法使用 Webhook（需要「管理 Webhook」权限），"
                "改用机器人身份发送：%s",
                exc,
            )
            self._webhook_unavailable = True
            return None


# ---------------------------------------------------------------------------
# QQ / 目标平台侧
# ---------------------------------------------------------------------------


class QQSender:
    """封装目标平台（默认 OneBot/aiocqhttp）的发送、删除与历史读取。"""

    def __init__(self, plugin: "DCQQRelayPlugin") -> None:
        self.plugin = plugin

    # -- 平台实例 -----------------------------------------------------------
    def platform(self, platform_id: str):
        manager = getattr(self.plugin.context, "platform_manager", None)
        instances = manager.get_insts() if manager is not None else []
        for inst in instances:
            try:
                meta = inst.meta()
            except Exception:
                continue
            if meta.id == platform_id:
                return inst
        for inst in instances:
            try:
                meta = inst.meta()
            except Exception:
                continue
            if meta.name == platform_id:
                return inst
        return None

    def client(self, platform_id: str):
        inst = self.platform(platform_id)
        if inst is None:
            return None
        bot = getattr(inst, "bot", None)
        if bot is not None:
            return bot
        getter = getattr(inst, "get_client", None)
        if callable(getter):
            try:
                return getter()
            except Exception:
                return None
        return None

    @staticmethod
    def _umo(relation: Relation) -> str:
        message_type = "GroupMessage" if relation.is_group else "FriendMessage"
        return f"{relation.forward_platform}:{message_type}:{relation.forward_channel}"

    # -- 发送 ---------------------------------------------------------------
    async def send(self, relation: Relation, chain: list[Any]) -> list[str]:
        segments = [seg for seg in (self._segment(c) for c in chain) if seg]
        if not segments:
            return []

        channel = str(relation.forward_channel)
        client = self.client(relation.forward_platform)
        is_numeric = channel.lstrip("-").isdigit()

        if client is None or not is_numeric:
            try:
                ok = await self.plugin.context.send_message(
                    self._umo(relation), MessageChain(chain)
                )
                if not ok:
                    logger.warning(
                        "[dcqq-relay] AstrBot 找不到平台 %s 的会话 %s",
                        relation.forward_platform,
                        relation.forward_channel,
                    )
            except Exception as exc:
                logger.error("[dcqq-relay] 发送消息到目标平台失败：%s", exc)
            return []

        try:
            if relation.is_group:
                ret = await client.call_action(
                    "send_group_msg", group_id=int(channel), message=segments
                )
            else:
                ret = await client.call_action(
                    "send_private_msg", user_id=int(channel), message=segments
                )
        except Exception as exc:
            logger.error("[dcqq-relay] 发送消息到 QQ 失败：%s", exc)
            return []

        if isinstance(ret, dict) and ret.get("message_id") not in (None, ""):
            return [str(ret["message_id"])]
        return []

    @staticmethod
    def _segment(seg: Any) -> dict | None:
        if isinstance(seg, Comp.Plain):
            return {"type": "text", "data": {"text": seg.text}} if seg.text else None
        if isinstance(seg, Comp.At):
            return {"type": "at", "data": {"qq": str(seg.qq)}}
        if isinstance(seg, Comp.Image):
            return {"type": "image", "data": {"file": seg.url or seg.file or ""}}
        if isinstance(seg, Comp.Record):
            return {"type": "record", "data": {"file": seg.url or seg.file or ""}}
        if isinstance(seg, Comp.Video):
            return {"type": "video", "data": {"file": seg.url or seg.file or ""}}
        if isinstance(seg, Comp.File):
            return {
                "type": "file",
                "data": {
                    "file": seg.url or getattr(seg, "file_", "") or "",
                    "name": seg.name or "",
                },
            }
        if isinstance(seg, Comp.Reply):
            return {"type": "reply", "data": {"id": str(seg.id)}}
        if isinstance(seg, Comp.Face):
            return {"type": "face", "data": {"id": str(seg.id)}}
        return None

    # -- 删除 ---------------------------------------------------------------
    async def delete(self, platform_id: str, message_id: Any) -> bool:
        client = self.client(platform_id)
        if client is None:
            return False
        try:
            await client.call_action("delete_msg", message_id=int(str(message_id)))
            return True
        except Exception as exc:
            logger.info(
                "[dcqq-relay] 删除 QQ 消息 %s 失败（可能已超过撤回时限）：%s",
                message_id,
                exc,
            )
            return False

    # -- 历史 ---------------------------------------------------------------
    async def history(
        self, relation: Relation, after_ts: float, max_pages: int = 8
    ) -> list[tuple[Any, float]]:
        """读取 QQ 群历史消息。

        依赖 OneBot v11 的 get_group_msg_history（NapCat / Lagrange 等支持）。
        不支持时返回空列表，恢复流程会只同步 Discord 侧历史。
        """
        adapter = self.platform(relation.forward_platform)
        client = self.client(relation.forward_platform)
        if adapter is None or client is None or not relation.is_group:
            return []
        if not str(relation.forward_channel).lstrip("-").isdigit():
            return []

        results: list[tuple[Any, float]] = []
        seq: Any = None
        warned = False
        for _ in range(max_pages):
            kwargs: dict[str, Any] = {
                "group_id": int(relation.forward_channel),
                "count": 50,
            }
            if seq is not None:
                kwargs["message_seq"] = seq
            try:
                ret = await client.call_action("get_group_msg_history", **kwargs)
            except Exception as exc:
                if not warned:
                    logger.warning(
                        "[dcqq-relay] 协议端不支持 get_group_msg_history 或调用失败，"
                        "本次恢复仅同步 Discord 侧：%s",
                        exc,
                    )
                    warned = True
                break

            batch = self._history_batch(ret)
            if not batch:
                break

            stop = False
            for raw in batch:
                timestamp = float(raw.get("time") or 0)
                if timestamp < after_ts:
                    stop = True
                    continue
                abm = await self._convert_history(adapter, relation, raw)
                if abm is not None:
                    results.append((abm, timestamp))
            if stop:
                break
            seq = (
                batch[0].get("message_seq")
                or batch[0].get("real_id")
                or batch[0].get("message_id")
            )
            if seq is None:
                break
            await asyncio.sleep(0.3)

        results.sort(key=lambda item: item[1])
        return results

    @staticmethod
    def _history_batch(ret: Any) -> list[dict]:
        if isinstance(ret, dict):
            for key in ("messages", "data"):
                value = ret.get(key)
                if isinstance(value, list):
                    return [item for item in value if isinstance(item, dict)]
        if isinstance(ret, list):
            return [item for item in ret if isinstance(item, dict)]
        return []

    async def _convert_history(self, adapter, relation: Relation, raw: dict):
        payload = dict(raw)
        payload.setdefault("post_type", "message")
        payload.setdefault("message_type", "group")
        payload.setdefault(
            "group_id",
            int(relation.forward_channel)
            if str(relation.forward_channel).isdigit()
            else relation.forward_channel,
        )
        payload.setdefault("self_id", payload.get("self_id") or 0)
        if "sender" not in payload and "user_id" in payload:
            payload["sender"] = {
                "user_id": payload["user_id"],
                "nickname": payload.get("nickname") or str(payload["user_id"]),
            }
        try:
            from aiocqhttp import Event

            event = Event.from_payload(payload)
            abm = await adapter.convert_message(event)
            if abm is not None:
                return abm
        except Exception as exc:
            logger.debug("[dcqq-relay] 历史消息转换失败，回退为纯文本：%s", exc)
        return self._fallback_abm(raw, relation)

    def _fallback_abm(self, raw: dict, relation: Relation) -> Any:
        abm = AstrBotMessage()
        abm.self_id = str(raw.get("self_id") or "0")
        abm.message_id = str(raw.get("message_id") or "")
        abm.session_id = str(raw.get("group_id") or relation.forward_channel)
        abm.group_id = abm.session_id
        sender = raw.get("sender") or {}
        uid = raw.get("user_id") or sender.get("user_id") or ""
        nickname = sender.get("card") or sender.get("nickname") or str(uid)
        abm.sender = MessageMember(user_id=str(uid), nickname=str(nickname))
        text = self._raw_text(raw)
        abm.message = [Comp.Plain(text)] if text else []
        abm.message_str = text
        abm.raw_message = raw
        abm.timestamp = int(float(raw.get("time") or 0))
        return abm

    @staticmethod
    def _raw_text(raw: dict) -> str:
        message = raw.get("message")
        if isinstance(message, str):
            return message
        parts: list[str] = []
        for seg in message or []:
            if not isinstance(seg, dict):
                continue
            data = seg.get("data") or {}
            seg_type = seg.get("type")
            if seg_type == "text":
                parts.append(str(data.get("text") or ""))
            elif seg_type == "at":
                parts.append(f"@{data.get('qq')}")
            elif seg_type == "face":
                parts.append("[表情]")
        return "".join(parts)


# ---------------------------------------------------------------------------
# 插件主体
# ---------------------------------------------------------------------------


class DCQQRelayPlugin(Star):
    """Discord ⇄ QQ 双向消息互通。"""

    def __init__(self, context: Context, config: AstrBotConfig) -> None:
        super().__init__(context)
        self.config = config
        self.relations: list[Relation] = parse_relations(config.get("relations"))
        self.store: RelayStore | None = None
        self.http: HttpClient | None = None
        self.emoji: QQEmojiIndex | None = None
        self.translator: Translator | None = None
        self.dc: DiscordSide | None = None
        self.qq: QQSender | None = None

        self._data_dir: Path | None = None
        self._initialized = False
        self._self_deleted: dict[str, float] = {}
        self._recovery_ready: dict[str, asyncio.Event] = {}
        self._recovering = False
        self._listener_client_id: int | None = None
        self._attach_running = False

    # -- 生命周期 -----------------------------------------------------------
    async def initialize(self) -> None:
        self._data_dir = StarTools.get_data_dir(PLUGIN_NAME)
        max_mb = int((self.config.get("advanced") or {}).get("max_file_mb") or 20)
        self.http = HttpClient(max_mb * 1024 * 1024)
        self.store = RelayStore(self._data_dir / "relay.db")
        await self.store.init()

        emoji_url = str(
            (self.config.get("advanced") or {}).get("emoji_index_url")
            or "https://koishi.js.org/QFace/assets/qq_emoji/_index.v2.json"
        )
        self.emoji = QQEmojiIndex(
            emoji_url, self._data_dir / "qq_emoji_index.json", self.http
        )
        asyncio.create_task(self.emoji.load())

        self.translator = Translator(self)
        self.dc = DiscordSide(self)
        self.qq = QQSender(self)

        for platform in self.config.get("recovery_platforms") or []:
            if isinstance(platform, str) and platform.strip():
                self._recovery_ready[platform.strip()] = asyncio.Event()

        self._initialized = True
        logger.info(
            "[dcqq-relay] 插件已加载：%d 条互通关系，翻译 %s",
            len(self.relations),
            "开启" if (self.config.get("translation") or {}).get("enabled") else "关闭",
        )
        asyncio.create_task(self._attach_discord_listeners())

    async def terminate(self) -> None:
        client = self.dc.client if self.dc is not None else None
        if client is not None:
            for handler, name in (
                (self._on_discord_delete, "on_raw_message_delete"),
                (self._on_discord_edit, "on_raw_message_edit"),
            ):
                try:
                    client.remove_listener(handler, name)
                except Exception:
                    pass
        self._listener_client_id = None
        if self.store is not None:
            await self.store.close()
            self.store = None
        if self.http is not None:
            await self.http.close()
        self._initialized = False

    # -- 平台查找 -----------------------------------------------------------
    def discord_platform(self):
        manager = getattr(self.context, "platform_manager", None)
        instances = manager.get_insts() if manager is not None else []
        target_id = str(self.config.get("discord_platform_id") or "").strip()
        for inst in instances:
            try:
                meta = inst.meta()
            except Exception:
                continue
            if meta.name != "discord":
                continue
            if target_id and meta.id != target_id:
                continue
            return inst
        return None

    async def _attach_discord_listeners(self) -> None:
        if self._attach_running:
            return
        self._attach_running = True
        try:
            for _ in range(30):
                platform = self.discord_platform()
                client = (
                    getattr(platform, "client", None) if platform is not None else None
                )
                if client is not None:
                    if self._listener_client_id != id(client):
                        try:
                            client.add_listener(
                                self._on_discord_delete, "on_raw_message_delete"
                            )
                            client.add_listener(
                                self._on_discord_edit, "on_raw_message_edit"
                            )
                            self._listener_client_id = id(client)
                            logger.info("[dcqq-relay] 已挂载 Discord 撤回 / 编辑监听")
                        except Exception as exc:
                            logger.warning(
                                "[dcqq-relay] 挂载 Discord 监听失败：%s", exc
                            )
                    return
                await asyncio.sleep(2)
        finally:
            self._attach_running = False

    # -- 事件入口 -----------------------------------------------------------
    @filter.event_message_type(filter.EventMessageType.ALL)
    async def on_any_message(self, event: AstrMessageEvent):
        try:
            await self._handle_event(event)
        except Exception:
            logger.exception("[dcqq-relay] 处理消息事件时发生异常")

    async def _handle_event(self, event: AstrMessageEvent) -> None:
        if not self._initialized or self.store is None:
            return
        if not self.config.get("enabled", True):
            return
        if self._listener_client_id is None:
            asyncio.create_task(self._attach_discord_listeners())

        raw = event.message_obj.raw_message
        if isinstance(raw, dict) and raw.get("post_type") == "notice":
            await self._handle_notice(event, raw)
            return

        match = self._match(event)
        if match is None:
            return
        relation, direction = match

        # 不转发机器人自己发的消息
        if str(event.get_sender_id()) == str(event.message_obj.self_id):
            return

        if not self._has_content(event):
            return
        if self._is_own_command(event):
            return

        if direction == "dc2qq" and discord is not None and isinstance(
            raw, discord.Message
        ):
            if getattr(getattr(raw, "author", None), "bot", False):
                return

        if self.config.get("suppress_llm", True):
            try:
                event.should_call_llm(False)
            except Exception:
                pass

        if await self._is_relayed(event, relation, direction):
            return

        ready = self._recovery_ready.get(relation.forward_platform)
        if ready is not None and not ready.is_set():
            asyncio.create_task(self._relay_when_ready(event, relation, direction))
            return

        await self._relay(event, relation, direction)

    async def _relay_when_ready(
        self, event: AstrMessageEvent, relation: Relation, direction: str
    ) -> None:
        ready = self._recovery_ready.get(relation.forward_platform)
        if ready is None:
            return
        await ready.wait()
        try:
            if await self._is_relayed(event, relation, direction):
                return
            await self._relay(event, relation, direction)
        except Exception:
            logger.exception("[dcqq-relay] 恢复后补发消息失败")

    # -- 匹配与判断 ---------------------------------------------------------
    def _match(self, event: AstrMessageEvent) -> tuple[Relation, str] | None:
        platform_name = event.get_platform_name()
        platform_id = event.get_platform_id()
        channel_id = str(event.message_obj.session_id or "")
        # 兼容某些配置下 session_id 带 "用户ID_" 之类前缀的情况
        channel_ids = {channel_id}
        if "_" in channel_id:
            channel_ids.add(channel_id.rsplit("_", 1)[-1])
        group = getattr(event.message_obj, "group", None)
        group_id = str(group.group_id) if group is not None else ""
        sender_id = str(event.get_sender_id())

        for relation in self.relations:
            if (
                platform_name == "discord"
                and relation.discord_channel_id in channel_ids
            ):
                return relation, "dc2qq"
            if platform_id == relation.forward_platform or (
                platform_name == relation.forward_platform
            ):
                if relation.is_group and relation.forward_channel in (
                    group_id,
                    channel_id,
                    *channel_ids,
                ):
                    return relation, "qq2dc"
                if not relation.is_group and relation.forward_channel in (
                    channel_id,
                    sender_id,
                    *channel_ids,
                ):
                    return relation, "qq2dc"
        return None

    @staticmethod
    def _has_content(event: AstrMessageEvent) -> bool:
        components = event.message_obj.message or []
        meaningful = [c for c in components if not isinstance(c, Comp.Reply)]
        if meaningful:
            return True
        return bool((event.message_str or "").strip())

    @staticmethod
    def _is_own_command(event: AstrMessageEvent) -> bool:
        text = (event.message_str or "").strip()
        for name in ("relay", "relay_status", "relay_recovery"):
            if text == f"/{name}" or text.startswith(f"/{name} "):
                return True
        return False

    async def _is_relayed(
        self, event: AstrMessageEvent, relation: Relation, direction: str
    ) -> bool:
        if self.store is None:
            return False
        message_id = str(event.message_obj.message_id or "")
        if not message_id:
            return False
        if direction == "dc2qq":
            return await self.store.exists_dc(message_id)
        return await self.store.exists_forward(
            relation.forward_platform, relation.forward_channel, message_id
        )

    def _delete_sync_enabled(self) -> bool:
        return bool((self.config.get("advanced") or {}).get("delete_sync", True))

    def _edit_sync_enabled(self) -> bool:
        return bool((self.config.get("advanced") or {}).get("edit_sync", True))

    # -- 自己触发的删除事件防抖 ---------------------------------------------
    def _remember_self_delete(self, key: str) -> None:
        now = time.time()
        for old_key, stamp in list(self._self_deleted.items()):
            if now - stamp > SELF_DELETE_TTL:
                self._self_deleted.pop(old_key, None)
        self._self_deleted[key] = now

    def _is_self_deleted(self, key: str) -> bool:
        stamp = self._self_deleted.get(key)
        if stamp is None:
            return False
        if time.time() - stamp > SELF_DELETE_TTL:
            self._self_deleted.pop(key, None)
            return False
        return True

    # -- 转发 ---------------------------------------------------------------
    async def _relay(
        self, event: AstrMessageEvent, relation: Relation, direction: str
    ) -> None:
        if direction == "dc2qq":
            await self._relay_dc_to_qq(event, relation)
        else:
            await self._relay_qq_to_dc(event, relation)

    async def _relay_dc_to_qq(
        self, event: AstrMessageEvent, relation: Relation, prefix: str = ""
    ) -> None:
        message = event.message_obj.raw_message
        if discord is not None and isinstance(message, discord.Message):
            await self._send_dc_message_to_qq(message, relation, prefix)
            return

        parts = [c for c in (event.message_obj.message or []) if isinstance(
            c, (Comp.Plain, Comp.Image, Comp.Record, Comp.Video, Comp.File)
        )]
        parts = await self._translate_parts(parts, "dc2qq")
        name = event.get_sender_name() or event.get_sender_id()
        chain = [Comp.Plain(f"{prefix}{name}: \n")] + parts
        ids = await self.qq.send(relation, chain)
        for mid in ids:
            await self.store.add(
                relation=relation.key,
                dc_id=str(event.message_obj.message_id),
                dc_channel=str(event.message_obj.session_id),
                forward_platform=relation.forward_platform,
                forward_channel=relation.forward_channel,
                forward_id=mid,
            )

    async def _send_dc_message_to_qq(
        self, message, relation: Relation, prefix: str = ""
    ) -> None:
        parts = await self._build_dc_parts(message, relation)
        if not parts:
            return
        parts = await self._translate_parts(parts, "dc2qq")
        label = self._author_label(message)
        head = f"{prefix}{label}: \n"

        if relation.reverse_name and self._only_media(parts):
            chain = parts + [Comp.Plain("\n" + head.strip())]
        else:
            chain = [Comp.Plain(head)] + parts

        ids = await self.qq.send(relation, chain)
        dc_channel = str(
            getattr(getattr(message, "channel", None), "id", "")
            or relation.discord_channel_id
        )
        for mid in ids:
            await self.store.add(
                relation=relation.key,
                dc_id=str(getattr(message, "id", "")),
                dc_channel=dc_channel,
                forward_platform=relation.forward_platform,
                forward_channel=relation.forward_channel,
                forward_id=mid,
            )

    async def _relay_qq_to_dc(
        self, event: AstrMessageEvent, relation: Relation, prefix: str = ""
    ) -> None:
        await self._send_qq_message_to_dc(
            event.message_obj,
            relation,
            prefix,
            event.get_platform_id(),
            event.get_platform_name(),
        )

    async def _send_qq_message_to_dc(
        self,
        message_obj: Any,
        relation: Relation,
        prefix: str,
        platform_id: str,
        platform_name: str,
    ) -> None:
        parts = await self._build_qq_parts(message_obj, relation)
        if not parts:
            return
        parts = await self._translate_parts(parts, "qq2dc")

        reference_id: str | None = None
        cleaned: list[Any] = []
        for part in parts:
            if isinstance(part, Comp.Reply):
                if reference_id is None:
                    reference_id = str(part.id)
                continue
            cleaned.append(part)
        parts = cleaned

        sender = getattr(message_obj, "sender", None)
        sender_id = str(getattr(sender, "user_id", "") or "")
        sender_name = str(
            getattr(sender, "nickname", "") or sender_id or "QQ 用户"
        )

        username = self._webhook_username(sender_name, sender_id, platform_name)
        display_name = sender_name
        if relation.show_forward_id and sender_id:
            display_name = f"[QQ:{sender_id}] {sender_name}"
        fallback_prefix = f"{prefix}{display_name}: \n"

        avatar = self._qq_avatar(platform_id, sender_id)
        dc_id = await self.dc.send(
            relation,
            parts,
            username=username,
            avatar_url=avatar,
            fallback_prefix=fallback_prefix,
            reference_id=reference_id,
        )
        if dc_id:
            await self.store.add(
                relation=relation.key,
                dc_id=dc_id,
                dc_channel=relation.discord_channel_id,
                forward_platform=relation.forward_platform,
                forward_channel=relation.forward_channel,
                forward_id=str(getattr(message_obj, "message_id", "")),
            )

    # -- 消息转换 -----------------------------------------------------------
    @staticmethod
    def _only_media(parts: list[Any]) -> bool:
        has_text = any(
            isinstance(p, Comp.Plain) and (p.text or "").strip() for p in parts
        )
        has_media = any(
            isinstance(p, (Comp.Image, Comp.Record, Comp.Video, Comp.File))
            for p in parts
        )
        return has_media and not has_text

    async def _build_dc_parts(self, message, relation: Relation) -> list[Any]:
        parts: list[Any] = []

        # 引用回复：Discord 消息 ID → 之前转发到 QQ 的消息 ID
        reference = getattr(message, "reference", None)
        reference_id = getattr(reference, "message_id", None) if reference else None
        if reference_id and self.store is not None:
            rows = await self.store.by_dc(str(reference_id))
            if rows:
                parts.append(Comp.Reply(id=rows[0].forward_id))

        text = self._format_discord_text(message)
        if text:
            parts.append(Comp.Plain(text))

        for attachment in getattr(message, "attachments", None) or []:
            component = await self._discord_attachment(attachment, relation)
            if component is not None:
                parts.append(component)

        for sticker in getattr(message, "stickers", None) or []:
            sticker_id = getattr(sticker, "id", None)
            if sticker_id:
                parts.append(
                    Comp.Image.fromURL(
                        f"https://media.discordapp.net/stickers/{sticker_id}.png"
                    )
                )

        embed_text = self._format_discord_embeds(message)
        if embed_text:
            parts.append(Comp.Plain(embed_text))
        return parts

    async def _discord_attachment(self, attachment, relation: Relation):
        url = str(getattr(attachment, "url", "") or "")
        filename = str(getattr(attachment, "filename", "") or "") or guess_filename(
            url, "file"
        )
        content_type = str(getattr(attachment, "content_type", "") or "").lower()
        size = int(getattr(attachment, "size", 0) or 0)
        max_bytes = self.http.max_bytes if self.http is not None else 20 * 1024 * 1024

        if size and size > max_bytes:
            return Comp.Plain(f"\n[文件过大，已跳过：{filename}]")

        if content_type.startswith("image/"):
            if relation.download_asset and self.http is not None:
                data = await self.http.get_bytes(url)
                if data:
                    return Comp.Image.fromBase64(base64.b64encode(data).decode())
            return Comp.Image(file=url, url=url)
        if content_type.startswith("audio/"):
            return Comp.Record(file=url, url=url)
        if content_type.startswith("video/"):
            return Comp.Video(file=url, url=url)
        return Comp.File(name=filename, url=url)

    def _format_discord_text(self, message) -> str:
        content = str(getattr(message, "content", "") or "")

        for member in getattr(message, "mentions", None) or []:
            name = (
                getattr(member, "display_name", None)
                or getattr(member, "name", "")
                or "用户"
            )
            content = content.replace(f"<@{member.id}>", f"@{name}")
            content = content.replace(f"<@!{member.id}>", f"@{name}")
        for role in getattr(message, "role_mentions", None) or []:
            content = content.replace(
                f"<@&{role.id}>", f"@{getattr(role, 'name', '身份组')}"
            )
        for channel in getattr(message, "channel_mentions", None) or []:
            content = content.replace(
                f"<#{channel.id}>", f"#{getattr(channel, 'name', '频道')}"
            )

        content = re.sub(r"<a?:([A-Za-z0-9_]+):\d+>", r"[表情:\1]", content)

        if self.config.get("ignore_at_all", True):
            content = re.sub(r"@(everyone|here)\b", "", content)

        return content.strip()

    @staticmethod
    def _format_discord_embeds(message) -> str:
        blocks: list[str] = []
        for embed in getattr(message, "embeds", None) or []:
            lines: list[str] = []
            title = getattr(embed, "title", None)
            description = getattr(embed, "description", None)
            if title:
                lines.append(f"【{title}】")
            if description:
                lines.append(str(description))
            for field in getattr(embed, "fields", None) or []:
                lines.append(
                    f"{getattr(field, 'name', '')}: {getattr(field, 'value', '')}"
                )
            footer = getattr(embed, "footer", None)
            footer_text = getattr(footer, "text", None) if footer else None
            if footer_text:
                lines.append(f"— {footer_text}")
            if lines:
                blocks.append("\n".join(lines))
        return "\n\n".join(blocks).strip()

    @staticmethod
    def _author_label(message) -> str:
        author = getattr(message, "author", None)
        if author is None:
            return "Unknown"
        display = (
            getattr(author, "display_name", None)
            or getattr(author, "name", "")
            or "Unknown"
        )
        username = getattr(author, "name", "") or ""
        if username and username != display:
            return f"{display} (@{username})"
        return f"@{username or display}"

    async def _build_qq_parts(self, message_obj: Any, relation: Relation) -> list[Any]:
        parts: list[Any] = []
        ignore_at_all = bool(self.config.get("ignore_at_all", True))

        for seg in message_obj.message or []:
            if isinstance(seg, Comp.Plain):
                if seg.text:
                    parts.append(Comp.Plain(seg.text))
            elif isinstance(seg, Comp.At):
                if str(seg.qq) == "all":
                    if not ignore_at_all:
                        parts.append(Comp.Plain("@everyone"))
                else:
                    parts.append(Comp.Plain(f"@{seg.name or seg.qq}"))
            elif isinstance(seg, Comp.Face):
                description = (
                    self.emoji.describe(seg.id) if self.emoji is not None else f"表情:{seg.id}"
                )
                parts.append(Comp.Plain(f"[{description}]"))
            elif isinstance(seg, Comp.Image):
                parts.append(Comp.Image(file=seg.url or seg.file))
            elif isinstance(seg, Comp.Record):
                ref = seg.url or seg.file or ""
                suffix = os.path.splitext(guess_filename(ref, ""))[1] or ".amr"
                parts.append(Comp.File(name=f"语音{suffix}", url=ref))
            elif isinstance(seg, Comp.Video):
                parts.append(Comp.File(name="视频.mp4", url=seg.url or seg.file))
            elif isinstance(seg, Comp.File):
                parts.append(
                    Comp.File(name=seg.name or "文件", url=seg.url or seg.file_)
                )
            elif isinstance(seg, Comp.Reply):
                if self.store is None:
                    continue
                rows = await self.store.by_forward(
                    relation.forward_platform,
                    relation.forward_channel,
                    str(seg.id),
                )
                if rows:
                    parts.append(Comp.Reply(id=rows[0].dc_id))
            else:
                continue
        return parts

    async def _translate_parts(self, parts: list[Any], direction: str) -> list[Any]:
        if self.translator is None:
            return parts
        if not self.translator.target_language(direction):
            return parts

        show_original = bool(
            (self.config.get("translation") or {}).get("show_original")
        )
        indexes = [
            index
            for index, part in enumerate(parts)
            if isinstance(part, Comp.Plain) and (part.text or "").strip()
        ]
        if not indexes:
            return parts

        results = await asyncio.gather(
            *[self.translator.translate(parts[i].text, direction) for i in indexes],
            return_exceptions=True,
        )
        for index, result in zip(indexes, results):
            if isinstance(result, Exception) or not result:
                continue
            original = parts[index].text
            if show_original:
                parts[index] = Comp.Plain(f"{result}\n{original}")
            else:
                parts[index] = Comp.Plain(result)
        return parts

    def _webhook_username(
        self, name: str, sender_id: str, platform_name: str
    ) -> str:
        template = str(
            (self.config.get("discord_webhook") or {}).get("nickname_template")
            or "{name}"
        )
        try:
            return template.format(
                name=name, id=sender_id, platform=platform_name
            )[:80]
        except Exception:
            return name[:80]

    def _qq_avatar(self, platform_id: str, sender_id: str) -> str | None:
        if not sender_id.isdigit():
            return None
        platform_name = ""
        if self.qq is not None:
            inst = self.qq.platform(platform_id)
            if inst is not None:
                try:
                    platform_name = inst.meta().name
                except Exception:
                    platform_name = ""
        if platform_name != "aiocqhttp":
            return None
        return f"https://q1.qlogo.cn/g?b=qq&nk={sender_id}&s=640"

    # -- 撤回 / 删除同步 ----------------------------------------------------
    async def _handle_notice(self, event: AstrMessageEvent, raw: dict) -> None:
        if not self._delete_sync_enabled() or self.store is None:
            return
        notice_type = str(raw.get("notice_type") or "")
        if notice_type not in ("group_recall", "friend_recall"):
            return
        if event.get_platform_name() == "discord":
            return

        forward_id = str(raw.get("message_id") or "")
        if not forward_id:
            return
        group_id = str(raw.get("group_id") or "")
        platform_id = event.get_platform_id()
        platform_name = event.get_platform_name()

        rows: list[RelayRow] = []
        seen: set[int] = set()
        for relation in self.relations:
            if relation.forward_platform not in (platform_id, platform_name):
                continue
            if relation.is_group and group_id != relation.forward_channel:
                continue
            matched = await self.store.by_forward(
                relation.forward_platform, relation.forward_channel, forward_id
            )
            for row in matched:
                if row.id not in seen:
                    seen.add(row.id)
                    rows.append(row)
        if not rows:
            return

        await self.store.mark_deleted([row.id for row in rows])
        for row in rows:
            if not row.dc_channel:
                continue
            self._remember_self_delete(f"dc:{row.dc_channel}:{row.dc_id}")
            await self.dc.delete(row.dc_channel, row.dc_id)

    async def _on_discord_delete(self, payload) -> None:
        try:
            if not self._delete_sync_enabled() or self.store is None:
                return
            channel_id = str(getattr(payload, "channel_id", "") or "")
            message_id = str(getattr(payload, "message_id", "") or "")
            if not message_id:
                return
            key = f"dc:{channel_id}:{message_id}"
            if self._is_self_deleted(key):
                return
            rows = await self.store.by_dc(message_id)
            if not rows:
                return
            await self.store.mark_deleted([row.id for row in rows])
            for row in rows:
                await self.qq.delete(row.forward_platform, row.forward_id)
        except Exception:
            logger.exception("[dcqq-relay] 处理 Discord 删除事件失败")

    async def _on_discord_edit(self, payload) -> None:
        try:
            if not self._edit_sync_enabled() or self.store is None:
                return
            data = getattr(payload, "data", None) or {}
            author = data.get("author") or {}
            if author.get("bot"):
                return
            content = str(data.get("content") or "").strip()
            if not content:
                return

            channel_id = str(getattr(payload, "channel_id", "") or "")
            message_id = str(getattr(payload, "message_id", "") or "")
            relation = next(
                (
                    item
                    for item in self.relations
                    if item.discord_channel_id == channel_id
                ),
                None,
            )
            if relation is None:
                return

            old_rows = await self.store.by_dc(message_id)
            await self.store.mark_deleted([row.id for row in old_rows])
            for row in old_rows:
                await self.qq.delete(row.forward_platform, row.forward_id)

            display = (
                author.get("global_name") or author.get("username") or "Unknown"
            )
            username = author.get("username") or ""
            label = (
                f"{display} (@{username})"
                if username and username != display
                else f"@{username or display}"
            )
            parts = [Comp.Plain(content + "\n(edited)")]
            parts = await self._translate_parts(parts, "dc2qq")
            ids = await self.qq.send(
                relation, [Comp.Plain(f"{label}: \n")] + parts
            )
            for mid in ids:
                await self.store.add(
                    relation=relation.key,
                    dc_id=message_id,
                    dc_channel=channel_id,
                    forward_platform=relation.forward_platform,
                    forward_channel=relation.forward_channel,
                    forward_id=mid,
                )
        except Exception:
            logger.exception("[dcqq-relay] 处理 Discord 编辑事件失败")

    # -- 历史恢复 -----------------------------------------------------------
    async def _recover_relation(
        self, relation: Relation, begin: dt.datetime, dry: bool
    ) -> int:
        items: list[tuple[float, str, Any]] = []

        dc_messages = await self.dc.history(relation.discord_channel_id, begin)
        for message in dc_messages:
            if getattr(getattr(message, "author", None), "bot", False):
                continue
            items.append((message.created_at.timestamp(), "dc", message))

        if self.qq is not None:
            for abm, timestamp in await self.qq.history(relation, begin.timestamp()):
                items.append((timestamp, "qq", abm))

        items.sort(key=lambda item: item[0])
        logger.info(
            "[dcqq-relay] 关系 %s 共读取到 %d 条历史消息", relation.key, len(items)
        )

        count = 0
        for timestamp, kind, obj in items:
            stamp = (
                dt.datetime.fromtimestamp(timestamp)
                .astimezone()
                .isoformat(timespec="seconds")
            )
            prefix = f"{RECOVERY_PREFIX} [{stamp}] "

            if kind == "dc":
                if await self.store.exists_dc(str(obj.id)):
                    continue
            else:
                if await self.store.exists_forward(
                    relation.forward_platform,
                    relation.forward_channel,
                    str(obj.message_id),
                ):
                    continue

            count += 1
            if dry:
                continue
            try:
                if kind == "dc":
                    await self._send_dc_message_to_qq(obj, relation, prefix)
                else:
                    await self._send_qq_message_to_dc(
                        obj, relation, prefix, relation.forward_platform, ""
                    )
            except Exception:
                logger.exception("[dcqq-relay] 恢复 %s 消息失败", kind)
            await asyncio.sleep(1)
        return count

    @staticmethod
    def _parse_time(text: str) -> dt.datetime | None:
        text = text.strip().replace("T", " ")
        tz = dt.datetime.now().astimezone().tzinfo
        formats = (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d %H:%M",
            "%Y/%m/%d",
            "%m-%d %H:%M",
            "%m-%d",
        )
        for fmt in formats:
            try:
                value = dt.datetime.strptime(text, fmt)
            except ValueError:
                continue
            if "%Y" not in fmt:
                value = value.replace(year=dt.datetime.now().year)
            return value.replace(tzinfo=tz)
        return None

    # -- 指令 ---------------------------------------------------------------
    @filter.command("relay")
    async def relay(self, event: AstrMessageEvent):
        """查看 DC-QQ 互通帮助信息。"""
        return event.plain_result(self._help_text())

    @filter.command("relay_status")
    async def relay_status(self, event: AstrMessageEvent):
        """查看互通关系状态。"""
        if not self._initialized or self.store is None:
            return event.plain_result("插件尚未初始化完成，请稍后再试。")

        lines = [f"DC-QQ 互通：{len(self.relations)} 条关系"]
        for relation in self.relations:
            ready = self._recovery_ready.get(relation.forward_platform)
            state = ""
            if ready is not None:
                state = "（已就绪）" if ready.is_set() else "（等待恢复）"
            lines.append(
                f"• {relation.name}：discord:{relation.discord_channel_id} ⇄ "
                f"{relation.forward_platform}:{relation.forward_channel}"
                f"（{'群聊' if relation.is_group else '私聊'}）{state}"
            )
        lines.append(f"消息映射记录：{await self.store.count()} 条")
        translation = self.config.get("translation") or {}
        lines.append(
            "自动翻译："
            + (
                f"开启（QQ→DC：{translation.get('to_discord_language') or '不翻译'}；"
                f"DC→QQ：{translation.get('to_qq_language') or '不翻译'}）"
                if translation.get("enabled")
                else "关闭"
            )
        )
        lines.append(
            "忽略 @全体："
            + ("是" if self.config.get("ignore_at_all", True) else "否")
        )
        webhook = self.config.get("discord_webhook") or {}
        if self.dc is not None and self.dc._webhook_unavailable:
            webhook_state = "已尝试但不可用（已回退为机器人身份）"
        else:
            webhook_state = "开启" if webhook.get("enabled", True) else "关闭"
        lines.append(f"Discord Webhook 身份：{webhook_state}")
        if self.dc is not None and not self.dc.available:
            lines.append("⚠️ 未找到可用的 Discord 平台实例，请检查适配器是否已启动。")
        return event.plain_result("\n".join(lines))

    @filter.command("relay_recovery")
    async def relay_recovery(self, event: AstrMessageEvent, args: GreedyStr):
        """恢复指定时间之后的历史消息。"""
        if not self._is_admin(event):
            yield event.plain_result("只有管理员可以执行历史恢复。")
            return
        if not self._initialized or self.store is None:
            yield event.plain_result("插件尚未初始化完成，请稍后再试。")
            return

        raw_args = str(args or "").strip()
        tokens = raw_args.split()
        dry_run = any(token in {"--dry", "--dry-run", "dry"} for token in tokens)
        tokens = [
            token
            for token in tokens
            if token not in {"--dry", "--dry-run", "dry"}
        ]
        if not tokens:
            yield event.plain_result(
                "用法：/relay_recovery <开始时间> [平台ID|all] [--dry]\n"
                "例如：/relay_recovery 2026-11-01 12:00 aiocqhttp\n"
                "      /relay_recovery 2026-11-01 --dry"
            )
            return

        begin_text = tokens[0]
        index = 1
        if index < len(tokens) and re.fullmatch(
            r"\d{1,2}:\d{2}(:\d{2})?", tokens[index]
        ):
            begin_text = f"{begin_text} {tokens[index]}"
            index += 1
        platform = tokens[index].strip() if index < len(tokens) else "all"
        platform = platform or "all"

        begin_time = self._parse_time(begin_text)
        if begin_time is None:
            yield event.plain_result(
                f"无法识别的时间：{begin_text}\n"
                "支持格式：2026-11-01、2026-11-01 12:00、2026-11-01 12:00:00、11-01 12:00"
            )
            return

        targets = [
            relation
            for relation in self.relations
            if platform == "all" or relation.forward_platform == platform
        ]
        if not targets:
            yield event.plain_result(f"没有匹配平台 {platform} 的互通关系。")
            return
        if self._recovering:
            yield event.plain_result("正在恢复中，请稍后再试。")
            return

        self._recovering = True
        total = 0
        failed: list[str] = []
        try:
            for relation in targets:
                try:
                    total += await self._recover_relation(
                        relation, begin_time, dry_run
                    )
                except Exception as exc:
                    logger.exception(
                        "[dcqq-relay] 恢复关系 %s 失败", relation.key
                    )
                    failed.append(f"{relation.key}: {exc}")
        finally:
            self._recovering = False
            if not dry_run:
                for relation in targets:
                    ready = self._recovery_ready.get(relation.forward_platform)
                    if ready is not None:
                        ready.set()

        message = f"恢复完成：共检查 / 转发 {total} 条消息"
        if dry_run:
            message += "（dry-run，未实际发送）"
        if failed:
            message += "\n失败：" + "；".join(failed)
        yield event.plain_result(message)

    def _is_admin(self, event: AstrMessageEvent) -> bool:
        try:
            if event.is_admin():
                return True
        except Exception:
            pass
        configured = self.config.get("admin_ids") or []
        if isinstance(configured, str):
            configured = [configured]
        sender_id = str(event.get_sender_id())
        return sender_id in {str(item) for item in configured}

    @staticmethod
    def _help_text() -> str:
        return (
            "DC-QQ 互通插件\n"
            "把 Discord 频道和 QQ 群双向连起来，配置在 WebUI 的插件设置里，"
            "一条关系即双向，不需要分别配置两个方向。\n\n"
            "指令：\n"
            "/relay — 显示本帮助\n"
            "/relay_status — 查看关系、映射数量、翻译与 Webhook 状态\n"
            "/relay_recovery <开始时间> [平台ID|all] [--dry] — 恢复历史消息（管理员）\n\n"
            "原始 Koishi 插件：https://github.com/koishijs/koishi-plugin-dcqq-relay"
        )

