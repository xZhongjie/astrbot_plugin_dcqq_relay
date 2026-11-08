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
import html
import json
import os
import random
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
BIND_CODE_TTL = 300  # 聊天绑定码有效期（秒）
DISCORD_EMOJI_RE = re.compile(r"<(a?):([A-Za-z0-9_]+):(\d+)>")
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
    """复用 aiohttp session 的小封装，带体积上限、按 URL 选择代理、强制 IPv4。"""

    def __init__(
        self, max_bytes: int, proxy_resolver=None, force_ipv4: bool = True
    ) -> None:
        self.max_bytes = max_bytes
        self._proxy_resolver = proxy_resolver
        self.force_ipv4 = force_ipv4
        self._session: aiohttp.ClientSession | None = None

    def _proxy_for(self, url: str) -> str | None:
        if self._proxy_resolver is None:
            return None
        try:
            return self._proxy_resolver(url)
        except Exception:
            return None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=90)
            headers = {
                # 部分 QQ / Discord CDN 会拒绝非浏览器 UA
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            }
            connector = None
            if self.force_ipv4:
                # IPv6 出网不通时，大文件会在传输中途被截断
                try:
                    import socket

                    connector = aiohttp.TCPConnector(family=socket.AF_INET)
                except Exception:
                    connector = None
            self._session = aiohttp.ClientSession(
                timeout=timeout, headers=headers, connector=connector
            )
        return self._session

    async def request_full(
        self,
        url: str,
        *,
        headers: dict | None = None,
        timeout: float = 60,
    ) -> tuple[int, bytes, dict]:
        session = await self._get_session()
        try:
            async with session.get(
                url,
                headers=headers or {},
                timeout=aiohttp.ClientTimeout(total=timeout),
                proxy=self._proxy_for(url),
            ) as resp:
                body = await resp.content.read(self.max_bytes + 1)
                return resp.status, body, {k.lower(): v for k, v in resp.headers.items()}
        except Exception as exc:
            logger.warning("[dcqq-relay] 下载失败：%s（%s）", url[:200], exc)
            return 0, b"", {}

    async def download_range(
        self,
        url: str,
        *,
        headers: dict | None = None,
        chunk_size: int = 2 * 1024 * 1024,
        max_bytes: int = 0,
        timeout: float = 120,
    ) -> bytes | None:
        """分片下载，绕过单连接被截断。支持 206 与整包 200。"""
        if not url.startswith(("http://", "https://")):
            return await self.get_bytes(url)
        status, first, resp_headers = await self.request_full(
            url,
            headers={**(headers or {}), "Range": f"bytes=0-{chunk_size - 1}"},
            timeout=timeout,
        )
        if status == 200:
            return first or None
        if status != 206 or not first:
            return None
        total = 0
        content_range = resp_headers.get("content-range") or ""
        if "/" in content_range:
            try:
                total = int(content_range.rsplit("/", 1)[-1])
            except ValueError:
                total = 0
        data = bytearray(first)
        offset = len(first)
        while total and offset < total:
            if max_bytes and offset >= max_bytes:
                return None
            end = min(offset + chunk_size - 1, total - 1)
            status, part, _ = await self.request_full(
                url,
                headers={**(headers or {}), "Range": f"bytes={offset}-{end}"},
                timeout=timeout,
            )
            if status not in (200, 206) or not part:
                break
            data.extend(part)
            offset += len(part)
        if total and len(data) < total:
            logger.warning(
                "[dcqq-relay] 分片下载不完整：期望 %d，实际 %d", total, len(data)
            )
            return None
        return bytes(data)

    async def get_bytes(self, url: str) -> bytes | None:
        if not url:
            return None
        url = html.unescape(str(url).strip())

        if url.startswith("base64://"):
            try:
                data = base64.b64decode(url[len("base64://") :])
            except Exception:
                return None
            return data if len(data) <= self.max_bytes else None

        if url.startswith("data:"):
            header, _, payload = url.partition(",")
            if ";base64" in header:
                try:
                    data = base64.b64decode(payload)
                except Exception:
                    return None
                return data if len(data) <= self.max_bytes else None
            return None

        if url.startswith("file://"):
            path = Path(uri_to_path(url))
            if path.is_file() and path.stat().st_size <= self.max_bytes:
                return await asyncio.to_thread(path.read_bytes)
            return None

        # 兼容只有路径 / 只有文件名的本地资源
        if not url.startswith(("http://", "https://")):
            path = Path(uri_to_path(url))
            try:
                if path.is_file() and path.stat().st_size <= self.max_bytes:
                    return await asyncio.to_thread(path.read_bytes)
            except OSError:
                pass
            return None

        session = await self._get_session()
        proxy = self._proxy_for(url)
        try:
            async with session.get(url, proxy=proxy) as resp:
                resp.raise_for_status()
                if resp.content_length and resp.content_length > self.max_bytes:
                    logger.warning(
                        "[dcqq-relay] 跳过超大文件（%s 字节）：%s",
                        resp.content_length,
                        url[:200],
                    )
                    return None
                data = await resp.content.read(self.max_bytes + 1)
                if len(data) > self.max_bytes:
                    return None
                return data
        except Exception as exc:
            logger.warning("[dcqq-relay] 下载资源失败：%s（%s）", url[:200], exc)
            return None

    async def get_json(self, url: str) -> Any:
        session = await self._get_session()
        try:
            async with session.get(url, proxy=self._proxy_for(url)) as resp:
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
        self._webhook_retry_at = 0.0
        self._webhook_error = ""

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
        webhook = await self._get_webhook(channel)
        plain_content = payload.content
        with_prefix = (fallback_prefix + plain_content).strip()

        # Webhook 不支持 reply（reference 参数会直接 TypeError），改用引用文本
        ref = reference_id or payload.reference_id
        quote_text = ""
        if ref and webhook is not None:
            quote_text = await self._quote_reference(channel, ref)
            if quote_text:
                plain_content = f"{quote_text}\n{plain_content}".strip()
                with_prefix = f"{quote_text}\n{with_prefix}".strip()

        if len(with_prefix) > max_chars:
            with_prefix = with_prefix[:max_chars].rstrip() + "\n…（消息过长已截断）"
        if len(plain_content) > max_chars:
            plain_content = plain_content[:max_chars].rstrip() + "\n…（消息过长已截断）"

        content = plain_content if webhook is not None else with_prefix
        if not content and not payload.files:
            return None

        kwargs: dict[str, Any] = {"allowed_mentions": discord.AllowedMentions.none()}
        if content:
            kwargs["content"] = content
        if payload.files:
            kwargs["files"] = payload.files[:10]

        if ref and webhook is None:
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
                # 只有权限/Token/删除这类永久性错误才禁用 Webhook；
                # 限流、网络抖动等临时错误保留缓存，下一条继续用 Webhook。
                status = getattr(exc, "status", None) or getattr(exc, "code", None)
                if status in (401, 403, 404):
                    self._webhooks.pop(getattr(channel, "id", 0), None)
                    self._webhook_unavailable = True
                    self._webhook_retry_at = time.time() + 300
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

    async def _quote_reference(self, channel, ref: Any) -> str:
        """Webhook 不支持 reply，拉取被引用消息生成引用文本。"""
        try:
            message = await channel.fetch_message(int(str(ref)))
        except Exception as exc:
            logger.debug("[dcqq-relay] 拉取被引用消息失败（%s）：%s", ref, exc)
            return ""
        author = getattr(getattr(message, "author", None), "display_name", "") or "未知"
        text = (getattr(message, "content", "") or "").replace("\n", " ").strip()
        if not text and getattr(message, "embeds", None):
            embeds = message.embeds
            title = getattr(embeds[0], "title", "") if embeds else ""
            text = str(title or "")
        if not text:
            return ""
        if len(text) > 80:
            text = text[:80] + "…"
        return f"> 回复 {author}：{text}"

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
                data = await self._component_bytes(seg)
                if data:
                    self._append_file(
                        data,
                        self._media_name(self._first_ref(seg), "image.png"),
                        payload.files,
                    )
                else:
                    parts.append(self._media_fallback(seg, "图片"))
            elif isinstance(seg, Comp.Record):
                data = await self._component_bytes(seg)
                if data:
                    self._append_file(
                        data,
                        self._media_name(self._first_ref(seg), "voice.wav"),
                        payload.files,
                    )
                else:
                    parts.append(self._media_fallback(seg, "语音"))
            elif isinstance(seg, Comp.Video):
                data = await self._component_bytes(seg)
                if data:
                    self._append_file(
                        data,
                        self._media_name(self._first_ref(seg), "video.mp4"),
                        payload.files,
                    )
                else:
                    parts.append(self._media_fallback(seg, "视频"))
            elif isinstance(seg, Comp.File):
                data = await self._component_bytes(seg)
                if data:
                    self._append_file(
                        data,
                        seg.name or self._media_name(self._first_ref(seg), "file.bin"),
                        payload.files,
                    )
                else:
                    parts.append(f"[文件: {seg.name or '未知'}]")
        payload.content = "".join(parts).strip()
        if not payload.content and not payload.files:
            return None
        return payload

    @staticmethod
    def _append_file(data: bytes, filename: str, files: list[Any]) -> bool:
        if len(files) >= 10:
            return False
        files.append(discord.File(BytesIO(data), filename=filename))
        return True

    # -- 媒体解析 -----------------------------------------------------------
    @staticmethod
    def _first_ref(seg: Any) -> str:
        attrs = ("url", "file_", "path") if isinstance(seg, Comp.File) else ("url", "file", "path")
        for attr in attrs:
            value = getattr(seg, attr, None)
            if isinstance(value, str) and value:
                return value
        return ""

    @staticmethod
    def _candidates(seg: Any) -> list[str]:
        """收集所有可能的资源引用，去掉重复并还原 HTML 转义。"""
        if isinstance(seg, Comp.File):
            attrs = ("url", "file_", "path")
        else:
            attrs = ("url", "file", "path")
        values: list[str] = []
        for attr in attrs:
            value = getattr(seg, attr, None)
            if isinstance(value, str) and value.strip():
                values.append(html.unescape(value.strip()))
        seen: set[str] = set()
        out: list[str] = []
        for value in values:
            if value not in seen:
                seen.add(value)
                out.append(value)
        return out

    @staticmethod
    def _uniq(values: list[str]) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []
        for value in values:
            if value and value not in seen:
                seen.add(value)
                out.append(value)
        return out

    def _media_fallback(self, seg: Any, kind: str) -> str:
        """下载失败时的占位：能拿到 http 链接就发链接，Discord 会自动展开预览。"""
        for value in self._candidates(seg):
            if value.startswith(("http://", "https://")):
                logger.warning(
                    "[dcqq-relay] %s下载失败，已回退为原始链接（%s）", kind, value[:200]
                )
                return f"\n{value}"
        logger.warning(
            "[dcqq-relay] %s下载失败且没有可用链接，已用占位文本（type=%s, refs=%s）",
            kind,
            type(seg).__name__,
            self._candidates(seg)[:3],
        )
        return f"[{kind}]"

    async def _component_bytes(self, seg: Any) -> bytes | None:
        """把消息组件转换成字节。

        优先使用 AstrBot 组件自带的解析（支持 url / base64 / 本地路径 / 各平台特殊格式），
        失败后再用插件自己的 Http 客户端逐个候选地址下载。
        """
        max_bytes = (
            self.plugin.http.max_bytes if self.plugin.http is not None else 20 * 1024 * 1024
        )

        # 1) AstrBot 组件解析
        try:
            if isinstance(seg, Comp.Image):
                convert = getattr(seg, "convert_to_base64", None)
                if callable(convert):
                    base64_data = await convert()
                    if base64_data:
                        data = base64.b64decode(base64_data)
                        if data and len(data) <= max_bytes:
                            return data
            elif isinstance(seg, (Comp.Record, Comp.Video)):
                convert = getattr(seg, "convert_to_file_path", None)
                if callable(convert):
                    path = await convert()
                    if path:
                        file_path = Path(path)
                        if file_path.is_file() and file_path.stat().st_size <= max_bytes:
                            return await asyncio.to_thread(file_path.read_bytes)
            elif isinstance(seg, Comp.File):
                getter = getattr(seg, "get_file", None)
                if callable(getter):
                    path = await getter()
                    if path:
                        file_path = Path(path)
                        if file_path.is_file() and file_path.stat().st_size <= max_bytes:
                            return await asyncio.to_thread(file_path.read_bytes)
        except Exception as exc:
            logger.debug("[dcqq-relay] AstrBot 组件解析失败，尝试手动下载：%s", exc)

        # 2) 手动下载所有候选地址（大文件用分片，避免连接被截断）
        chunk_kb = int(
            (self.plugin.config.get("advanced") or {}).get("download_chunk_kb") or 0
        )
        for candidate in self._candidates(seg):
            if candidate.startswith(("http://", "https://")) and chunk_kb > 0:
                data = await self.plugin.http.download_range(
                    candidate,
                    chunk_size=chunk_kb * 1024,
                    max_bytes=max_bytes,
                )
                if data:
                    return data
            data = await self.plugin.http.get_bytes(candidate)
            if data:
                return data
        return None

    @staticmethod
    def _media_name(url: str, default: str) -> str:
        if not url:
            return default
        name = guess_filename(url, default)
        if "." not in os.path.basename(name):
            return default
        return name

    async def _get_webhook(self, channel):
        # 失败后不要永久禁用：权限可能后来才授予，每 5 分钟重试一次
        if discord is None or time.time() < self._webhook_retry_at:
            return None
        cfg = self.plugin.config.get("discord_webhook") or {}
        if not cfg.get("enabled", True):
            return None
        cid = int(getattr(channel, "id", 0))
        if cid in self._webhooks:
            return self._webhooks[cid]

        # 1) 手动填写的 Webhook URL：不需要机器人拥有「管理 Webhook」权限
        manual_url = str(cfg.get("webhook_url") or "").strip()
        if manual_url:
            try:
                session = None
                if self.plugin.http is not None:
                    session = await self.plugin.http._get_session()
                webhook = discord.Webhook.from_url(manual_url, session=session)
                self._webhooks[cid] = webhook
                self._webhook_unavailable = False
                self._webhook_retry_at = 0.0
                self._webhook_error = ""
                logger.info("[dcqq-relay] 使用手动配置的 Webhook（频道 %s）", cid)
                return webhook
            except Exception as exc:
                logger.warning("[dcqq-relay] 手动 Webhook URL 无效：%s", exc)
                self._webhook_error = f"手动 Webhook 失败：{exc}"

        name = str(cfg.get("name") or "DCQQ Relay")
        try:
            hooks = await channel.webhooks()
            webhook = next(
                (h for h in hooks if h.name == name and getattr(h, "token", None)), None
            )
            if webhook is None:
                webhook = await channel.create_webhook(name=name)
            self._webhooks[cid] = webhook
            self._webhook_unavailable = False
            self._webhook_retry_at = 0.0
            self._webhook_error = ""
            logger.info(
                "[dcqq-relay] 已启用 Discord Webhook「%s」（频道 %s）", name, cid
            )
            return webhook
        except Exception as exc:
            self._webhook_error = f"{type(exc).__name__}: {exc}"
            logger.warning(
                "[dcqq-relay] 无法使用 Webhook（需要「管理 Webhook」权限，"
                "或手动填写 discord_webhook.webhook_url），改用机器人身份发送，"
                "5 分钟后重试：%s",
                exc,
            )
            self._webhook_unavailable = True
            self._webhook_retry_at = time.time() + 300
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
        self._bind_codes: dict[str, dict] = {}
        self._emoji_cache: dict[str, bytes] = {}

    # -- 生命周期 -----------------------------------------------------------
    async def initialize(self) -> None:
        self._data_dir = StarTools.get_data_dir(PLUGIN_NAME)
        max_mb = int((self.config.get("advanced") or {}).get("max_file_mb") or 20)
        self.http = HttpClient(
            max_mb * 1024 * 1024,
            proxy_resolver=self._download_proxy,
            force_ipv4=bool(
                (self.config.get("advanced") or {}).get("force_ipv4", True)
            ),
        )
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

    def _download_proxy(self, url: str) -> str | None:
        """下载媒体时使用的代理。

        - 配置了 advanced.proxy 时全局使用；
        - 否则只对 Discord 域名自动复用 Discord 适配器的 discord_proxy，
          避免把 QQ 图片请求也绕进代理。
        """
        configured = str((self.config.get("advanced") or {}).get("proxy") or "").strip()
        if configured:
            return configured

        host = (urlparse(url).hostname or "").lower()
        if not host.endswith(
            ("discordapp.com", "discordapp.net", "discord.com", "discord.gg")
        ):
            return None

        inst = self.discord_platform()
        if inst is None:
            return None
        platform_config = getattr(inst, "config", None) or {}
        try:
            proxy = str(platform_config.get("discord_proxy") or "").strip()
        except Exception:
            return None
        return proxy or None

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
        names = (
            "relay",
            "relay_status",
            "relay_recovery",
            "绑定",
            "完成绑定",
            "解绑",
            "bind",
            "bindcode",
            "complete_bind",
            "unbind",
        )
        for name in names:
            for candidate in (name, f"/{name}"):
                if text == candidate or text.startswith(f"{candidate} "):
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
            parts.extend(await self._content_to_parts(text))

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

        if self.config.get("ignore_at_all", True):
            content = re.sub(r"@(everyone|here)\b", "", content)

        # 表情被拿走后可能在原位置留下多余空格，压缩一下（不影响换行）
        content = re.sub(r"[ \t]{2,}", " ", content)

        # 注意：自定义表情 <:name:id> 不在这里转文字，
        # 由 _content_to_parts 转成图片后再拼进消息链。
        return content.strip()

    async def _content_to_parts(self, content: str) -> list[Any]:
        """把 Discord 文本拆成「文字 + 自定义表情图片」。

        自定义表情（<:name:id> / <a:name:id>）会下载成图片发给 QQ，
        避免 QQ 侧只看到一串不可读的 [表情:...] 文本。
        """
        parts: list[Any] = []
        position = 0
        for match in DISCORD_EMOJI_RE.finditer(content):
            before = content[position : match.start()]
            if before.strip():
                parts.append(Comp.Plain(before))
            parts.append(
                await self._emoji_component(
                    animated=bool(match.group(1)),
                    name=match.group(2),
                    emoji_id=match.group(3),
                )
            )
            position = match.end()
        tail = content[position:]
        if tail.strip():
            parts.append(Comp.Plain(tail))
        return parts

    async def _emoji_component(self, animated: bool, name: str, emoji_id: str) -> Any:
        """把 Discord 自定义表情转成 QQ 可发送的图片段。"""
        cache_key = f"{'a' if animated else 's'}:{emoji_id}"
        data = self._emoji_cache.get(cache_key)
        if data is None and self.http is not None:
            suffix = "gif" if animated else "png"
            url = f"https://cdn.discordapp.com/emojis/{emoji_id}.{suffix}?size=48"
            data = await self.http.get_bytes(url)
            if data:
                if len(self._emoji_cache) >= 200:
                    self._emoji_cache.clear()
                self._emoji_cache[cache_key] = data
        if data:
            return Comp.Image(file=f"base64://{base64.b64encode(data).decode()}")
        logger.debug(
            "[dcqq-relay] Discord 表情下载失败，回退为文本（%s / %s）", name, emoji_id
        )
        return Comp.Plain(f"[表情:{name}]")

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

    @staticmethod
    def _raw_media_segments(message_obj: Any) -> dict[str, list[dict]]:
        """从 OneBot 原始消息里取出 file/video/image/record 段，用于补全 file_id。"""
        out: dict[str, list[dict]] = {"file": [], "video": [], "image": [], "record": []}
        raw = getattr(message_obj, "raw_message", None)
        try:
            message = raw.get("message") if hasattr(raw, "get") else None
        except Exception:
            message = None
        if not isinstance(message, list):
            return out
        for item in message:
            if not isinstance(item, dict):
                continue
            kind = str(item.get("type") or "")
            if kind not in out:
                continue
            data = item.get("data")
            if isinstance(data, dict):
                out[kind].append(data)
        return out

    async def _build_qq_parts(self, message_obj: Any, relation: Relation) -> list[Any]:
        parts: list[Any] = []
        ignore_at_all = bool(self.config.get("ignore_at_all", True))
        raw_media = self._raw_media_segments(message_obj)
        queues: dict[str, list[dict]] = {key: list(value) for key, value in raw_media.items()}

        def take(kind: str) -> dict | None:
            queue = queues.get(kind) or []
            return queue.pop(0) if queue else None

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
                ref = await self._resolve_media_ref(
                    relation, seg, "image", take("image")
                )
                parts.append(
                    Comp.Image(file=ref, url=ref if ref.startswith("http") else "")
                )
            elif isinstance(seg, Comp.Record):
                ref = await self._resolve_media_ref(
                    relation, seg, "record", take("record")
                )
                suffix = os.path.splitext(guess_filename(ref, ""))[1] or ".amr"
                parts.append(Comp.File(name=f"语音{suffix}", url=ref))
            elif isinstance(seg, Comp.Video):
                ref = await self._resolve_media_ref(
                    relation, seg, "video", take("video")
                )
                parts.append(Comp.File(name="视频.mp4", url=ref))
            elif isinstance(seg, Comp.File):
                ref = await self._resolve_media_ref(
                    relation, seg, "file", take("file")
                )
                parts.append(Comp.File(name=seg.name or "文件", url=ref))
            elif isinstance(seg, Comp.Reply):
                mapped = False
                if self.store is not None:
                    rows = await self.store.by_forward(
                        relation.forward_platform,
                        relation.forward_channel,
                        str(seg.id),
                    )
                    if rows:
                        parts.append(Comp.Reply(id=rows[0].dc_id))
                        mapped = True
                if not mapped:
                    # 被回复的消息没有转发记录（QQ 原生消息），拉取原文做引用
                    quote = await self._qq_reply_quote(relation, str(seg.id))
                    if quote:
                        parts.append(Comp.Plain(quote))
            else:
                continue
        return parts

    async def _resolve_media_ref(
        self, relation: Relation, seg: Any, kind: str, raw: dict | None = None
    ) -> str:
        """把消息组件的资源引用解析成可直接下载的地址。

        有些 OneBot 实现（如部分 NapCat / go-cqhttp 配置）的图片段只有裸文件名，
        没有 http url，这时调用协议端的 get_image 换取真实链接；文件/视频段还会
        用原始消息里的 file_id 调 get_group_file_url / get_private_file_url / get_file。
        """
        candidates = DiscordSide._candidates(seg)
        if isinstance(raw, dict):
            for key in ("url", "file", "path"):
                value = raw.get(key)
                if isinstance(value, str) and value:
                    candidates.append(value)
        candidates = DiscordSide._uniq(candidates)
        for value in candidates:
            if value.startswith(("http://", "https://", "base64://", "data:", "file://")):
                return value
            try:
                if Path(uri_to_path(value)).is_file():
                    return value
            except OSError:
                continue

        if self.qq is None:
            return ""
        client = self.qq.client(relation.forward_platform)
        if client is None:
            return ""

        # 1) 图片 / 语音：走协议端专用接口
        ref = DiscordSide._first_ref(seg)
        action = {"image": "get_image", "record": "get_record"}.get(kind)
        if action and ref:
            try:
                ret = await client.call_action(action, file=ref)
            except Exception as exc:
                logger.debug("[dcqq-relay] %s 解析媒体失败（%s）：%s", action, ref[:80], exc)
                ret = None
            url = self._extract_media_url(ret)
            if url:
                logger.info("[dcqq-relay] 已通过 %s 解析出媒体链接（%s）", action, ref[:60])
                return url

        # 2) 文件 / 视频：用原始消息里的 file_id 换取下载地址
        if kind in ("file", "video") and isinstance(raw, dict):
            file_id = raw.get("file_id") or raw.get("fileId") or raw.get("file")
            group_id = raw.get("group_id") or relation.forward_channel
            if file_id:
                url = await self._resolve_file_url(client, str(group_id or ""), str(file_id), raw)
                if url:
                    return url
        return ""

    @staticmethod
    def _extract_media_url(ret: Any) -> str:
        if isinstance(ret, dict):
            for key in ("url", "file", "file_url"):
                value = ret.get(key)
                if isinstance(value, str) and value.startswith(("http://", "https://")):
                    return value
        return ""

    async def _resolve_file_url(
        self, client: Any, group_id: str, file_id: str, raw: dict
    ) -> str:
        busid = raw.get("busid") or 0
        attempts: list[tuple[str, dict]] = []
        if group_id:
            attempts.append(
                (
                    "get_group_file_url",
                    {"group_id": int(group_id) if group_id.isdigit() else group_id,
                     "file_id": file_id, "busid": busid},
                )
            )
        attempts.append(("get_private_file_url", {"file_id": file_id}))
        attempts.append(("get_file", {"file_id": file_id}))
        for action, params in attempts:
            try:
                ret = await client.call_action(action, **params)
            except Exception as exc:
                logger.debug("[dcqq-relay] %s 失败：%s", action, exc)
                continue
            url = self._extract_media_url(ret)
            if url:
                logger.info("[dcqq-relay] 已通过 %s 解析出文件链接（%s）", action, file_id[:60])
                return url
        return ""

    async def _qq_reply_quote(self, relation: Relation, message_id: str) -> str:
        """用 OneBot get_msg 拉取被回复消息的原文，生成引用文本。"""
        if self.qq is None or not message_id:
            return ""
        client = self.qq.client(relation.forward_platform)
        if client is None:
            return ""
        params: dict[str, Any] = {"message_id": message_id}
        if message_id.isdigit():
            params["message_id"] = int(message_id)
        try:
            ret = await client.call_action("get_msg", **params)
        except Exception as exc:
            logger.debug("[dcqq-relay] get_msg 失败（%s）：%s", message_id, exc)
            return ""
        if not isinstance(ret, dict):
            return ""
        sender = ret.get("sender") or {}
        name = ""
        if isinstance(sender, dict):
            name = str(sender.get("card") or sender.get("nickname") or "")
        text_parts: list[str] = []
        segments = ret.get("message")
        if isinstance(segments, list):
            for item in segments:
                if not isinstance(item, dict):
                    continue
                if item.get("type") == "text":
                    data = item.get("data") or {}
                    value = data.get("text")
                    if isinstance(value, str):
                        text_parts.append(value)
                elif item.get("type") == "at":
                    data = item.get("data") or {}
                    qq = data.get("qq")
                    if qq:
                        text_parts.append(f"@{qq}")
        text = "".join(text_parts).strip()
        if not text:
            return ""
        text = text.replace("\n", " ")
        if len(text) > 80:
            text = text[:80] + "…"
        name = name or "QQ 用户"
        return f"> 回复 {name}：{text}"

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
            if isinstance(part, Comp.Plain)
            and (part.text or "").strip()
            and not (part.text or "").strip().startswith(("@", ">"))
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
            if self.config.get("ignore_at_all", True):
                content = re.sub(r"@(everyone|here)\b", "", content)
            parts = await self._content_to_parts(content)
            parts.append(Comp.Plain("\n(edited)"))
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

    # -- 聊天绑定 -----------------------------------------------------------
    def _capture_endpoint(self, event: AstrMessageEvent) -> dict:
        """把当前会话抽象成一个可绑定的端点。"""
        message_obj = event.message_obj
        raw = message_obj.raw_message
        platform_name = event.get_platform_name()
        platform_id = event.get_platform_id() or platform_name
        session_id = str(getattr(message_obj, "session_id", "") or "")

        group = getattr(message_obj, "group", None)
        group_id = (
            str(getattr(group, "group_id", "") or "") if group is not None else ""
        )
        is_group = bool(group_id)
        channel = group_id or session_id

        channel_name = ""
        if group is not None and getattr(group, "group_name", None):
            channel_name = str(group.group_name)
        raw_channel = getattr(raw, "channel", None)
        if raw_channel is not None and getattr(raw_channel, "name", None):
            channel_name = channel_name or str(raw_channel.name)
        if not channel_name:
            channel_name = f"{platform_name}:{channel}"

        guild_id = ""
        guild = getattr(raw, "guild", None)
        if guild is not None:
            guild_id = str(getattr(guild, "id", "") or "")

        return {
            "platform_name": platform_name,
            "platform_id": str(platform_id),
            "channel": channel,
            "message_type": "GroupMessage" if is_group else "FriendMessage",
            "guild_id": guild_id,
            "channel_name": channel_name,
            "session_id": session_id,
        }

    @staticmethod
    def _endpoint_text(endpoint: dict) -> str:
        name = endpoint.get("channel_name") or endpoint.get("channel")
        return f"{endpoint.get('platform_name')} · {name}（{endpoint.get('channel')}）"

    def _find_relation_for_endpoint(self, endpoint: dict):
        platforms = {endpoint.get("platform_id"), endpoint.get("platform_name")}
        for relation in self.relations:
            if endpoint["platform_name"] == "discord":
                if relation.discord_channel_id == endpoint["channel"]:
                    return relation
            elif (
                relation.forward_platform in platforms
                and relation.forward_channel == endpoint["channel"]
            ):
                return relation
        return None

    def _prune_bind_codes(self) -> None:
        now = time.time()
        for code, entry in list(self._bind_codes.items()):
            if now - float(entry.get("created", 0)) > BIND_CODE_TTL:
                self._bind_codes.pop(code, None)

    def _new_bind_code(self) -> str:
        for _ in range(100):
            code = f"{random.randint(0, 999999):06d}"
            if code not in self._bind_codes:
                return code
        raise RuntimeError("无法生成绑定码")

    def _save_config_safe(self) -> bool:
        save = getattr(self.config, "save_config", None)
        if not callable(save):
            return False
        try:
            save()
            return True
        except Exception as exc:
            logger.warning("[dcqq-relay] 保存插件配置失败：%s", exc)
            return False

    @staticmethod
    def _relation_matches(relation: Relation, item: Any) -> bool:
        if not isinstance(item, dict):
            return False
        return (
            str(item.get("discord_channel_id") or "").strip()
            == relation.discord_channel_id
            and str(item.get("forward_channel") or "").strip()
            == relation.forward_channel
            and str(item.get("forward_platform") or "").strip()
            == relation.forward_platform
        )

    def _auto_relation_name(self, dc_endpoint: dict, other: dict) -> str:
        left = dc_endpoint.get("channel_name") or f"discord:{dc_endpoint['channel']}"
        right = other.get("channel_name") or f"{other['platform_name']}:{other['channel']}"
        return f"{left} ⇄ {right}"[:60]

    @filter.command("绑定", alias={"bind"})
    async def bind(self, event: AstrMessageEvent, args: GreedyStr):
        """生成绑定码，把当前会话与另一个频道/群互绑。"""
        if not self._is_admin(event):
            yield event.plain_result("只有管理员可以使用绑定功能。")
            return
        if not self._initialized:
            yield event.plain_result("插件尚未初始化完成，请稍后再试。")
            return

        endpoint = self._capture_endpoint(event)
        if not endpoint.get("channel"):
            yield event.plain_result("无法识别当前会话，绑定失败。")
            return

        existing = self._find_relation_for_endpoint(endpoint)
        if existing is not None:
            yield event.plain_result(
                f"当前会话已经在互通关系「{existing.name}」里了，无需重复绑定。"
            )
            return

        self._prune_bind_codes()
        code = self._new_bind_code()
        self._bind_codes[code] = {
            "endpoint": endpoint,
            "name": str(args or "").strip()[:40],
            "created": time.time(),
            "creator": str(event.get_sender_id()),
        }
        minutes = max(1, BIND_CODE_TTL // 60)
        logger.info("[dcqq-relay] 生成绑定码 %s（%s）", code, endpoint["channel"])
        yield event.plain_result(
            f"🔗 绑定码：{code}\n"
            f"当前会话：{self._endpoint_text(endpoint)}\n\n"
            f"请在 {minutes} 分钟内，到要互通的另一个频道/群发送：\n"
            f"/完成绑定 {code}\n"
            "（另一端同样需要管理员权限；其中一端必须是 Discord 频道）"
        )

    @filter.command("完成绑定", alias={"bindcode", "complete_bind"})
    async def complete_bind(self, event: AstrMessageEvent, args: GreedyStr):
        """使用 /绑定 生成的绑定码完成双向绑定。"""
        if not self._is_admin(event):
            yield event.plain_result("只有管理员可以使用绑定功能。")
            return
        if not self._initialized:
            yield event.plain_result("插件尚未初始化完成，请稍后再试。")
            return

        code = "".join(str(args or "").split())
        if not code or not code.isdigit():
            yield event.plain_result("用法：/完成绑定 <绑定码>")
            return

        self._prune_bind_codes()
        entry = self._bind_codes.get(code)
        if entry is None:
            yield event.plain_result("绑定码无效或已过期，请重新发送 /绑定 获取。")
            return

        first = entry["endpoint"]
        current = self._capture_endpoint(event)
        if (
            first["platform_name"] == current["platform_name"]
            and first["channel"] == current["channel"]
        ):
            yield event.plain_result("不能和同一个会话绑定，请到另一个频道/群完成绑定。")
            return

        dc_endpoints = [
            point
            for point in (first, current)
            if point["platform_name"] == "discord"
        ]
        if len(dc_endpoints) != 1:
            yield event.plain_result(
                "绑定需要其中一端是 Discord 频道（两端都不是 / 两端都是 Discord 暂不支持）。"
            )
            return
        dc_endpoint = dc_endpoints[0]
        other = current if dc_endpoint is first else first

        for relation in self.relations:
            if (
                relation.discord_channel_id == dc_endpoint["channel"]
                and relation.forward_channel == other["channel"]
                and relation.forward_platform
                in {other["platform_id"], other["platform_name"]}
            ):
                self._bind_codes.pop(code, None)
                yield event.plain_result(
                    f"这两个会话已经绑定过了（{relation.name}），无需重复绑定。"
                )
                return

        name = entry.get("name") or self._auto_relation_name(dc_endpoint, other)
        record = {
            "__template_key": "relation",
            "name": name,
            "discord_guild_id": dc_endpoint.get("guild_id", ""),
            "discord_channel_id": dc_endpoint["channel"],
            "forward_platform": other.get("platform_id") or other["platform_name"],
            "forward_channel": other["channel"],
            "forward_message_type": other["message_type"],
            "show_forward_id": True,
            "reverse_name": False,
            "download_asset": False,
        }
        relations = list(self.config.get("relations") or [])
        relations.append(record)
        self.config["relations"] = relations
        saved = self._save_config_safe()
        self.relations = parse_relations(relations)
        self._bind_codes.pop(code, None)
        logger.info(
            "[dcqq-relay] 聊天绑定完成：discord:%s ⇄ %s:%s",
            dc_endpoint["channel"],
            other["platform_name"],
            other["channel"],
        )

        text = (
            f"✅ 绑定完成：{name}\n"
            f"Discord：{self._endpoint_text(dc_endpoint)}\n"
            f"QQ 侧：{self._endpoint_text(other)}\n"
            "双向同步已生效。"
        )
        if not saved:
            text += (
                "\n⚠️ 配置保存失败：本次运行有效，重启后会丢失。"
                "请到 WebUI 手动把这条关系补进配置。"
            )
        yield event.plain_result(text)

    @filter.command("解绑", alias={"unbind"})
    async def unbind(self, event: AstrMessageEvent, args: GreedyStr):
        """解除一条互通关系（按 /relay_status 里的序号）。"""
        if not self._is_admin(event):
            yield event.plain_result("只有管理员可以使用解绑功能。")
            return

        raw = str(args or "").strip()
        if not raw.isdigit():
            lines = ["用法：/解绑 <序号>", "当前关系："]
            for index, relation in enumerate(self.relations, start=1):
                lines.append(
                    f"{index}. {relation.name}"
                    f"（discord:{relation.discord_channel_id} ⇄ "
                    f"{relation.forward_platform}:{relation.forward_channel}）"
                )
            if len(lines) == 2:
                lines.append("（暂无）")
            yield event.plain_result("\n".join(lines))
            return

        index = int(raw) - 1
        if index < 0 or index >= len(self.relations):
            yield event.plain_result(
                f"序号超出范围（当前共 {len(self.relations)} 条关系）。"
            )
            return

        target = self.relations[index]
        kept: list[Any] = []
        removed = False
        for item in list(self.config.get("relations") or []):
            if not removed and self._relation_matches(target, item):
                removed = True
                continue
            kept.append(item)
        if not removed:
            yield event.plain_result(
                "在配置里没有找到这条关系，可能已被 WebUI 修改，请手动检查配置。"
            )
            return

        self.config["relations"] = kept
        saved = self._save_config_safe()
        self.relations = parse_relations(kept)
        text = f"已解绑：{target.name}"
        if not saved:
            text += "\n⚠️ 配置保存失败：重启后会恢复，请到 WebUI 手动删除。"
        yield event.plain_result(text)

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
        for index, relation in enumerate(self.relations, start=1):
            ready = self._recovery_ready.get(relation.forward_platform)
            state = ""
            if ready is not None:
                state = "（已就绪）" if ready.is_set() else "（等待恢复）"
            lines.append(
                f"{index}. {relation.name}：discord:{relation.discord_channel_id} ⇄ "
                f"{relation.forward_platform}:{relation.forward_channel}"
                f"（{'群聊' if relation.is_group else '私聊'}）{state}"
            )
        if not self.relations:
            lines.append("（暂无，可用 /绑定 + /完成绑定 在聊天里创建）")
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
            wait = max(0, int(self.dc._webhook_retry_at - time.time()))
            webhook_state = (
                f"已回退为机器人身份（检查「管理 Webhook」权限，{wait} 秒后重试）"
            )
            if self.dc._webhook_error:
                webhook_state += f"\n  错误：{self.dc._webhook_error[:300]}"
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
            "把 Discord 频道和 QQ 群双向连起来。可以在 WebUI 插件设置里配置，"
            "也可以直接在聊天里绑定（一条关系即双向）。\n\n"
            "聊天绑定：\n"
            "/绑定 — 当前会话生成 6 位绑定码（管理员）\n"
            "/完成绑定 <绑定码> — 在另一个频道/群完成绑定（管理员）\n"
            "/解绑 <序号> — 解除一条互通关系（管理员）\n\n"
            "指令：\n"
            "/relay — 显示本帮助\n"
            "/relay_status — 查看关系、映射数量、翻译与 Webhook 状态\n"
            "/relay_recovery <开始时间> [平台ID|all] [--dry] — 恢复历史消息（管理员）\n\n"
            "原始 Koishi 插件：https://github.com/koishijs/koishi-plugin-dcqq-relay"
        )

