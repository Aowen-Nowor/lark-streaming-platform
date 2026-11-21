"""stdio 绑定 — 行分隔 JSON（01-protocol §3；托管桥接子进程形态）.

fd 拓扑（引擎进程视角；由 01-protocol §3「引擎 stderr 同格式回 ack」
锚定——acks 走 stderr 是唯一无歧义表述，入站事件 therefore 走 stdin，
stdout 保留给运行日志）：

- **stdin** ：收事件行（桥接/接入方进程写入）
- **stderr**：回 ack / error 行（与事件行同格式，逐行 JSON）
- **stdout**：保留给运行日志（不承载协议流量）

无握手（§3：进程即会话）：协议版本以进程参数等效传递（§6）——构造期
校验 ``protocol_v``，不支持即拒绝启动（等价于 WS 的协商失败关闭连接）。

回执时序（05-adapters §1 红线：接入方不得假设 ack 时序）：逐帧并发
提交、完成即回写——跨 chat 无队头阻塞；同 chat 的 ack 顺序由引擎
per-chat 单消费者保证与消费序一致。
"""

from __future__ import annotations

import asyncio
import json
import sys
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

from .messages import WireFrameError, wire_error, parse_frame

__all__ = ["StdioBinding", "serve_stdio", "MAX_STDIN_LINE_BYTES"]

EmitFn = Callable[[str], Awaitable[None]]

#: 逐行输入上限（与 StreamReader limit 一致；audit #4：线程泵路径修复前无上限——
# 恶意/失控生产者一行可内存击穿托管进程）
MAX_STDIN_LINE_BYTES = 8 * 1024 * 1024

#: 行超限哨兵（不会与合法 JSON 文本行撞——原始 NUL 不出现于 JSON 文本；
# serve 循环检测到即回 frame_too_large 错误行并继续——输入劣化 ≠ 进程故障）
_LINE_TOO_LARGE = "\x00__LSP_LINE_TOO_LARGE__"


async def _stderr_emit(text: str) -> None:
    """默认回写通道：stderr 逐行（小帧直写，flush 保证桥接侧实时可读）."""
    sys.stderr.write(text + "\n")
    sys.stderr.flush()


async def _stdin_lines() -> AsyncIterator[str]:
    """stdin 行源：pipe 直连优先；非 pipe 场景回退线程泵."""
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader(limit=8 * 1024 * 1024)  # 单帧上限 8MB
    try:
        protocol = asyncio.StreamReaderProtocol(reader)
        await loop.connect_read_pipe(lambda: protocol, sys.stdin)
    except Exception:  # noqa: BLE001 — tty/已挂接等场景回退线程泵
        async for line in _stdin_lines_via_thread():
            yield line
        return
    while True:
        try:
            line = await reader.readline()
        except ValueError:
            # 单帧超限（audit #4）：StreamReader 缓冲已清空，流继续——
            # 劣化输入回错误行而非进程停机（绑定层不可击穿）
            yield _LINE_TOO_LARGE
            continue
        if not line:
            return
        yield line.decode("utf-8", errors="replace")


async def _stdin_lines_via_thread() -> AsyncIterator[str]:
    import queue
    import threading

    q: queue.Queue = queue.Queue()
    too_large = _LINE_TOO_LARGE.encode()

    def _pump() -> None:
        try:
            eof = False
            while not eof:
                raw = sys.stdin.buffer.readline(MAX_STDIN_LINE_BYTES + 1)
                if not raw:
                    eof = True
                    break
                if len(raw) > MAX_STDIN_LINE_BYTES:
                    q.put(too_large)
                    # 丢弃该行剩余字节直到换行（泵不承载协议语义；防御性）
                    while not raw.endswith(b"\n"):
                        raw = sys.stdin.buffer.readline(MAX_STDIN_LINE_BYTES)
                        if not raw:
                            eof = True  # EOF 截断：不 return——出口统一 put(None)
                            break
                    continue
                q.put(raw)
        except Exception:  # noqa: BLE001 — stdin 关闭/不可读
            pass
        q.put(None)

    threading.Thread(target=_pump, daemon=True).start()
    while True:
        item = await asyncio.to_thread(q.get)
        if item is None:
            return
        yield item.decode("utf-8", errors="replace")


class StdioBinding:
    """stdio 绑定：stdin 事件行 → 引擎 → stderr 回执行."""

    def __init__(
        self,
        engine: Any,
        *,
        protocol_v: int = 1,
        settings: Any = None,
    ) -> None:
        # 进程参数等效握手（01-protocol §6；不支持即拒绝启动）
        if protocol_v != 1:
            raise ValueError(f"stdio binding: unsupported protocol_v {protocol_v}")
        self._engine = engine
        self._settings = settings

    async def serve(
        self,
        *,
        lines: AsyncIterator[str] | None = None,
        emit: EmitFn | None = None,
    ) -> int:
        """消费事件行直到输入耗尽；返回受理的行数（含错误行）.

        ``lines``/``emit`` 可注入（测试台架）；缺省 = stdin / stderr。
        输入耗尽后等待全部在途回执落定（优雅排空）。
        """
        emit = emit or _stderr_emit
        send_lock = asyncio.Lock()
        pending: set[asyncio.Task[None]] = set()
        count = 0

        async for line in (lines if lines is not None else _stdin_lines()):
            stripped = line.strip()
            if not stripped:
                continue
            count += 1
            if stripped == _LINE_TOO_LARGE:
                # 单帧超限（audit #4）：回错误行并继续——输入劣化不是进程
                # 故障（修复前 readline ValueError 一帧带停全进程）
                await self._send(emit, send_lock, wire_error("frame_too_large"))
                continue
            task = asyncio.create_task(self._handle_line(stripped, emit, send_lock))
            pending.add(task)
            task.add_done_callback(pending.discard)
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        return count

    async def _handle_line(self, line: str, emit: EmitFn, lock: asyncio.Lock) -> None:
        try:
            raw = parse_frame(line)
        except WireFrameError as e:
            await self._send(emit, lock, wire_error(e.detail, e.frame_id))
            return
        try:
            ack = await self._engine.submit(raw)
            wire = ack.to_protocol()
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 — 绑定层兜底（引擎自身保证失败必回执）
            wire = wire_error("binding_internal_error")
        await self._send(emit, lock, wire)

    @staticmethod
    async def _send(emit: EmitFn, lock: asyncio.Lock, payload: dict[str, Any]) -> None:
        async with lock:
            await emit(json.dumps(payload, ensure_ascii=False))


async def serve_stdio(engine: Any, **kw: Any) -> int:
    """便捷入口：stdin/stderr 直连运行 stdio 绑定（Phase 4 网关 shell 复用）."""
    return await StdioBinding(engine, **kw).serve()
