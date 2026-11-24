"""SMTP 测试中继。

- SMTP 端口：模拟 220 问候（多行、分段发送）与 NOOP/ EHLO/ QUIT 应答。
- 控制端口：极简 HTTP，POST /config 动态调整行为，GET /state 查看配置与计数。

可配项（POST /config，JSON）：
  greeting_delay  发送问候前的等待秒数
  response_delay  每条命令应答前的等待秒数
  reset           true 时建连后立即以 RST 断开（模拟对端失败）
"""
import asyncio
import json
import os
import socket
import struct
import time

NAME = os.environ.get("RELAY_NAME", "relay")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "2525"))
CTRL_PORT = int(os.environ.get("CTRL_PORT", "8081"))

STATE = {
    "greeting_delay": 0.2,
    "response_delay": 0.1,
    "reset": False,
}
STATS = {
    "accepted": 0,   # 接受的 TCP 连接数
    "reset": 0,      # 主动 RST 的次数
    "greeted": 0,    # 完整发出问候的次数
    "noop": 0,       # 收到 NOOP 的次数
    "closed": 0,     # 会话结束（连接释放）次数
}


DISCONNECT_POLL = 0.05  # 断线检测粒度：与问候分段间隔一致，足够"随即"察觉


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] [{NAME}] {msg}", flush=True)


async def _sleep_unless_eof(reader: asyncio.StreamReader, delay: float) -> bool:
    """睡眠 delay 秒；期间对端断开（FIN/RST，reader 收到 EOF）则提前醒来。

    返回 True 表示睡满且连接仍在；False 表示对端已断开，调用方应取消
    尚未发出的问候/应答并随即收尾。只读本连接的 reader，不影响其他会话。
    """
    loop = asyncio.get_running_loop()
    end = loop.time() + max(0.0, delay)
    while True:
        if reader.at_eof():
            return False
        remaining = end - loop.time()
        if remaining <= 0:
            return True
        await asyncio.sleep(min(DISCONNECT_POLL, remaining))


def _reset_connection(writer: asyncio.StreamWriter) -> None:
    """SO_LINGER(1,0) + close，让对端收到 RST，模拟建连即失败。"""
    sock = writer.get_extra_info("socket")
    if sock is not None:
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                            struct.pack("ii", 1, 0))
        except OSError:
            pass
    writer.close()


async def smtp_session(reader: asyncio.StreamReader,
                       writer: asyncio.StreamWriter) -> None:
    peer = writer.get_extra_info("peername")
    STATS["accepted"] += 1
    log(f"接受连接 {peer}")
    try:
        if STATE.get("reset"):
            STATS["reset"] += 1
            log("按配置立即复位连接 (RST)")
            _reset_connection(writer)
            return

        # 问候前延迟：期间对端断开则取消未发出的问候，随即收尾
        if not await _sleep_unless_eof(reader,
                                       float(STATE.get("greeting_delay", 0.0))):
            log("对端在问候发出前断开，取消问候")
            return

        # 多行 220 问候，故意拆成多个 TCP 分段、段间留小间隔，
        # 探测端必须能跨分段、跨行收齐。段间对端断开则取消剩余分段。
        payload = (
            f"220-{NAME}.test ESMTP TestRelay\r\n"
            f"220-PIPELINING\r\n"
            f"220 {NAME}.test ready\r\n"
        ).encode()
        n = len(payload)
        bounds = [0, n // 4, n // 2, (3 * n) // 4, n]
        chunks = [payload[a:b] for a, b in zip(bounds, bounds[1:])]
        for i, chunk in enumerate(chunks):
            writer.write(chunk)
            await writer.drain()
            if i < len(chunks) - 1:  # 段间间隔，最后一段发出后不再空等
                if not await _sleep_unless_eof(reader, 0.05):
                    log("对端在问候分段间断开，取消剩余问候分段")
                    return
        STATS["greeted"] += 1
        log("已发送分段多行 220 问候")

        while True:
            line = await reader.readline()
            if not line:
                break
            cmd = line.decode("utf-8", "replace").strip()
            if not cmd:
                continue
            verb = cmd.split()[0].upper()
            if verb == "NOOP":
                # 读到完整 NOOP 即计数，即使随后对端断开、没机会回 250
                STATS["noop"] += 1
            # 应答前延迟：期间对端断开则取消本次应答，随即收尾
            if not await _sleep_unless_eof(reader,
                                           float(STATE.get("response_delay", 0.0))):
                log(f"对端在应答 {verb} 前断开，取消应答")
                return
            if verb == "NOOP":
                writer.write(b"250 OK\r\n")
            elif verb in ("EHLO", "HELO"):
                writer.write(f"250-{NAME}.test\r\n250 PIPELINING\r\n".encode())
            elif verb == "QUIT":
                writer.write(b"221 Bye\r\n")
                await writer.drain()
                break
            else:
                writer.write(b"502 Command not implemented\r\n")
            await writer.drain()
    except (ConnectionResetError, BrokenPipeError) as e:
        log(f"连接异常: {type(e).__name__}: {e}")
    except Exception as e:  # noqa: BLE001 - 测试中继不让单会话拖垮服务
        log(f"会话错误: {type(e).__name__}: {e}")
    finally:
        STATS["closed"] += 1
        try:
            writer.close()
        except Exception:
            pass
        log(f"连接释放 {peer}")


async def _send_json(writer: asyncio.StreamWriter, status: int, obj: dict) -> None:
    reason = {200: "OK", 400: "Bad Request", 404: "Not Found"}.get(status, "OK")
    body = json.dumps(obj).encode()
    writer.write(
        f"HTTP/1.1 {status} {reason}\r\n"
        f"Content-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\n"
        f"Connection: close\r\n\r\n".encode() + body
    )
    await writer.drain()


async def ctrl_session(reader: asyncio.StreamReader,
                       writer: asyncio.StreamWriter) -> None:
    try:
        head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=5)
        request_line, *header_lines = head.decode("iso-8859-1").split("\r\n")
        method, path, _ = request_line.split(" ", 2)
        headers = {}
        for hl in header_lines:
            if ":" in hl:
                k, v = hl.split(":", 1)
                headers[k.strip().lower()] = v.strip()
        body = b""
        clen = int(headers.get("content-length", "0") or "0")
        if clen:
            body = await reader.readexactly(clen)

        if method == "POST" and path == "/config":
            data = json.loads(body.decode() or "{}")
            for k in ("greeting_delay", "response_delay"):
                if k in data:
                    STATE[k] = max(0.0, float(data[k]))
            if "reset" in data:
                STATE["reset"] = bool(data["reset"])
            log(f"配置更新: {STATE}")
            await _send_json(writer, 200, {"ok": True, "state": STATE})
        elif method == "GET" and path == "/state":
            await _send_json(writer, 200,
                             {"name": NAME, "state": STATE, "stats": STATS})
        elif method == "GET" and path == "/healthz":
            await _send_json(writer, 200, {"ok": True})
        else:
            await _send_json(writer, 404, {"error": "not found"})
    except Exception as e:  # noqa: BLE001
        try:
            await _send_json(writer, 400, {"error": f"{type(e).__name__}: {e}"})
        except Exception:
            pass
    finally:
        try:
            writer.close()
        except Exception:
            pass


async def main() -> None:
    smtp_server = await asyncio.start_server(smtp_session, "0.0.0.0", SMTP_PORT)
    ctrl_server = await asyncio.start_server(ctrl_session, "0.0.0.0", CTRL_PORT)
    log(f"SMTP 监听 :{SMTP_PORT}，控制口 :{CTRL_PORT}")
    async with smtp_server, ctrl_server:
        await asyncio.gather(smtp_server.serve_forever(),
                             ctrl_server.serve_forever())


if __name__ == "__main__":
    asyncio.run(main())
