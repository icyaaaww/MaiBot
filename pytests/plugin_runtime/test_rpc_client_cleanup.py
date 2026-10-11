"""RPC 连接退出与请求取消的回归；仅隔离日志初始化，使用真实 TCP 传输。"""

from unittest.mock import patch

import asyncio
import logging
import sys
import types
import unittest

logger_module = types.ModuleType("src.common.logger")
logger_module.get_logger = logging.getLogger
with patch.dict(sys.modules, {"src.common.logger": logger_module}):
    from src.plugin_runtime.protocol.codec import MsgPackCodec
    from src.plugin_runtime.protocol.errors import RPCError
    from src.plugin_runtime.runner.rpc_client import RPCClient
    from src.plugin_runtime.transport.base import Connection


class RPCClientCleanupTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.received = asyncio.Event()
        self.release = asyncio.Event()
        self.respond = False
        self.peers = set()

        async def peer(reader, writer):
            task = asyncio.current_task()
            self.peers.add(task)
            connection = Connection(reader, writer)
            try:
                codec = MsgPackCodec()
                request = codec.decode_envelope(await connection.recv_frame())
                self.received.set()
                await self.release.wait()
                if self.respond:
                    await connection.send_frame(codec.encode_envelope(request.make_response({"ok": True})))
            finally:
                await connection.close()
                self.peers.discard(task)

        self.server = await asyncio.start_server(peer, "127.0.0.1", 0)
        reader, writer = await asyncio.open_connection("127.0.0.1", self.server.sockets[0].getsockname()[1])
        self.client = RPCClient("unused", "unused")
        self.client._connection = Connection(reader, writer)
        self.client._running = True
        self.client._recv_task = asyncio.create_task(self.client._recv_loop())

    async def asyncTearDown(self):
        self.release.set()
        await self.client.disconnect()
        if self.peers:
            await asyncio.gather(*self.peers, return_exceptions=True)
        self.server.close()
        await self.server.wait_closed()

    async def test_remote_close_fails_pending_without_waiting_for_timeout(self):
        request = asyncio.create_task(self.client.send_request("probe", timeout_ms=30000))
        await asyncio.wait_for(self.received.wait(), 2)
        self.release.set()
        with self.assertRaises(RPCError):
            await asyncio.wait_for(request, 2)
        self.assertFalse(self.client.is_connected)
        self.assertEqual(self.client._pending_requests, {})
        with self.assertRaises(RPCError):
            await self.client.send_request("after-close")
        await self.client.disconnect()
        await self.client.disconnect()

    async def test_cancel_removes_pending_without_closing_connection(self):
        request = asyncio.create_task(self.client.send_request("probe"))
        await asyncio.wait_for(self.received.wait(), 2)
        request.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await request
        self.assertEqual(self.client._pending_requests, {})
        self.assertTrue(self.client.is_connected)

    async def test_normal_response_survives_peer_close(self):
        self.respond = True
        request = asyncio.create_task(self.client.send_request("probe"))
        await asyncio.wait_for(self.received.wait(), 2)
        self.release.set()
        response = await asyncio.wait_for(request, 2)
        self.assertEqual(response.payload, {"ok": True})
        self.assertEqual(self.client._pending_requests, {})

    async def test_explicit_disconnect_wakes_pending_request(self):
        request = asyncio.create_task(self.client.send_request("probe"))
        await asyncio.wait_for(self.received.wait(), 2)
        await self.client.disconnect()
        with self.assertRaises(RPCError):
            await request
        self.assertFalse(self.client.is_connected)


if __name__ == "__main__":
    unittest.main()
