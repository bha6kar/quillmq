import ssl

import pytest
import trustme

from quillmq import connect
from quillmq.broker import Broker
from quillmq.server import BrokerServer


def _server_context(ca: trustme.CA) -> ssl.SSLContext:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ca.issue_cert("127.0.0.1").configure_cert(ctx)
    return ctx


async def test_tls_roundtrip_with_trusted_ca():
    ca = trustme.CA()
    server = BrokerServer(
        Broker(), host="127.0.0.1", port=0, ssl_context=_server_context(ca)
    )
    await server.start()
    try:
        client_ctx = ssl.create_default_context()
        ca.configure_trust(client_ctx)
        conn = await connect(
            f"quills://127.0.0.1:{server.port}", ssl_context=client_ctx
        )
        ch = await conn.channel()
        await ch.declare_queue("q", durable=False)
        await ch.publish("", "q", {"n": 1})
        async for msg in ch.consume("q", auto_ack=True):
            assert msg.body == {"n": 1}
            break
        await conn.close()
    finally:
        await server.stop()


async def test_quills_scheme_rejects_untrusted_cert():
    # A quills:// URL with no explicit context uses the system trust store,
    # which must reject the self-signed test certificate.
    ca = trustme.CA()
    server = BrokerServer(
        Broker(), host="127.0.0.1", port=0, ssl_context=_server_context(ca)
    )
    await server.start()
    try:
        with pytest.raises(ssl.SSLError):
            await connect(f"quills://127.0.0.1:{server.port}")
    finally:
        await server.stop()
