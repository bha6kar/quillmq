"""TLS client example.

First generate a self-signed cert and start the broker with TLS:

    openssl req -x509 -newkey rsa:2048 -nodes -keyout key.pem -out cert.pem \
        -days 365 -subj "/CN=127.0.0.1" \
        -addext "subjectAltName=IP:127.0.0.1"
    uv run quillmq serve --port 5556 --tls-cert cert.pem --tls-key key.pem

Then run this client, which trusts that certificate and connects over TLS
using a quills:// URL.
"""

import asyncio
import ssl

from quillmq import connect


async def main() -> None:
    # Trust the broker's self-signed certificate. In production you would trust
    # a real CA and skip this, letting the system trust store verify the server.
    ctx = ssl.create_default_context()
    ctx.load_verify_locations("cert.pem")

    conn = await connect("quills://127.0.0.1:5556", ssl_context=ctx)
    ch = await conn.channel()
    await ch.declare_queue("secure", durable=False)
    await ch.publish("", "secure", {"over": "tls"})
    async for msg in ch.consume("secure", auto_ack=True):
        print("received over TLS:", msg.body)
        break
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
