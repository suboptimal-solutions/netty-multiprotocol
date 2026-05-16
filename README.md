# netty-multiprotocol

[![Maven Central](https://img.shields.io/maven-central/v/io.github.suboptimal-solutions/netty-multiprotocol.svg)](https://central.sonatype.com/artifact/io.github.suboptimal-solutions/netty-multiprotocol)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)

Multiplex multiple application-layer protocols (Connect, Twirp, REST, custom)
on a single Netty server port. Handles transport negotiation (TLS+ALPN, H2C
upgrade and prior knowledge) and routes each connection or HTTP/2 stream to the
right protocol implementation by URI pattern.

## At a glance

- Single port for HTTP/1.1, H2C, and HTTP/2-over-TLS.
- URI-pattern routing (exact, path-prefix, default) following Servlet 3.1 semantics.
- Per-protocol pipeline configuration via `AppChannelConfigurer`.
- Optional `ChannelPipeline` customizers for cross-cutting handlers
  (access log, metrics, idle-state timeouts, request id) that persist across
  HTTP/1.1 keep-alive requests.
- No reflection, no annotations, no framework lock-in - just a `ChannelInitializer`
  you wire into your own `ServerBootstrap`.

## Usage

```java
AppProtocolRegistry registry = new AppProtocolRegistry();
registry.register("/twirp/*", new TwirpProtocol(...));
registry.register("/", new RestProtocol(...));

ChannelInitializer<Channel> initializer = NettyMultiprotocol.builder()
    .sslContext(sslContext)          // optional; plaintext if null
    .registry(registry)
    .onHttp1ChannelConfigured(p ->
        p.addLast("accessLog", new MyAccessLogHandler()))
    .build();

new ServerBootstrap()
    .group(boss, worker)
    .channel(NioServerSocketChannel.class)
    .childHandler(initializer)
    .bind(port).sync();
```

## How transport negotiation works

All accepted channels start in a small negotiation pipeline before the library
installs the HTTP transport handlers.

With TLS, Netty's ALPN result selects HTTP/2 or HTTP/1.1. HTTP/2 channels use a
frame codec plus a multiplex handler; each inbound stream gets its own child
channel and is dispatched independently.

With plaintext, the H2C negotiator peeks at the first bytes for the HTTP/2
prior-knowledge preface. If it is present, the channel is configured as HTTP/2.
Otherwise the channel is configured as HTTP/1.1 with H2C upgrade support, so an
`Upgrade: h2c` request can switch the connection to HTTP/2.

After the transport is selected, `AppProtocolRegistry` resolves the request URI
using exact match, longest path-prefix match, then the default `/` pattern. The
matched `AppProtocol` supplies the `AppChannelConfigurer` for HTTP/1.1 or for
the individual HTTP/2 child stream channel.

## Development

```powershell
.\mvnw.cmd -q test
```

On Unix-like systems, use `./mvnw -q test`.

## License

Apache 2.0
