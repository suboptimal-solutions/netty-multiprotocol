# netty-multiprotocol

`netty-multiprotocol` is a Netty library for configuring a single server port that can route HTTP/1.1, H2C, and HTTP/2 over TLS with ALPN to application protocol handlers selected by URI pattern.

## Build

```powershell
.\mvnw.cmd -q test
```

On Unix-like systems, use `./mvnw -q test`.
