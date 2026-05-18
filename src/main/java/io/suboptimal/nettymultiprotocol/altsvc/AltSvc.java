package io.suboptimal.nettymultiprotocol.altsvc;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.util.AsciiString;

/**
 * Factories for outbound handlers that advertise an alternative service via the
 * {@code Alt-Svc} response header (<a href="https://datatracker.ietf.org/doc/html/rfc7838">RFC 7838</a>).
 *
 * <p>The canonical use case is advertising an HTTP/3 endpoint to clients connected
 * over HTTP/1.1 or HTTP/2 so they can switch transports on their next request.
 *
 * <p>Two variants are provided, one per HTTP transport this library serves over TCP:
 * <ul>
 *   <li>{@link #forHttp1(CharSequence)} — install via
 *       {@link io.suboptimal.nettymultiprotocol.NettyMultiprotocol.Builder#onHttp1ChannelConfigured}.</li>
 *   <li>{@link #forHttp2Stream(CharSequence)} — install via
 *       {@link io.suboptimal.nettymultiprotocol.NettyMultiprotocol.Builder#onHttp2StreamChannelConfigured}
 *       (the <b>stream</b> customizer; response {@code HEADERS} frames travel on stream child
 *       channels, not the parent).</li>
 * </ul>
 *
 * <p>Both handlers are {@link ChannelHandler.Sharable}: a single instance can be reused
 * across many pipelines, provided the supplied {@code value} is immutable (a {@link String}
 * or {@link AsciiString}; do not pass a {@link StringBuilder}).
 *
 * <h2>Shared-instance usage</h2>
 *
 * Build the handlers once and add the same instance from each customizer:
 *
 * <pre>{@code
 * CharSequence altSvc = "h3=\":443\"; ma=86400";
 * ChannelHandler http1AltSvc = AltSvc.forHttp1(altSvc);
 * ChannelHandler http2AltSvc = AltSvc.forHttp2Stream(altSvc);
 *
 * NettyMultiprotocol.builder()
 *     .sslContext(sslContext)
 *     .registry(registry)
 *     .onHttp1ChannelConfigured(p -> p.addLast(http1AltSvc))
 *     .onHttp2StreamChannelConfigured(p -> p.addLast(http2AltSvc))
 *     .build();
 * }</pre>
 *
 * <p>Equivalently, you may inline the factory call inside the customizer to produce
 * a fresh instance per channel — both patterns are supported.
 *
 * <h2>What to put in the value</h2>
 *
 * In practice servers advertise only {@code h3} (not {@code h2}): an HTTP/1.1 or HTTP/2
 * client connected over TLS already had a chance to negotiate {@code h2} via ALPN, so
 * advertising it again is redundant. A typical value is
 * {@code "h3=\":443\"; ma=86400"} — the port the UDP listener is bound to, plus a
 * cache lifetime in seconds.
 */
public final class AltSvc {
    private AltSvc() {
    }

    /**
     * Outbound handler for the HTTP/1.1 pipeline that adds an {@code Alt-Svc}
     * header to every {@link HttpResponse} it sees. Install via
     * {@link io.suboptimal.nettymultiprotocol.NettyMultiprotocol.Builder#onHttp1ChannelConfigured}.
     *
     * <p>The returned handler is {@link ChannelHandler.Sharable} — one instance can be
     * reused across many pipelines, provided {@code value} is immutable (e.g. a
     * {@link String} or {@link AsciiString}). Equivalently, you may call this factory
     * inside the customizer to produce a fresh instance per channel.
     *
     * @param value the Alt-Svc field value, e.g. {@code "h3=\":443\"; ma=86400"}
     */
    public static ChannelHandler forHttp1(CharSequence value) {
        return new Http1AltSvcHandler(value);
    }

    /**
     * Outbound handler for an HTTP/2 stream child channel that adds an {@code alt-svc}
     * header to every response {@link Http2HeadersFrame} (i.e. one with the {@code :status}
     * pseudo-header set). Trailers are not modified. Install via
     * {@link io.suboptimal.nettymultiprotocol.NettyMultiprotocol.Builder#onHttp2StreamChannelConfigured}.
     *
     * <p>The returned handler is {@link ChannelHandler.Sharable} — one instance can be
     * reused across many pipelines, provided {@code value} is immutable (e.g. a
     * {@link String} or {@link AsciiString}). Equivalently, you may call this factory
     * inside the customizer to produce a fresh instance per channel.
     *
     * @param value the Alt-Svc field value, e.g. {@code "h3=\":443\"; ma=86400"}
     */
    public static ChannelHandler forHttp2Stream(CharSequence value) {
        return new Http2StreamAltSvcHandler(value);
    }

    @ChannelHandler.Sharable
    private static final class Http1AltSvcHandler extends ChannelOutboundHandlerAdapter {
        private final CharSequence value;

        Http1AltSvcHandler(CharSequence value) {
            this.value = value;
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
            if (msg instanceof HttpResponse response) {
                response.headers().add(HttpHeaderNames.ALT_SVC, value);
            }
            ctx.write(msg, promise);
        }
    }

    @ChannelHandler.Sharable
    private static final class Http2StreamAltSvcHandler extends ChannelOutboundHandlerAdapter {
        private final CharSequence value;

        Http2StreamAltSvcHandler(CharSequence value) {
            this.value = value;
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
            if (msg instanceof Http2HeadersFrame frame && frame.headers().status() != null) {
                frame.headers().add(HttpHeaderNames.ALT_SVC, value);
            }
            ctx.write(msg, promise);
        }
    }
}
