package io.suboptimal.nettymultiprotocol.http3;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http3.DefaultHttp3DataFrame;
import io.netty.handler.codec.http3.DefaultHttp3Headers;
import io.netty.handler.codec.http3.DefaultHttp3HeadersFrame;
import io.netty.handler.codec.http3.Http3FrameToHttpObjectCodec;
import io.netty.handler.codec.http3.Http3HeadersFrame;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.AsciiString;
import io.netty.util.ReferenceCountUtil;
import io.suboptimal.nettymultiprotocol.AppChannelConfigurer;
import io.suboptimal.nettymultiprotocol.AppProtocol;
import io.suboptimal.nettymultiprotocol.AppProtocolRegistry;

import java.nio.charset.StandardCharsets;

/**
 * Dispatch handler for HTTP/3 request stream channels ({@code QuicStreamChannel}).
 *
 * <p>Intercepts the first {@link Http3HeadersFrame}, extracts the {@code :path}
 * pseudo-header, and resolves the {@link AppProtocol} via
 * {@link AppProtocolRegistry#resolve}. Pipeline configuration is then delegated
 * to the matching {@link AppChannelConfigurer}:
 * <ol>
 *   <li>{@link AppProtocol#http3()} - native HTTP/3 path using raw stream frames</li>
 *   <li>{@link AppProtocol#http1()} - fallback: installs
 *       {@link Http3FrameToHttpObjectCodec} to convert frames to
 *       HTTP objects, then delegates to the HTTP/1.1 configurer</li>
 * </ol>
 *
 * <p>After the configurer sets up the pipeline, this handler removes itself and
 * re-fires the headers frame into the newly configured pipeline.
 *
 * <p>Error responses are sent as HTTP/3 HEADERS and DATA frames, then the stream
 * output is shut down via {@code QuicStreamChannel.shutdownOutput()}:
 * <ul>
 *   <li>404 - no protocol registered for the request path</li>
 *   <li>505 - protocol found but supports neither HTTP/3 nor HTTP/1.1</li>
 * </ul>
 *
 * <p>This handler is currently not wired by the library — users construct it
 * manually and pass it as the request-stream handler to
 * {@code Http3ServerConnectionHandler}. See {@link AppProtocolRegistry} for
 * routing semantics.
 */
public class Http3StreamDispatchHandler extends SimpleChannelInboundHandler<Http3HeadersFrame> {
    private static final AsciiString TYPE_UTF8 = AsciiString.cached("text/plain; charset=utf-8");

    private static final ChannelFutureListener SHUTDOWN_OUTPUT_IF_QUIC = future -> {
        if (future.channel() instanceof QuicStreamChannel qsc) {
            qsc.shutdownOutput();
        }
    };

    private final AppProtocolRegistry registry;

    public Http3StreamDispatchHandler(AppProtocolRegistry registry) {
        super(false);
        this.registry = registry;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Http3HeadersFrame headers) {
        CharSequence path = headers.headers().path();
        AppProtocol protocol = path == null ? null : registry.resolve(path.toString());
        if (protocol == null) {
            sendError(ctx, HttpResponseStatus.NOT_FOUND, "No protocol registered for path: " + path);
            ReferenceCountUtil.release(headers);
            return;
        }

        AppChannelConfigurer configurer = protocol.http3();
        if (configurer == null) {
            configurer = protocol.http1();

            if (configurer == null) {
                sendError(ctx, HttpResponseStatus.HTTP_VERSION_NOT_SUPPORTED,
                    "Protocol at this path does not support HTTP/3");
                ReferenceCountUtil.release(headers);
                return;
            }

            ctx.pipeline().addLast(new Http3FrameToHttpObjectCodec(true));
        }

        configurer.configure(ctx.channel());

        ctx.pipeline().remove(this);

        ctx.fireChannelRead(headers);
    }

    private void sendError(ChannelHandlerContext ctx, HttpResponseStatus status, String message) {
        ByteBuf body = ctx.alloc().buffer();
        body.writeBytes(message.getBytes(StandardCharsets.UTF_8));

        var headers = new DefaultHttp3Headers()
            .status(status.codeAsText())
            .set(HttpHeaderNames.CONTENT_TYPE, TYPE_UTF8)
            .set(HttpHeaderNames.CONTENT_LENGTH, String.valueOf(body.readableBytes()));

        ctx.write(new DefaultHttp3HeadersFrame(headers));
        ctx.writeAndFlush(new DefaultHttp3DataFrame(body))
            .addListener(SHUTDOWN_OUTPUT_IF_QUIC);
    }
}
