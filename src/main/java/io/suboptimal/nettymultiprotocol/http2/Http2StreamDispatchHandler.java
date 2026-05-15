package io.suboptimal.nettymultiprotocol.http2;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.handler.codec.http2.Http2StreamFrameToHttpObjectCodec;
import io.netty.util.AsciiString;
import io.netty.util.ReferenceCountUtil;
import io.suboptimal.nettymultiprotocol.AppChannelConfigurer;
import io.suboptimal.nettymultiprotocol.AppProtocol;
import io.suboptimal.nettymultiprotocol.AppProtocolRegistry;

import java.nio.charset.StandardCharsets;

/**
 * Dispatch handler for HTTP/2 stream child channels.
 *
 * <p>Intercepts the first {@link Http2HeadersFrame}, extracts the {@code :path}
 * pseudo-header, and resolves the {@link AppProtocol} via
 * {@link AppProtocolRegistry#resolve}. Pipeline configuration is then delegated
 * to the matching {@link AppChannelConfigurer}:
 * <ol>
 *   <li>{@link AppProtocol#http2()} - native HTTP/2 path using raw stream frames</li>
 *   <li>{@link AppProtocol#http1()} - fallback: installs
 *       {@link Http2StreamFrameToHttpObjectCodec} to convert stream frames to
 *       HTTP objects, then delegates to the HTTP/1.1 configurer</li>
 * </ol>
 *
 * <p>After the configurer sets up the pipeline, this handler removes itself and
 * re-fires the headers frame into the newly configured pipeline.
 *
 * <p>Error responses are sent as HTTP/2 HEADERS and DATA frames:
 * <ul>
 *   <li>404 - no protocol registered for the request path</li>
 *   <li>505 - protocol found but supports neither HTTP/2 nor HTTP/1.1</li>
 * </ul>
 */
public class Http2StreamDispatchHandler extends SimpleChannelInboundHandler<Http2HeadersFrame> {
    private static final AsciiString TYPE_UTF8 = AsciiString.cached("text/plain; charset=utf-8");

    private final AppProtocolRegistry registry;

    public Http2StreamDispatchHandler(AppProtocolRegistry registry) {
        super(false);
        this.registry = registry;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Http2HeadersFrame headers) {
        String path = headers.headers().path().toString();

        AppProtocol protocol = registry.resolve(path);
        if (protocol == null) {
            sendError(ctx, HttpResponseStatus.NOT_FOUND, "No protocol registered for path: " + path);
            ReferenceCountUtil.release(headers);
            return;
        }

        AppChannelConfigurer configurer = protocol.http2();
        if (configurer == null) {
            configurer = protocol.http1();

            if (configurer == null) {
                sendError(ctx, HttpResponseStatus.HTTP_VERSION_NOT_SUPPORTED,
                        "Protocol at this path does not support HTTP/2");
                ReferenceCountUtil.release(headers);
                return;
            }

            ctx.pipeline().addLast(new Http2StreamFrameToHttpObjectCodec(true));
        }

        configurer.configure(ctx.channel());

        ctx.pipeline().remove(this);

        ctx.fireChannelRead(headers);
    }

    private void sendError(ChannelHandlerContext ctx, HttpResponseStatus status, String message) {
        ByteBuf body = ctx.alloc().buffer();
        body.writeBytes(message.getBytes(StandardCharsets.UTF_8));

        var headers = new DefaultHttp2Headers()
                .status(status.codeAsText())
                .set(HttpHeaderNames.CONTENT_TYPE, TYPE_UTF8)
                .set(HttpHeaderNames.CONTENT_LENGTH, String.valueOf(body.readableBytes()));

        ctx.write(new DefaultHttp2HeadersFrame(headers, false));
        ctx.writeAndFlush(new DefaultHttp2DataFrame(body, true));
    }
}
