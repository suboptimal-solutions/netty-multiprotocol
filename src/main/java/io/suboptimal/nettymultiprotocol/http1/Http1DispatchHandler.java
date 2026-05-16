package io.suboptimal.nettymultiprotocol.http1;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.AsciiString;
import io.netty.util.ReferenceCountUtil;
import io.suboptimal.nettymultiprotocol.AppChannelConfigurer;
import io.suboptimal.nettymultiprotocol.AppProtocol;
import io.suboptimal.nettymultiprotocol.AppProtocolRegistry;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Dispatch handler for HTTP/1.1 pipelines.
 *
 * <p>Stays in the pipeline for the lifetime of the connection and routes each
 * incoming {@link HttpRequest} via {@link AppProtocolRegistry#resolve}, supporting
 * HTTP/1.1 keep-alive across sequential requests, potentially with different
 * application protocols on the same TCP connection.
 *
 * <p>Lifecycle per request:
 * <ol>
 *   <li>On inbound {@link HttpRequest}: resolves protocol, invokes
 *       {@link AppChannelConfigurer#configure}, sets {@code requestInFlight = true},
 *       re-fires the request.</li>
 *   <li>On outbound {@link LastHttpContent}: attaches a listener to the write
 *       promise. When the promise succeeds, removes per-request protocol
 *       handlers, clears {@code requestInFlight}, and makes the channel ready
 *       for the next request.</li>
 * </ol>
 *
 * <p>Per-request cleanup uses a snapshot of the pipeline taken at {@code handlerAdded}
 * time. Handlers present at that moment are considered persistent (transport, codec,
 * cross-cutting customizers); handlers added later by {@link AppChannelConfigurer#configure}
 * are per-request and are removed after the response ends.
 *
 * <p>If the http1 configurer takes exclusive ownership of the channel, for
 * example after a WebSocket handshake means the channel is no longer HTTP/1.1,
 * it must remove this dispatch handler from the pipeline; otherwise the
 * dispatcher remains and will try to route subsequent bytes as HTTP.
 *
 * <p>Channel is closed on:
 * <ul>
 *   <li>Pipelined inbound, when a second {@link HttpRequest} arrives while
 *       {@code requestInFlight}; pipelining is not supported.</li>
 *   <li>HTTP-codec or transport exceptions reaching {@link #exceptionCaught};
 *       framing is compromised, with no safe boundary for the next request.</li>
 * </ul>
 *
 * <p>Error responses for protocol resolution failures (404, 505) are graceful:
 * when HTTP keep-alive remains valid they emit a complete {@link FullHttpResponse}
 * and the next request can be served on the same TCP socket.
 */
public class Http1DispatchHandler extends ChannelDuplexHandler {
    private static final AsciiString TYPE_UTF8 = AsciiString.cached("text/plain; charset=utf-8");

    private final AppProtocolRegistry registry;

    // Implements the keep-alive feature https://datatracker.ietf.org/doc/html/rfc7230#section-6.3
    private boolean keepAliveConnection = true;

    // Request pipelining is not supported.
    private boolean requestInFlight = false;

    // Snapshot of handler names present when this handler was added; used by cleanupPipeline.
    private @Nullable Set<String> persistentHandlerNames;

    public Http1DispatchHandler(AppProtocolRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        persistentHandlerNames = Set.copyOf(ctx.pipeline().names());
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof HttpRequest request) {
            if (requestInFlight) {
                ReferenceCountUtil.release(request);
                ctx.close();
                return;
            }

            if (keepAliveConnection) {
                keepAliveConnection = HttpUtil.isKeepAlive(request);
            }

            String path = request.uri();
            AppProtocol protocol = registry.resolve(path);
            if (protocol == null) {
                sendError(ctx, HttpResponseStatus.NOT_FOUND, "No protocol registered for path: " + path);
                ReferenceCountUtil.release(request);
                return;
            }

            AppChannelConfigurer channelConfigurer = protocol.http1();
            if (channelConfigurer == null) {
                sendError(ctx, HttpResponseStatus.HTTP_VERSION_NOT_SUPPORTED,
                        "Protocol at this path does not support HTTP/1.1");
                ReferenceCountUtil.release(request);
                return;
            }

            channelConfigurer.configure(ctx.channel());

            requestInFlight = true;

            ctx.fireChannelRead(request);
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (msg instanceof HttpResponse response) {
            if (!HttpUtil.isKeepAlive(response)) {
                keepAliveConnection = false;
            }
            if (!keepAliveConnection) {
                HttpUtil.setKeepAlive(response, false);
            }
        }

        if (msg instanceof LastHttpContent) {
            if (keepAliveConnection) {
                promise = promise.unvoid().addListener(future -> {
                    if (future.isSuccess()) {
                        requestInFlight = false;
                        cleanupPipeline(ctx.pipeline());
                    }
                });
            } else {
                promise.unvoid().addListener(ChannelFutureListener.CLOSE);
            }
        }

        ctx.write(msg, promise);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        requestInFlight = false;
        cleanupPipeline(ctx.pipeline());
        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.close();
    }

    private void cleanupPipeline(ChannelPipeline pipeline) {
        Set<String> baseline = persistentHandlerNames;
        if (baseline == null) {
            return;
        }
        List<ChannelHandler> toRemove = new ArrayList<>();
        for (Map.Entry<String, ChannelHandler> entry : pipeline) {
            if (!baseline.contains(entry.getKey())) {
                toRemove.add(entry.getValue());
            }
        }
        for (ChannelHandler handler : toRemove) {
            pipeline.remove(handler);
        }
    }

    private void sendError(ChannelHandlerContext ctx, HttpResponseStatus status, String message) {
        ByteBuf body = ctx.alloc().buffer();
        body.writeBytes(message.getBytes(StandardCharsets.UTF_8));

        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, body);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, TYPE_UTF8);
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, String.valueOf(body.readableBytes()));
        HttpUtil.setKeepAlive(response, keepAliveConnection);

        ChannelFuture promise = ctx.writeAndFlush(response);
        if (!keepAliveConnection) {
            promise.addListener(ChannelFutureListener.CLOSE);
        }
    }
}
