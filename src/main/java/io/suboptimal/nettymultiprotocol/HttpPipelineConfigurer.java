package io.suboptimal.nettymultiprotocol;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerUpgradeHandler;
import io.netty.handler.codec.http2.Http2CodecUtil;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.handler.codec.http2.Http2ServerUpgradeCodec;
import io.netty.util.AsciiString;
import io.suboptimal.nettymultiprotocol.http1.Http1DispatchHandler;
import io.suboptimal.nettymultiprotocol.http2.Http2StreamDispatchHandler;
import org.jspecify.annotations.Nullable;

import java.util.function.Consumer;

/**
 * Configures Netty pipelines for each supported transport.
 */
public class HttpPipelineConfigurer {
    public static final String DISPATCH_HANDLER = "dispatch";
    public static final String H2C_UPGRADE_HANDLER = "h2cUpgrade";
    public static final String H2C_UPGRADE_DECISION_HANDLER = "h2cUpgradeDecision";

    private final AppProtocolRegistry registry;
    private final @Nullable Consumer<ChannelPipeline> http1Customizer;
    private final @Nullable Consumer<ChannelPipeline> http2Customizer;
    private final @Nullable Consumer<ChannelPipeline> http2StreamCustomizer;

    public HttpPipelineConfigurer(
            AppProtocolRegistry registry,
            @Nullable Consumer<ChannelPipeline> http1Customizer,
            @Nullable Consumer<ChannelPipeline> http2Customizer,
            @Nullable Consumer<ChannelPipeline> http2StreamCustomizer)
    {
        this.registry = registry;
        this.http1Customizer = http1Customizer;
        this.http2Customizer = http2Customizer;
        this.http2StreamCustomizer = http2StreamCustomizer;
    }

    /**
     * Configures the pipeline for HTTP/1.1.
     *
     * <p>When {@code allowH2cUpgrade} is false (ALPN HTTP/1.1): adds
     * {@code HttpServerCodec}, runs {@code http1Customizer}, then adds
     * {@link Http1DispatchHandler}.
     *
     * <p>When {@code allowH2cUpgrade} is true (plaintext): adds
     * {@code HttpServerCodec}, {@link HttpServerUpgradeHandler}, and a decision
     * handler that defers both customizer invocation and dispatcher installation
     * until the protocol is confirmed by the first inbound message.
     */
    public void installHttp1(ChannelPipeline pipeline, boolean allowH2cUpgrade) {
        HttpServerCodec httpCodec = new HttpServerCodec();
        pipeline.addLast(httpCodec);

        if (allowH2cUpgrade) {
            pipeline.addLast(H2C_UPGRADE_HANDLER, createH2cUpgradeHandler(httpCodec));
            pipeline.addLast(H2C_UPGRADE_DECISION_HANDLER, new H2cUpgradeDecisionHandler());
        } else {
            if (http1Customizer != null) {
                http1Customizer.accept(pipeline);
            }
            pipeline.addLast(DISPATCH_HANDLER, new Http1DispatchHandler(registry));
        }
    }

    /**
     * Configures the pipeline for HTTP/2.
     *
     * <p>Adds {@code Http2FrameCodec} and {@code Http2MultiplexHandler}, runs
     * {@code http2Customizer}. Each inbound H2 stream gets its own child channel with
     * an {@link Http2StreamDispatchHandler} preceded by any {@code http2StreamCustomizer}
     * handlers.
     */
    public void installHttp2(ChannelPipeline pipeline) {
        pipeline.addLast(Http2FrameCodecBuilder.forServer().build());
        pipeline.addLast(new Http2MultiplexHandler(new Http2ChildChannelInitializer()));
        if (http2Customizer != null) {
            http2Customizer.accept(pipeline);
        }
    }

    private HttpServerUpgradeHandler createH2cUpgradeHandler(HttpServerCodec httpCodec) {
        HttpServerUpgradeHandler.UpgradeCodecFactory factory = protocol -> {
            if (AsciiString.contentEquals(Http2CodecUtil.HTTP_UPGRADE_PROTOCOL_NAME, protocol)) {
                return new Http2ServerUpgradeCodec(
                    Http2FrameCodecBuilder.forServer().build(),
                    new Http2MultiplexHandler(new Http2ChildChannelInitializer()));
            }
            return null;
        };
        return new HttpServerUpgradeHandler(httpCodec, factory);
    }

    /**
     * Defers per-protocol pipeline setup until the first inbound message reveals
     * whether the connection upgrades to HTTP/2 or stays on HTTP/1.1.
     *
     * <ul>
     *   <li>First inbound {@link HttpRequest} (no upgrade) → HTTP/1.1 confirmed:
     *       runs {@code http1Customizer}, installs {@link Http1DispatchHandler}, removes H2c handlers.</li>
     *   <li>{@link HttpServerUpgradeHandler.UpgradeEvent} → HTTP/2 confirmed:
     *       runs {@code http2Customizer}, removes self.</li>
     * </ul>
     */
    private class H2cUpgradeDecisionHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof HttpRequest) {
                ChannelPipeline pipeline = ctx.pipeline();
                if (http1Customizer != null) {
                    http1Customizer.accept(pipeline);
                }
                pipeline.addLast(DISPATCH_HANDLER, new Http1DispatchHandler(registry));
                ctx.fireChannelRead(msg);
                pipeline.remove(H2C_UPGRADE_HANDLER);
                pipeline.remove(H2C_UPGRADE_DECISION_HANDLER);
            } else {
                ctx.fireChannelRead(msg);
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
            if (evt instanceof HttpServerUpgradeHandler.UpgradeEvent) {
                ChannelPipeline pipeline = ctx.pipeline();
                if (http2Customizer != null) {
                    http2Customizer.accept(pipeline);
                }
                pipeline.remove(H2C_UPGRADE_DECISION_HANDLER);
            } else {
                ctx.fireUserEventTriggered(evt);
            }
        }
    }

    /**
     * Initializer for HTTP/2 stream child channels created by {@code Http2MultiplexHandler}.
     */
    private class Http2ChildChannelInitializer extends ChannelInitializer<Channel> {
        @Override
        protected void initChannel(Channel ch) {
            ChannelPipeline pipeline = ch.pipeline();
            if (http2StreamCustomizer != null) {
                http2StreamCustomizer.accept(pipeline);
            }
            pipeline.addLast(DISPATCH_HANDLER, new Http2StreamDispatchHandler(registry));
        }
    }
}
