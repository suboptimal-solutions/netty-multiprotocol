package io.suboptimal.nettymultiprotocol;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
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
 * Static factory methods that configure Netty pipelines for each supported transport.
 */
public class HttpPipelines {
    public static final String DISPATCH_HANDLER = "dispatch";
    public static final String H2C_UPGRADE_HANDLER = "h2cUpgrade";
    public static final String H2C_UPGRADE_DETECTION_HANDLER = "h2cUpgradeDetection";

    private HttpPipelines() {}

    /**
     * Configures the pipeline for HTTP/2.
     *
     * <p>Adds {@code Http2FrameCodec} and {@code Http2MultiplexHandler}. Each
     * inbound H2 stream gets its own child channel with an
     * {@link Http2StreamDispatchHandler} that resolves the protocol per stream.
     */
    public static void http2(ChannelPipeline pipeline,
                             AppProtocolRegistry registry,
                             @Nullable Consumer<ChannelPipeline> streamCustomizer) {
        pipeline.addLast(Http2FrameCodecBuilder.forServer().build());
        pipeline.addLast(new Http2MultiplexHandler(new Http2ChildChannelInitializer(registry, streamCustomizer)));
    }

    /**
     * Configures the pipeline for HTTP/1.1.
     *
     * @param allowH2cUpgrade {@code true} for plaintext connections; adds
     *                        {@link HttpServerUpgradeHandler} to support {@code Upgrade: h2c}
     */
    public static void http1(ChannelPipeline pipeline, AppProtocolRegistry registry, boolean allowH2cUpgrade) {
        HttpServerCodec httpCodec = new HttpServerCodec();
        pipeline.addLast(httpCodec);

        if (allowH2cUpgrade) {
            pipeline.addLast(H2C_UPGRADE_HANDLER, createH2cUpgradeHandler(httpCodec, registry));
            pipeline.addLast(H2C_UPGRADE_DETECTION_HANDLER, new Http2UpgradeDetectionHandler());
        }

        pipeline.addLast(DISPATCH_HANDLER, new Http1DispatchHandler(registry));
    }

    private static HttpServerUpgradeHandler createH2cUpgradeHandler(
            HttpServerCodec httpCodec, AppProtocolRegistry registry) {

        HttpServerUpgradeHandler.UpgradeCodecFactory factory = protocol -> {
            if (AsciiString.contentEquals(Http2CodecUtil.HTTP_UPGRADE_PROTOCOL_NAME, protocol)) {
                return new Http2ServerUpgradeCodec(
                        Http2FrameCodecBuilder.forServer().build(),
                        new Http2MultiplexHandler(new Http2ChildChannelInitializer(registry, null)));
            }
            return null;
        };

        return new HttpServerUpgradeHandler(httpCodec, factory);
    }

    private static class Http2UpgradeDetectionHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
            if (evt instanceof HttpServerUpgradeHandler.UpgradeEvent) {
                ctx.pipeline().remove(DISPATCH_HANDLER);
            }
        }
    }

    /**
     * Initializer for HTTP/2 stream child channels created by {@code Http2MultiplexHandler}.
     */
    private static class Http2ChildChannelInitializer extends ChannelInitializer<Channel> {
        private final AppProtocolRegistry registry;
        private final @Nullable Consumer<ChannelPipeline> streamCustomizer;

        Http2ChildChannelInitializer(AppProtocolRegistry registry,
                                     @Nullable Consumer<ChannelPipeline> streamCustomizer) {
            this.registry = registry;
            this.streamCustomizer = streamCustomizer;
        }

        @Override
        protected void initChannel(Channel ch) {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast(DISPATCH_HANDLER, new Http2StreamDispatchHandler(registry));
            if (streamCustomizer != null) {
                streamCustomizer.accept(pipeline);
            }
        }
    }
}
