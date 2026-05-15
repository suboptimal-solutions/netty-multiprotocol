package io.suboptimal.nettymultiprotocol;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ApplicationProtocolNegotiationHandler;
import org.jspecify.annotations.Nullable;

import java.util.function.Consumer;

/**
 * Protocol negotiation handler for TLS connections.
 * Reads the ALPN result and configures the pipeline for HTTP/2 or HTTP/1.1.
 */
class AlpnNegotiationHandler extends ApplicationProtocolNegotiationHandler {
    private final AppProtocolRegistry registry;
    private final @Nullable Consumer<ChannelPipeline> http1Customizer;
    private final @Nullable Consumer<ChannelPipeline> http2Customizer;
    private final @Nullable Consumer<ChannelPipeline> http2StreamCustomizer;

    AlpnNegotiationHandler(AppProtocolRegistry registry,
                           @Nullable Consumer<ChannelPipeline> http1Customizer,
                           @Nullable Consumer<ChannelPipeline> http2Customizer,
                           @Nullable Consumer<ChannelPipeline> http2StreamCustomizer) {
        super(ApplicationProtocolNames.HTTP_1_1);
        this.registry = registry;
        this.http1Customizer = http1Customizer;
        this.http2Customizer = http2Customizer;
        this.http2StreamCustomizer = http2StreamCustomizer;
    }

    @Override
    protected void configurePipeline(ChannelHandlerContext ctx, String protocol) {
        ChannelPipeline pipeline = ctx.pipeline();
        if (ApplicationProtocolNames.HTTP_2.equals(protocol)) {
            HttpPipelines.http2(pipeline, registry, http2StreamCustomizer);
            if (http2Customizer != null) {
                http2Customizer.accept(pipeline);
            }
        } else {
            HttpPipelines.http1(pipeline, registry, false);
            if (http1Customizer != null) {
                http1Customizer.accept(pipeline);
            }
        }
    }
}
