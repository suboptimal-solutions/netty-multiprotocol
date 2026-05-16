package io.suboptimal.nettymultiprotocol;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslContext;
import org.jspecify.annotations.Nullable;

import java.util.function.Consumer;

/**
 * Top-level channel initializer. Sets up the protocol negotiation entry point.
 */
class NettyMultiprotocolInitializer extends ChannelInitializer<Channel> {
    private final @Nullable SslContext sslContext;
    private final HttpPipelineConfigurer configurer;

    NettyMultiprotocolInitializer(@Nullable SslContext sslContext,
                                  AppProtocolRegistry registry,
                                  @Nullable Consumer<ChannelPipeline> http1Customizer,
                                  @Nullable Consumer<ChannelPipeline> http2Customizer,
                                  @Nullable Consumer<ChannelPipeline> http2StreamCustomizer) {
        this.sslContext = sslContext;
        this.configurer = new HttpPipelineConfigurer(registry, http1Customizer, http2Customizer, http2StreamCustomizer);
    }

    @Override
    protected void initChannel(Channel ch) {
        ChannelPipeline pipeline = ch.pipeline();

        if (sslContext != null) {
            pipeline.addLast(sslContext.newHandler(ch.alloc()));
            pipeline.addLast(new AlpnNegotiationHandler(configurer));
        } else {
            pipeline.addLast(new H2cNegotiationHandler(configurer));
        }
    }
}
