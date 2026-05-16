package io.suboptimal.nettymultiprotocol;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ApplicationProtocolNegotiationHandler;

/**
 * Protocol negotiation handler for TLS connections.
 * Reads the ALPN result and configures the pipeline for HTTP/2 or HTTP/1.1.
 */
class AlpnNegotiationHandler extends ApplicationProtocolNegotiationHandler {
    private final HttpPipelineConfigurer configurer;

    AlpnNegotiationHandler(HttpPipelineConfigurer configurer) {
        super(ApplicationProtocolNames.HTTP_1_1);
        this.configurer = configurer;
    }

    @Override
    protected void configurePipeline(ChannelHandlerContext ctx, String protocol) {
        if (ApplicationProtocolNames.HTTP_2.equals(protocol)) {
            configurer.installHttp2(ctx.pipeline());
        } else {
            configurer.installHttp1(ctx.pipeline(), false);
        }
    }
}
