package io.suboptimal.nettymultiprotocol;

import io.netty.channel.Channel;

/**
 * Configures a Netty channel pipeline for a specific application protocol and
 * transport combination.
 *
 * <p>Called by the dispatch handler after protocol resolution. The configurer adds
 * protocol-specific handlers (codecs, aggregators, business logic) to the channel's
 * pipeline. The first inbound message is delivered via {@code ctx.fireChannelRead}
 * after the configurer returns.
 *
 * <p>Configurers only install handlers; they do not receive a cleanup callback.
 * For HTTP/1.1 keep-alive, the dispatch layer snapshots the pipeline at install
 * time and removes every handler added after that snapshot once the terminal
 * response content is written. Persistent cross-cutting handlers (access log,
 * metrics) belong in the builder's pipeline customizers so they are captured in
 * the snapshot and survive keep-alive cleanup.
 *
 * @see AppProtocol
 */
public interface AppChannelConfigurer {
    void configure(Channel channel);
}
