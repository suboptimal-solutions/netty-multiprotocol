package io.suboptimal.nettymultiprotocol;

import org.jspecify.annotations.Nullable;

/**
 * An application-layer protocol that can be mounted on the multi-protocol server
 * at a specific URI pattern. Declares which HTTP transports it supports by
 * returning an {@link AppChannelConfigurer} for each one, or {@code null}.
 *
 * <p>Implementations override only the transports they need.
 *
 * <h2>WebSocket</h2>
 *
 * WebSocket is layered on HTTP/1.1, not a separate transport. If a protocol
 * supports WebSocket, its {@link #http1()} configurer is responsible for
 * detecting the {@code Upgrade: websocket} header, performing the handshake
 * and reconfiguring the pipeline. The configurer may also remove the
 * dispatch handler from the pipeline if it takes exclusive ownership of
 * the channel.
 */
public interface AppProtocol {

    /**
     * Returns a configurer for HTTP/1.1 connections. The pipeline already contains
     * {@code HttpServerCodec} when the configurer is called.
     *
     * @return configurer, or {@code null} if this protocol does not support HTTP/1.1
     */
    default @Nullable AppChannelConfigurer http1() { return null; }

    /**
     * Returns a configurer for HTTP/2 streams. The pipeline operates on raw
     * {@code Http2StreamFrame}s (no HTTP-object conversion). If this method returns
     * {@code null} but {@link #http1()} does not, the dispatch layer will install
     * {@code Http2StreamFrameToHttpObjectCodec} and fall back to the HTTP/1.1 configurer.
     *
     * @return configurer, or {@code null} if this protocol does not handle native HTTP/2 frames
     */
    default @Nullable AppChannelConfigurer http2() { return null; }

    /**
     * Returns a configurer for HTTP/3 streams. The pipeline operates on raw
     * {@code Http3RequestStreamFrame}s (no HTTP-object conversion). If this method returns
     * {@code null} but {@link #http1()} does not, an HTTP/3 dispatcher will install
     * {@code Http3FrameToHttpObjectCodec} and fall back to the HTTP/1.1 configurer.
     *
     * <p>This method is invoked by {@code Http3StreamDispatchHandler} only when the user
     * has wired an HTTP/3 (UDP/QUIC) server with that handler installed per request stream.
     *
     * @return configurer, or {@code null} if this protocol does not handle native HTTP/3 frames
     */
    default @Nullable AppChannelConfigurer http3() { return null; }
}
