package io.suboptimal.nettymultiprotocol;

import java.util.Objects;
import java.util.function.Consumer;

import org.jspecify.annotations.Nullable;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslContext;

/**
 * Entry point for building a multi-protocol HTTP server. Produces a
 * {@link ChannelInitializer} that you wire into your own {@code ServerBootstrap}:
 *
 * <pre>{@code
 * AppProtocolRegistry registry = new AppProtocolRegistry();
 * registry.register("/twirp/*", new TwirpProtocol(...));
 * registry.register("/", new RestProtocol(...));
 *
 * ChannelInitializer<Channel> initializer = NettyMultiprotocol.builder()
 *     .sslContext(sslContext)          // optional; plaintext if null
 *     .registry(registry)
 *     .onHttp1ChannelConfigured(p ->
 *         p.addBefore(HttpPipelines.DISPATCH_HANDLER, "accessLog", new MyAccessLogHandler()))
 *     .build();
 *
 * new ServerBootstrap()
 *     .group(boss, worker)
 *     .channel(NioServerSocketChannel.class)
 *     .childHandler(initializer)
 *     .bind(port).sync();
 * }</pre>
 *
 * <h2>Pipeline customizers</h2>
 *
 * Three optional hooks let you install cross-cutting handlers (access log,
 * request id, metrics, idle-state timeouts) without subclassing the initializer:
 *
 * <ul>
 *   <li>{@link Builder#onHttp1ChannelConfigured} - runs after {@code HttpServerCodec}
 *       and {@code Http1DispatchHandler} are installed; the channel handles HTTP/1.1
 *       traffic over the lifetime of the connection (keep-alive). Handlers in this
 *       zone see parsed {@code HttpRequest}/{@code HttpResponse} objects.</li>
 *   <li>{@link Builder#onHttp2ChannelConfigured} - runs once per HTTP/2 parent
 *       channel, after the frame codec and multiplex handler. Operates on
 *       {@code Http2Frame}s; useful for connection-scoped concerns.</li>
 *   <li>{@link Builder#onHttp2StreamChannelConfigured} - runs once per HTTP/2 stream
 *       child channel. The pipeline only contains {@code Http2StreamDispatchHandler}
 *       at this point; the codec ({@code Http2StreamFrameToHttpObjectCodec}) is only
 *       installed later if the resolved protocol falls back to HTTP/1.1. Handlers
 *       in this zone therefore see raw {@code Http2StreamFrame}s by default.</li>
 * </ul>
 *
 * Use {@link HttpPipelines#DISPATCH_HANDLER} as a stable anchor for
 * {@code pipeline.addBefore} / {@code addAfter}.
 */
public final class NettyMultiprotocol {

    private NettyMultiprotocol() {}

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private @Nullable SslContext sslContext;
        private @Nullable AppProtocolRegistry registry;
        private @Nullable Consumer<ChannelPipeline> http1Customizer;
        private @Nullable Consumer<ChannelPipeline> http2Customizer;
        private @Nullable Consumer<ChannelPipeline> http2StreamCustomizer;

        private Builder() {}

        /** Optional. If {@code null}, the server accepts plaintext connections only. */
        public Builder sslContext(@Nullable SslContext sslContext) {
            this.sslContext = sslContext;
            return this;
        }

        /** Required. The registry that maps URI patterns to {@link AppProtocol}s. */
        public Builder registry(AppProtocolRegistry registry) {
            this.registry = Objects.requireNonNull(registry, "registry");
            return this;
        }

        public Builder onHttp1ChannelConfigured(Consumer<ChannelPipeline> customizer) {
            this.http1Customizer = Objects.requireNonNull(customizer, "customizer");
            return this;
        }

        public Builder onHttp2ChannelConfigured(Consumer<ChannelPipeline> customizer) {
            this.http2Customizer = Objects.requireNonNull(customizer, "customizer");
            return this;
        }

        public Builder onHttp2StreamChannelConfigured(Consumer<ChannelPipeline> customizer) {
            this.http2StreamCustomizer = Objects.requireNonNull(customizer, "customizer");
            return this;
        }

        public ChannelInitializer<Channel> build() {
            if (registry == null) {
                throw new IllegalStateException("registry is required");
            }
            return new NettyMultiprotocolInitializer(
                    sslContext, registry, http1Customizer, http2Customizer, http2StreamCustomizer);
        }
    }
}
