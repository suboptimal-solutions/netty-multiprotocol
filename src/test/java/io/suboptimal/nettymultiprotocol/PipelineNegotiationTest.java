package io.suboptimal.nettymultiprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.AbstractServerChannel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.DefaultChannelConfig;
import io.netty.channel.DefaultChannelId;
import io.netty.channel.EventLoop;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http2.Http2CodecUtil;
import io.netty.handler.codec.http2.Http2FrameCodec;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.handler.codec.http2.Http2StreamFrameToHttpObjectCodec;
import io.netty.util.ReferenceCountUtil;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class PipelineNegotiationTest {
    @Nested
    class Plaintext {
        EmbeddedChannel channel;
        AppProtocolRegistry registry;
        PipelineCaptor captor;

        @BeforeEach
        void setUp() {
            channel = new EmbeddedChannel(new FakeServerChannel(), DefaultChannelId.newInstance(), true, false);
            captor = new PipelineCaptor();
            registry = new AppProtocolRegistry();
            new NettyMultiprotocolInitializer(null, registry, null, null, null).initChannel(channel);
        }

        @AfterEach
        void tearDown() {
            channel.finishAndReleaseAll();
        }

        @Test
        void http1Pipeline() {
            registry.register("/http1", new AppProtocol() {
                @Override
                public AppChannelConfigurer http1() {
                    return ch -> ch.pipeline().addLast(captor);
                }
            });

            channel.writeInbound(Unpooled.copiedBuffer("GET /http1 HTTP/1.1\r\n\r\n", StandardCharsets.US_ASCII));

            ChannelPipeline pipeline = captor.pipeline();
            assertThat(pipeline).isNotNull();
            assertThat(pipeline.get(HttpServerCodec.class)).isNotNull();
            assertThat(pipeline.get(HttpObjectAggregator.class)).isNull();
            assertThat(pipeline.get(HttpPipelines.DISPATCH_HANDLER)).isNotNull();
            assertThat(pipeline.get(HttpPipelines.H2C_UPGRADE_HANDLER)).isNull();
            assertThat(pipeline.get(HttpPipelines.H2C_UPGRADE_DETECTION_HANDLER)).isNull();
        }

        @Test
        void h2cPriorKnowledgePipeline() {
            registry.register("/h2c", new AppProtocol() {
                @Override
                public AppChannelConfigurer http2() {
                    return ch -> ch.pipeline().addLast(captor);
                }
            });

            channel.writeInbound(h2PrefaceSettingsAndHeaders());

            ChannelPipeline parentPipeline = channel.pipeline();
            assertThat(parentPipeline.get(Http2FrameCodec.class)).isNotNull();
            assertThat(parentPipeline.get(Http2MultiplexHandler.class)).isNotNull();
            assertThat(parentPipeline.get(HttpServerCodec.class)).isNull();

            ChannelPipeline childPipeline = captor.pipeline();
            assertThat(childPipeline).isNotNull();
            assertThat(childPipeline.get(Http2StreamFrameToHttpObjectCodec.class)).isNull();
            assertThat(childPipeline.get(HttpPipelines.DISPATCH_HANDLER)).isNull();
        }

        @Test
        void h2cUpgradePipeline() {
            registry.register("/h2c", new AppProtocol() {
                @Override
                public AppChannelConfigurer http2() {
                    return ch -> ch.pipeline().addLast(captor);
                }
            });

            channel.writeInbound(h2cUpgradeRequest());

            ChannelPipeline parentPipeline = channel.pipeline();
            assertThat(parentPipeline.get(Http2FrameCodec.class)).isNotNull();
            assertThat(parentPipeline.get(Http2MultiplexHandler.class)).isNotNull();
            assertThat(parentPipeline.get(HttpServerCodec.class)).isNull();

            ChannelPipeline childPipeline = captor.pipeline();
            assertThat(childPipeline).isNotNull();
            assertThat(childPipeline.get(Http2StreamFrameToHttpObjectCodec.class)).isNull();
            assertThat(childPipeline.get(HttpPipelines.DISPATCH_HANDLER)).isNull();
        }

        @Test
        void h2cFallbackPipeline() {
            registry.register("/h2c", new AppProtocol() {
                @Override
                public AppChannelConfigurer http1() {
                    return ch -> ch.pipeline().addLast(captor);
                }
            });

            channel.writeInbound(h2cUpgradeRequest());

            ChannelPipeline parentPipeline = channel.pipeline();
            assertThat(parentPipeline.get(Http2FrameCodec.class)).isNotNull();
            assertThat(parentPipeline.get(Http2MultiplexHandler.class)).isNotNull();
            assertThat(parentPipeline.get(HttpServerCodec.class)).isNull();

            ChannelPipeline childPipeline = captor.pipeline();
            assertThat(childPipeline).isNotNull();
            assertThat(childPipeline.get(Http2StreamFrameToHttpObjectCodec.class)).isNotNull();
            assertThat(childPipeline.get(HttpPipelines.DISPATCH_HANDLER)).isNull();
        }
    }

    @Test
    void customizerInvokedAfterPipelineConfigured() {
        AtomicBoolean http1CustomizerInvoked = new AtomicBoolean();
        AppProtocolRegistry http1Registry = new AppProtocolRegistry();
        http1Registry.register("/http1", new AppProtocol() {
            @Override
            public AppChannelConfigurer http1() {
                return ch -> ch.pipeline().addLast(new PipelineCaptor());
            }
        });
        EmbeddedChannel http1Channel = new EmbeddedChannel(new FakeServerChannel(), DefaultChannelId.newInstance(), true, false);
        try {
            new NettyMultiprotocolInitializer(null, http1Registry, pipeline -> {
                http1CustomizerInvoked.set(true);
                assertThat(pipeline.get(HttpPipelines.DISPATCH_HANDLER)).isNotNull();
                pipeline.addBefore(HttpPipelines.DISPATCH_HANDLER, "http1-customizer", new ChannelInboundHandlerAdapter());
            }, null, null).initChannel(http1Channel);

            http1Channel.writeInbound(Unpooled.copiedBuffer("GET /http1 HTTP/1.1\r\n\r\n", StandardCharsets.US_ASCII));

            assertThat(http1CustomizerInvoked).isTrue();
            assertThat(http1Channel.pipeline().names())
                    .containsSubsequence("http1-customizer", HttpPipelines.DISPATCH_HANDLER);
        } finally {
            http1Channel.finishAndReleaseAll();
        }

        AtomicBoolean http2StreamCustomizerInvoked = new AtomicBoolean();
        PipelineCaptor h2Captor = new PipelineCaptor();
        AppProtocolRegistry h2Registry = new AppProtocolRegistry();
        h2Registry.register("/h2c", new AppProtocol() {
            @Override
            public AppChannelConfigurer http2() {
                return ch -> ch.pipeline().addLast(h2Captor);
            }
        });
        EmbeddedChannel h2Channel = new EmbeddedChannel(new FakeServerChannel(), DefaultChannelId.newInstance(), true, false);
        try {
            new NettyMultiprotocolInitializer(null, h2Registry, null, null, pipeline -> {
                http2StreamCustomizerInvoked.set(true);
                assertThat(pipeline.get(HttpPipelines.DISPATCH_HANDLER)).isNotNull();
                pipeline.addAfter(HttpPipelines.DISPATCH_HANDLER, "h2-stream-customizer", new ChannelInboundHandlerAdapter());
            }).initChannel(h2Channel);

            h2Channel.writeInbound(h2PrefaceSettingsAndHeaders());

            ChannelPipeline childPipeline = h2Captor.pipeline();
            assertThat(http2StreamCustomizerInvoked).isTrue();
            assertThat(childPipeline).isNotNull();
            assertThat(childPipeline.names()).contains("h2-stream-customizer");
        } finally {
            h2Channel.finishAndReleaseAll();
        }
    }

    static ByteBuf h2cUpgradeRequest() {
        String http2Settings = Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[0]);
        return Unpooled.copiedBuffer(
                "GET /h2c HTTP/1.1\r\n" +
                        "Host: localhost\r\n" +
                        "Upgrade: h2c\r\n" +
                        "HTTP2-Settings: " + http2Settings + "\r\n" +
                        "Connection: Upgrade, HTTP2-Settings\r\n" +
                        "\r\n",
                StandardCharsets.US_ASCII);
    }

    static ByteBuf h2PrefaceSettingsAndHeaders() {
        ByteBuf preface = Http2CodecUtil.connectionPrefaceBuf();

        ByteBuf settings = Unpooled.buffer(9);
        settings.writeMedium(0);
        settings.writeByte(0x04);
        settings.writeByte(0x00);
        settings.writeInt(0);

        byte[] hpack = {
                (byte) 0x82,
                (byte) 0x86,
                0x44, 0x04, 0x2f, 0x68, 0x32, 0x63,
                0x41, 0x09, 0x6c, 0x6f, 0x63, 0x61, 0x6c, 0x68,
                0x6f, 0x73, 0x74
        };
        ByteBuf headers = Unpooled.buffer(9 + hpack.length);
        headers.writeMedium(hpack.length);
        headers.writeByte(0x01);
        headers.writeByte(0x05);
        headers.writeInt(1);
        headers.writeBytes(hpack);

        return Unpooled.wrappedBuffer(preface, settings, headers);
    }

    private static class PipelineCaptor extends ChannelInboundHandlerAdapter {
        private @Nullable ChannelPipeline pipeline;

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            pipeline = ctx.pipeline();
            ReferenceCountUtil.release(msg);
        }

        @Nullable ChannelPipeline pipeline() {
            return pipeline;
        }
    }

    private static class FakeServerChannel extends AbstractServerChannel {
        private final ChannelConfig config = new DefaultChannelConfig(this);

        @Override
        public ChannelConfig config() { return config; }

        @Override
        protected boolean isCompatible(EventLoop loop) { return true; }

        @Override
        protected SocketAddress localAddress0() { return null; }

        @Override
        public boolean isActive() { return true; }

        @Override
        public boolean isOpen() { return true; }

        @Override
        protected void doBind(SocketAddress localAddress) {}

        @Override
        protected void doClose() {}

        @Override
        protected void doBeginRead() {}
    }
}
