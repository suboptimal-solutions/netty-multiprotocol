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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

class PipelineNegotiationTest {
    @Nested
    class Plaintext {
        EmbeddedChannel channel;
        AppProtocolRegistry registry;
        PipelineCaptor captor;

        @BeforeEach
        void setUp() {
            registry = new AppProtocolRegistry();
            channel = new EmbeddedChannel(
                new FakeServerChannel(),
                DefaultChannelId.newInstance(),
                true,
                false,
                NettyMultiprotocol.builder().registry(registry).build());
            captor = new PipelineCaptor();
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

            channel.writeInbound(http1Request());

            ChannelPipeline pipeline = captor.pipeline();
            assertThat(pipeline).isNotNull();
            assertThat(pipeline.get(HttpServerCodec.class)).isNotNull();
            assertThat(pipeline.get(HttpPipelineConfigurer.DISPATCH_HANDLER)).isNotNull();

            assertThat(pipeline.get(HttpPipelineConfigurer.H2C_UPGRADE_HANDLER)).isNull();
            assertThat(pipeline.get(HttpPipelineConfigurer.H2C_UPGRADE_DECISION_HANDLER)).isNull();
            assertThat(pipeline.get(HttpObjectAggregator.class)).isNull();
        }

        @Test
        void h2cPriorKnowledgePipeline() {
            registry.register("/h2c", new AppProtocol() {
                @Override
                public AppChannelConfigurer http2() {
                    return ch -> ch.pipeline().addLast(captor);
                }
            });

            channel.writeInbound(h2PrefaceRequest());

            ChannelPipeline parentPipeline = channel.pipeline();
            assertThat(parentPipeline.get(Http2FrameCodec.class)).isNotNull();
            assertThat(parentPipeline.get(Http2MultiplexHandler.class)).isNotNull();
            assertThat(parentPipeline.get(HttpServerCodec.class)).isNull();

            ChannelPipeline childPipeline = captor.pipeline();
            assertThat(childPipeline).isNotNull();
            assertThat(childPipeline.get(Http2StreamFrameToHttpObjectCodec.class)).isNull();
            assertThat(childPipeline.get(HttpPipelineConfigurer.DISPATCH_HANDLER)).isNull();
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
            assertThat(childPipeline.get(HttpPipelineConfigurer.DISPATCH_HANDLER)).isNull();
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
            assertThat(childPipeline.get(HttpPipelineConfigurer.DISPATCH_HANDLER)).isNull();
        }
    }

    @Nested
    class PipelineCustomizers {
        EmbeddedChannel channel;
        AppProtocolRegistry registry;
        CustomizesCaptor customizesCaptor;

        @BeforeEach
        void setUp() {
            registry = new AppProtocolRegistry();
            registry.register("/http1", new AppProtocol() {
                @Override
                public AppChannelConfigurer http1() {
                    return ch -> {};
                }
            });
            registry.register("/h2c", new AppProtocol() {
                @Override
                public AppChannelConfigurer http2() {
                    return ch -> {};
                }
            });

            customizesCaptor = new CustomizesCaptor();
            channel = new EmbeddedChannel(
                new FakeServerChannel(),
                DefaultChannelId.newInstance(),
                true,
                false,
                NettyMultiprotocol
                    .builder()
                    .registry(registry)
                    .onHttp1ChannelConfigured(ignore -> customizesCaptor.onHttp1ChannelConfigured())
                    .onHttp2ChannelConfigured(ignore -> customizesCaptor.onHttp2ChannelConfigured())
                    .onHttp2StreamChannelConfigured(ignore -> customizesCaptor.onHttp2StreamChannelConfigured())
                    .build());
        }

        @AfterEach
        void tearDown() {
            channel.finishAndReleaseAll();
        }

        @Test
        void http1Customizer() {
            channel.writeInbound(http1Request());

            assertThat(customizesCaptor.http1CustomizerInvoked).isTrue();
            assertThat(customizesCaptor.http2CustomizerInvoked).isFalse();
            assertThat(customizesCaptor.http2StreamCustomizerInvoked).isFalse();
        }

        @Test
        void h2cUpgradeCustomizer() {
            channel.writeInbound(h2cUpgradeRequest());

            assertThat(customizesCaptor.http1CustomizerInvoked).isFalse();
            assertThat(customizesCaptor.http2CustomizerInvoked).isTrue();
            assertThat(customizesCaptor.http2StreamCustomizerInvoked).isTrue();
        }

        @Test
        void h2cPriorKnowledgeCustomizer() {
            channel.writeInbound(h2PrefaceRequest());

            assertThat(customizesCaptor.http1CustomizerInvoked).isFalse();
            assertThat(customizesCaptor.http2CustomizerInvoked).isTrue();
            assertThat(customizesCaptor.http2StreamCustomizerInvoked).isTrue();
        }

        static class CustomizesCaptor {
            boolean http1CustomizerInvoked;
            boolean http2CustomizerInvoked;
            boolean http2StreamCustomizerInvoked;

            void onHttp1ChannelConfigured() {
                http1CustomizerInvoked = true;
            }

            void onHttp2ChannelConfigured() {
                http2CustomizerInvoked = true;
            }

            void onHttp2StreamChannelConfigured() {
                http2StreamCustomizerInvoked = true;
            }
        }
    }

    static ByteBuf http1Request() {
        return Unpooled.copiedBuffer("GET /http1 HTTP/1.1\r\n\r\n", StandardCharsets.US_ASCII);
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

    static ByteBuf h2PrefaceRequest() {
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
        public void handlerAdded(ChannelHandlerContext ctx) {
            pipeline = ctx.pipeline();
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
