package io.suboptimal.nettymultiprotocol.http3;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http3.DefaultHttp3DataFrame;
import io.netty.handler.codec.http3.DefaultHttp3Headers;
import io.netty.handler.codec.http3.DefaultHttp3HeadersFrame;
import io.netty.handler.codec.http3.Http3DataFrame;
import io.netty.handler.codec.http3.Http3FrameToHttpObjectCodec;
import io.netty.handler.codec.http3.Http3HeadersFrame;
import io.suboptimal.nettymultiprotocol.AppChannelConfigurer;
import io.suboptimal.nettymultiprotocol.AppProtocol;
import io.suboptimal.nettymultiprotocol.AppProtocolRegistry;
import io.suboptimal.nettymultiprotocol.testutil.Http3FrameAssert;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class Http3StreamDispatchHandlerTest {
    private EmbeddedChannel channel;

    @BeforeEach
    void setUp() {
        channel = new EmbeddedChannel();
    }

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
    }

    @Nested
    class RequestProcessing {
        @Test
        void forwardsRequestToAppProtocolHandler() {
            var registry = new AppProtocolRegistry();
            registry.register("/test", new AlwaysOkProtocol(Protocol.HTTP3));
            channel.pipeline().addLast(new Http3StreamDispatchHandler(registry));

            channel.writeInbound(headersFrame("/test"));

            Http3FrameAssert.assertThat(channel.readOutbound())
                .hasStatus(HttpResponseStatus.OK);
            assertThat(channel.<Object>readOutbound()).isInstanceOf(Http3DataFrame.class);
        }

        @Test
        void fallsBackToHttp1WhenHttp3NotSupported() {
            var registry = new AppProtocolRegistry();
            registry.register("/test", new AppProtocol() {
                @Override
                public AppChannelConfigurer http1() {
                    return ch -> {
                        // This is a workaround which allows the test to use the Embedded Netty channel.
                        // It suppresses the inbound data frame before object codec sees it and
                        // mitigates the ClassCastException caused by the unchecked cast in the codec.
                        var codecCtx = ch.pipeline().context(Http3FrameToHttpObjectCodec.class);
                        ch.pipeline().addBefore(codecCtx.name(), null, BlackHoleSink.INSTANCE);
                    };
                }
            });
            channel.pipeline().addLast(new Http3StreamDispatchHandler(registry));

            channel.writeInbound(headersFrame("/test"));

            assertThat(channel.pipeline().get(Http3StreamDispatchHandler.class)).isNull();
            assertThat(channel.pipeline().get(Http3FrameToHttpObjectCodec.class)).isNotNull();
        }

        @Test
        void removesSelfAfterConfiguring() {
            var registry = new AppProtocolRegistry();
            registry.register("/test", new AppProtocol() {
                public AppChannelConfigurer http3() { return ch -> {}; }
            });
            channel.pipeline().addLast(new Http3StreamDispatchHandler(registry));

            channel.writeInbound(headersFrame("/test"));

            assertThat(channel.pipeline().get(Http3StreamDispatchHandler.class)).isNull();
        }
    }

    @Nested
    class Errors {
        @Test
        void sends404OnUnknownProtocol() {
            var registry = new AppProtocolRegistry();
            channel.pipeline().addLast(new Http3StreamDispatchHandler(registry));

            channel.writeInbound(headersFrame("/unknown"));

            Http3FrameAssert.assertThat((Http3HeadersFrame) channel.readOutbound())
                .hasStatus(HttpResponseStatus.NOT_FOUND);
            assertThat(channel.<Object>readOutbound()).isInstanceOf(Http3DataFrame.class);
        }

        @Test
        void sends505WhenProtocolSupportsNeitherHttp3NorHttp1() {
            var registry = new AppProtocolRegistry();
            registry.register("/test", new AppProtocol() {
            });
            channel.pipeline().addLast(new Http3StreamDispatchHandler(registry));

            channel.writeInbound(headersFrame("/test"));

            Http3FrameAssert.assertThat((Http3HeadersFrame) channel.readOutbound())
                .hasStatus(HttpResponseStatus.HTTP_VERSION_NOT_SUPPORTED);
            assertThat(channel.<Object>readOutbound()).isInstanceOf(Http3DataFrame.class);
        }
    }

    enum Protocol {HTTP1, HTTP3}

    static class AlwaysOkProtocol implements AppProtocol {
        private final Set<Protocol> protocols;

        AlwaysOkProtocol(Protocol... protocols) {
            this.protocols = Set.of(protocols);
        }

        @Override
        public @Nullable AppChannelConfigurer http1() {
            if (protocols.contains(Protocol.HTTP1)) {
                return ch -> ch.pipeline().addLast(new AlwaysOkHttp1Handler());
            }
            return null;
        }

        @Override
        public @Nullable AppChannelConfigurer http3() {
            if (protocols.contains(Protocol.HTTP3)) {
                return ch -> ch.pipeline().addLast(new AlwaysOkHttp3Handler());
            }
            return null;
        }
    }

    static class AlwaysOkHttp3Handler extends SimpleChannelInboundHandler<Http3HeadersFrame> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Http3HeadersFrame msg) {
            ctx.write(new DefaultHttp3HeadersFrame(
                new DefaultHttp3Headers().status(HttpResponseStatus.OK.codeAsText())));
            ctx.writeAndFlush(new DefaultHttp3DataFrame(Unpooled.EMPTY_BUFFER));
        }
    }

    static class AlwaysOkHttp1Handler extends SimpleChannelInboundHandler<LastHttpContent> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, LastHttpContent msg) {
            var body = Unpooled.copiedBuffer("ok", java.nio.charset.StandardCharsets.UTF_8);
            ctx.writeAndFlush(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, body));
        }
    }

    @ChannelHandler.Sharable
    static class BlackHoleSink extends SimpleChannelInboundHandler<Http3HeadersFrame> {
        static final ChannelHandler INSTANCE = new BlackHoleSink();

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Http3HeadersFrame msg) {
        }
    }

    static DefaultHttp3HeadersFrame headersFrame(String path) {
        var headers = new DefaultHttp3Headers()
            .method(HttpMethod.GET.asciiName())
            .path(path);
        return new DefaultHttp3HeadersFrame(headers);
    }
}
