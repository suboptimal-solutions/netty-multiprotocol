package io.suboptimal.nettymultiprotocol.http2;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.Http2DataFrame;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.suboptimal.nettymultiprotocol.AppChannelConfigurer;
import io.suboptimal.nettymultiprotocol.AppProtocol;
import io.suboptimal.nettymultiprotocol.AppProtocolRegistry;
import io.suboptimal.nettymultiprotocol.testutil.Http2FrameAssert;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
class Http2StreamDispatchHandlerTest {
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
            registry.register("/test", new AlwaysOkProtocol(Protocol.HTTP2));
            channel.pipeline().addLast(new Http2StreamDispatchHandler(registry));

            channel.writeInbound(headersFrame("/test"));

            Http2FrameAssert.assertThat((Http2HeadersFrame) channel.readOutbound())
                .hasStatus(HttpResponseStatus.OK);
            Http2FrameAssert.assertThat((Http2DataFrame) channel.readOutbound())
                .isEndStream();
        }

        @Test
        void fallsBackToHttp1WhenHttp2NotSupported() {
            var registry = new AppProtocolRegistry();
            registry.register("/test", new AlwaysOkProtocol(Protocol.HTTP1));
            channel.pipeline().addLast(new Http2StreamDispatchHandler(registry));

            channel.writeInbound(headersFrame("/test", true));

            Http2FrameAssert.assertThat((Http2HeadersFrame) channel.readOutbound())
                .hasStatus(HttpResponseStatus.OK);
            Http2FrameAssert.assertThat((Http2DataFrame) channel.readOutbound())
                .isEndStream();
        }

        @Test
        void removesSelfAfterConfiguring() {
            var registry = new AppProtocolRegistry();
            registry.register("/test", new AppProtocol() {
                public AppChannelConfigurer http2() { return ch -> {}; }
            });
            channel.pipeline().addLast(new Http2StreamDispatchHandler(registry));

            channel.writeInbound(headersFrame("/test"));

            assertThat(channel.pipeline().get(Http2StreamDispatchHandler.class)).isNull();
        }
    }

    @Nested
    class Errors {
        @Test
        void sends404OnUnknownProtocol() {
            var registry = new AppProtocolRegistry();
            channel.pipeline().addLast(new Http2StreamDispatchHandler(registry));

            channel.writeInbound(headersFrame("/unknown"));

            Http2FrameAssert.assertThat((Http2HeadersFrame) channel.readOutbound())
                .hasStatus(HttpResponseStatus.NOT_FOUND);
            Http2FrameAssert.assertThat((Http2DataFrame) channel.readOutbound())
                .isEndStream();
        }

        @Test
        void sends505WhenProtocolSupportsNeitherHttp2NorHttp1() {
            var registry = new AppProtocolRegistry();
            registry.register("/test", new AppProtocol() {});
            channel.pipeline().addLast(new Http2StreamDispatchHandler(registry));

            channel.writeInbound(headersFrame("/test"));

            Http2FrameAssert.assertThat((Http2HeadersFrame) channel.readOutbound())
                .hasStatus(HttpResponseStatus.HTTP_VERSION_NOT_SUPPORTED);
            Http2FrameAssert.assertThat((Http2DataFrame) channel.readOutbound())
                .isEndStream();
        }
    }

    enum Protocol { HTTP1, HTTP2 }

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
        public @Nullable AppChannelConfigurer http2() {
            if (protocols.contains(Protocol.HTTP2)) {
                return ch -> ch.pipeline().addLast(new AlwaysOkHttp2Handler());
            }
            return null;
        }
    }

    static class AlwaysOkHttp2Handler extends SimpleChannelInboundHandler<Http2HeadersFrame> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Http2HeadersFrame msg) {
            ctx.write(new DefaultHttp2HeadersFrame(new DefaultHttp2Headers().status(HttpResponseStatus.OK.codeAsText())));
            ctx.writeAndFlush(new DefaultHttp2DataFrame(Unpooled.EMPTY_BUFFER, true));
        }
    }

    static class AlwaysOkHttp1Handler extends SimpleChannelInboundHandler<LastHttpContent> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, LastHttpContent msg) {
            ctx.writeAndFlush(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));
        }
    }

    static DefaultHttp2HeadersFrame headersFrame(String path) {
        return headersFrame(path, false);
    }

    static DefaultHttp2HeadersFrame headersFrame(String path, boolean endStream) {
        var headers = new DefaultHttp2Headers()
                .method(HttpMethod.GET.asciiName())
                .path(path);
        return new DefaultHttp2HeadersFrame(headers, endStream);
    }
}
