package io.suboptimal.nettymultiprotocol.http1;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.suboptimal.nettymultiprotocol.AppChannelConfigurer;
import io.suboptimal.nettymultiprotocol.AppProtocol;
import io.suboptimal.nettymultiprotocol.AppProtocolRegistry;
import io.suboptimal.nettymultiprotocol.testutil.HttpResponseAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class Http1DispatchHandlerTest {
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
            registry.register("/test", new AlwaysOkHttp1Protocol());
            channel.pipeline().addLast(new Http1DispatchHandler(registry));

            channel.writeInbound(fullHttpRequest("/test"));

            HttpResponseAssert.assertThat(channel.readOutbound()).hasStatus(HttpResponseStatus.OK);
        }

        @Test
        void forwardsSubsequentRequests() {
            var registry = new AppProtocolRegistry();
            registry.register("/a", new AlwaysOkHttp1Protocol());
            registry.register("/b", new AlwaysOkHttp1Protocol());
            channel.pipeline().addLast(new Http1DispatchHandler(registry));

            channel.writeInbound(withKeepAlive(fullHttpRequest("/a")));
            HttpResponseAssert.assertThat(channel.readOutbound()).hasStatus(HttpResponseStatus.OK);

            channel.writeInbound(withKeepAlive(fullHttpRequest("/b")));
            HttpResponseAssert.assertThat(channel.readOutbound()).hasStatus(HttpResponseStatus.OK);
        }
    }

    @Nested
    class Errors {
        @Test
        void sends404OnUnknownProtocol() {
            var registry = new AppProtocolRegistry();
            channel.pipeline().addLast(new Http1DispatchHandler(registry));

            channel.writeInbound(fullHttpRequest("/unknown"));

            HttpResponseAssert.assertThat(channel.readOutbound()).hasStatus(HttpResponseStatus.NOT_FOUND);
        }

        @Test
        void sends505WhenProtocolDoesNotSupportHttp1() {
            var registry = new AppProtocolRegistry();
            registry.register("/test", new AppProtocol() {});
            channel.pipeline().addLast(new Http1DispatchHandler(registry));

            channel.writeInbound(fullHttpRequest("/test"));

            HttpResponseAssert.assertThat(channel.readOutbound()).hasStatus(HttpResponseStatus.HTTP_VERSION_NOT_SUPPORTED);
        }

        @Test
        void gracefulErrorPreservesKeepAlive() {
            var registry = new AppProtocolRegistry();
            registry.register("/ok", new AlwaysOkHttp1Protocol());
            channel.pipeline().addLast(new Http1DispatchHandler(registry));

            channel.writeInbound(withKeepAlive(fullHttpRequest("/unknown")));
            HttpResponseAssert.assertThat(channel.readOutbound()).hasStatus(HttpResponseStatus.NOT_FOUND);
            assertThat(channel.isActive()).isTrue();

            channel.writeInbound(withKeepAlive(fullHttpRequest("/ok")));
            HttpResponseAssert.assertThat(channel.readOutbound()).hasStatus(HttpResponseStatus.OK);
        }

        @Test
        void closesChannelAfterErrorWhenClientRequestedClose() {
            var registry = new AppProtocolRegistry();
            channel.pipeline().addLast(new Http1DispatchHandler(registry));

            channel.writeInbound(withNoKeepAlive(fullHttpRequest("/unknown")));
            channel.runPendingTasks();

            assertThat(channel.isActive()).isFalse();
        }

        @Test
        void closesChannelOnExceptionCaught() {
            channel.pipeline().addLast(new Http1DispatchHandler(new AppProtocolRegistry()));

            channel.pipeline().fireExceptionCaught(new RuntimeException("boom"));
            channel.runPendingTasks();

            assertThat(channel.isActive()).isFalse();
        }

        @Test
        void closesChannelOnRequestPipelining() {
            var registry = new AppProtocolRegistry();
            registry.register("/test", new AppProtocol() {
                public AppChannelConfigurer http1() { return ch -> {}; }
            });
            channel.pipeline().addLast(new Http1DispatchHandler(registry));

            channel.writeInbound(httpRequest("/test"));
            channel.writeInbound(httpRequest("/test"));
            channel.runPendingTasks();

            assertThat(channel.isActive()).isFalse();
        }
    }

    @Nested
    class KeepAlive {
        @Test
        void preservesChannelForKeepAliveRequests() {
            var registry = new AppProtocolRegistry();
            registry.register("/test", new AlwaysOkHttp1Protocol());
            channel.pipeline().addLast(new Http1DispatchHandler(registry));

            channel.writeInbound(withKeepAlive(fullHttpRequest("/test")));
            channel.runPendingTasks();

            assertThat(channel.isActive()).isTrue();
        }

        @Test
        void closesChannelForNonKeepAliveRequests() {
            var registry = new AppProtocolRegistry();
            registry.register("/test", new AlwaysOkHttp1Protocol());
            channel.pipeline().addLast(new Http1DispatchHandler(registry));

            channel.writeInbound(withNoKeepAlive(fullHttpRequest("/test")));
            channel.runPendingTasks();

            assertThat(channel.isActive()).isFalse();
        }

        @Test
        void closesChannelForNonKeepAliveResponses() {
            var registry = new AppProtocolRegistry();
            registry.register("/test", new AppProtocol() {
                public AppChannelConfigurer http1() { return ch -> {}; }
            });
            channel.pipeline().addLast(new Http1DispatchHandler(registry));

            channel.writeInbound(withKeepAlive(fullHttpRequest("/test")));

            var response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
            HttpUtil.setKeepAlive(response, false);
            channel.writeOutbound(response);
            channel.runPendingTasks();

            assertThat(channel.isActive()).isFalse();
        }
    }

    @Nested
    class PipelineCleanup {
        @Test
        void removesPerRequestHandlersAfterResponse() {
            var registry = new AppProtocolRegistry();
            registry.register("/test", new AlwaysOkHttp1Protocol());
            channel.pipeline().addLast(new Http1DispatchHandler(registry));

            channel.writeInbound(httpRequest("/test"));
            assertThat(channel.pipeline().get(AlwaysOkHandler.class)).isNotNull();

            channel.writeInbound(LastHttpContent.EMPTY_LAST_CONTENT);
            channel.runPendingTasks();
            assertThat(channel.pipeline().get(AlwaysOkHandler.class)).isNull();
        }

        @Test
        void preservesBaselineHandlers() {
            var baseline = new ChannelInboundHandlerAdapter();
            channel.pipeline().addLast(baseline);
            channel.pipeline().addLast(new Http1DispatchHandler(new AppProtocolRegistry()));

            channel.writeOutbound(LastHttpContent.EMPTY_LAST_CONTENT);
            channel.runPendingTasks();

            assertThat(channel.pipeline().toMap().values()).contains(baseline);
            assertThat(channel.pipeline().get(Http1DispatchHandler.class)).isNotNull();
        }

        @Test
        void cleansUpOnChannelInactive() {
            var registry = new AppProtocolRegistry();
            registry.register("/test", new AlwaysOkHttp1Protocol());
            channel.pipeline().addLast(new Http1DispatchHandler(registry));

            channel.writeInbound(httpRequest("/test"));  // AlwaysOkHandler ждёт LastHttpContent — ответа нет
            assertThat(channel.pipeline().get(AlwaysOkHandler.class)).isNotNull();

            channel.pipeline().fireChannelInactive();
            assertThat(channel.pipeline().get(AlwaysOkHandler.class)).isNull();
        }
    }

    static class AlwaysOkHttp1Protocol implements AppProtocol {
        @Override
        public AppChannelConfigurer http1() {
            return ch -> ch.pipeline().addLast(new AlwaysOkHandler());
        }
    }

    static class AlwaysOkHandler extends SimpleChannelInboundHandler<LastHttpContent> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, LastHttpContent msg) {
            ctx.writeAndFlush(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));
        }
    }

    static DefaultFullHttpRequest fullHttpRequest(String path) {
        return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, path);
    }

    static DefaultHttpRequest httpRequest(String path) {
        return new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, path);
    }

    static <T extends HttpRequest> T withKeepAlive(T request) {
        HttpUtil.setKeepAlive(request, true);
        return request;
    }

    static <T extends HttpRequest> T withNoKeepAlive(T request) {
        HttpUtil.setKeepAlive(request, false);
        return request;
    }
}
