package io.suboptimal.nettymultiprotocol.http1;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.AsciiString;
import io.netty.util.ReferenceCountUtil;
import io.suboptimal.nettymultiprotocol.AppChannelConfigurer;
import io.suboptimal.nettymultiprotocol.AppProtocol;
import io.suboptimal.nettymultiprotocol.AppProtocolRegistry;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class Http1DispatchHandlerTest {
    private EmbeddedChannel channel;

    @AfterEach
    void tearDown() {
        if (channel != null) {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void routesFirstRequestAndConfiguresPipeline() {
        CapturingConfigurer configurer = new CapturingConfigurer();
        channel = channelWith("/rpc/*", configurer);

        channel.writeInbound(request("/rpc/Service/Method"));

        assertThat(configurer.configureCalls).isEqualTo(1);
        assertThat(configurer.seenUris).containsExactly("/rpc/Service/Method");
    }

    @Test
    void dispatcherStaysInPipelineAfterFirstRequest() {
        channel = channelWith("/rpc/*", new CapturingConfigurer());

        channel.writeInbound(request("/rpc/Service/Method"));

        assertThat(channel.pipeline().get(Http1DispatchHandler.class)).isNotNull();
    }

    @Test
    void dispatcherRemovesProtocolHandlerAfterLastHttpContent() {
        CapturingConfigurer configurer = new CapturingConfigurer();
        channel = channelWith("/rpc/*", configurer);

        channel.writeInbound(request("/rpc/Service/Method"));
        writeResponseEnd();

        assertThat(channel.pipeline().get(configurer.lastHandlerName)).isNull();
        assertThat(channel.pipeline().get(Http1DispatchHandler.class)).isNotNull();
    }

    @Test
    void secondRequestProcessedAfterCleanup() {
        CapturingConfigurer configurer = new CapturingConfigurer();
        channel = channelWith("/rpc/*", configurer);

        channel.writeInbound(request("/rpc/First"));
        writeResponseEnd();
        channel.writeInbound(request("/rpc/Second"));

        assertThat(configurer.configureCalls).isEqualTo(2);
        assertThat(configurer.seenUris).containsExactly("/rpc/First", "/rpc/Second");
    }

    @Test
    void pipelinedSecondRequestClosesChannel() {
        CapturingConfigurer configurer = new CapturingConfigurer();
        channel = channelWith("/rpc/*", configurer);

        channel.writeInbound(request("/rpc/First"));
        channel.writeInbound(request("/rpc/Second"));

        assertThat(channel.isOpen()).isFalse();
        assertThat(configurer.seenUris).containsExactly("/rpc/First");
    }

    @Test
    void channelInactiveDuringInFlightRemovesProtocolHandler() {
        CapturingConfigurer configurer = new CapturingConfigurer();
        channel = channelWith("/rpc/*", configurer);

        channel.writeInbound(request("/rpc/Service/Method"));
        channel.close().syncUninterruptibly();

        assertThat(channel.pipeline().get(configurer.lastHandlerName)).isNull();
    }

    @Test
    void exceptionDuringInFlightClosesChannel() {
        channel = channelWith("/rpc/*", new CapturingConfigurer());

        channel.writeInbound(request("/rpc/Service/Method"));
        channel.pipeline().fireExceptionCaught(new RuntimeException("boom"));

        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    void unknownProtocolKeepsChannelOpen() {
        CapturingConfigurer configurer = new CapturingConfigurer();
        channel = channelWith("/rpc/*", configurer);

        channel.writeInbound(request("/missing"));

        FullHttpResponse response = channel.readOutbound();
        try {
            assertThat(response.status()).isEqualTo(HttpResponseStatus.NOT_FOUND);
        } finally {
            response.release();
        }
        assertThat(channel.isOpen()).isTrue();

        channel.writeInbound(request("/rpc/Service/Method"));

        assertThat(configurer.configureCalls).isEqualTo(1);
        assertThat(configurer.seenUris).containsExactly("/rpc/Service/Method");
    }

    @Test
    void unsupportedHttp1TransportKeepsChannelOpen() {
        AppProtocolRegistry registry = new AppProtocolRegistry();
        registry.register("/rpc/*", new AppProtocol() {});
        channel = new EmbeddedChannel(new Http1DispatchHandler(registry));

        channel.writeInbound(request("/rpc/Service/Method"));

        FullHttpResponse response = channel.readOutbound();
        try {
            assertThat(response.status()).isEqualTo(HttpResponseStatus.HTTP_VERSION_NOT_SUPPORTED);
        } finally {
            response.release();
        }
        assertThat(channel.isOpen()).isTrue();
    }

    @Test
    void clientConnectionCloseClosesAfterResponseEnd() {
        CapturingConfigurer configurer = new CapturingConfigurer();
        channel = channelWith("/rpc/*", configurer);
        DefaultFullHttpRequest request = request("/rpc/Service/Method");
        request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);

        channel.writeInbound(request);
        writeResponse(HttpHeaderValues.KEEP_ALIVE);
        writeResponseEnd();

        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    void protocolConnectionCloseClosesAfterResponseEnd() {
        channel = channelWith("/rpc/*", new CapturingConfigurer());

        channel.writeInbound(request("/rpc/Service/Method"));
        writeResponse(HttpHeaderValues.CLOSE);
        writeResponseEnd();

        assertThat(channel.isOpen()).isFalse();
    }

    private void writeResponse(AsciiString connection) {
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        response.headers().set(HttpHeaderNames.CONNECTION, connection);
        channel.writeOutbound(response);
        Object outbound = channel.readOutbound();
        ReferenceCountUtil.release(outbound);
    }

    private void writeResponseEnd() {
        LastHttpContent content = new DefaultLastHttpContent();
        channel.writeOutbound(content);
        Object outbound = channel.readOutbound();
        ReferenceCountUtil.release(outbound);
    }

    private static EmbeddedChannel channelWith(String pattern, CapturingConfigurer configurer) {
        AppProtocolRegistry registry = new AppProtocolRegistry();
        registry.register(pattern, new AppProtocol() {
            @Override
            public AppChannelConfigurer http1() {
                return configurer;
            }
        });
        return new EmbeddedChannel(new Http1DispatchHandler(registry));
    }

    private static DefaultFullHttpRequest request(String uri) {
        return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri);
    }

    private static final class CapturingConfigurer implements AppChannelConfigurer {
        private final List<String> seenUris = new ArrayList<>();
        private int configureCalls;
        private @Nullable String lastHandlerName;

        @Override
        public void configure(Channel channel) {
            configureCalls++;
            lastHandlerName = "captor-" + configureCalls;
            channel.pipeline().addLast(lastHandlerName, new ChannelInboundHandlerAdapter() {
                @Override
                public void channelRead(ChannelHandlerContext ctx, Object msg) {
                    if (msg instanceof DefaultFullHttpRequest request) {
                        seenUris.add(request.uri());
                    }
                    ReferenceCountUtil.release(msg);
                }
            });
        }
    }
}
