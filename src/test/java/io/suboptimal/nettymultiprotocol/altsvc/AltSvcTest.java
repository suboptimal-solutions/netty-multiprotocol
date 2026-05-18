package io.suboptimal.nettymultiprotocol.altsvc;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class AltSvcTest {
    @Nested
    class ForHttp1 {
        @Test
        void addsAltSvcHeaderToHttpResponse() {
            var ch = new EmbeddedChannel(AltSvc.forHttp1("h3=\":443\"; ma=86400"));
            var response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
            ch.writeOutbound(response);
            HttpResponse out = ch.readOutbound();
            assertThat(out.headers().get(HttpHeaderNames.ALT_SVC)).isEqualTo("h3=\":443\"; ma=86400");
            ch.finishAndReleaseAll();
        }

        @Test
        void ignoresNonHttpResponseMessages() {
            var ch = new EmbeddedChannel(AltSvc.forHttp1("h3=\":443\""));
            var buf = Unpooled.copiedBuffer("ping", StandardCharsets.UTF_8);
            ch.writeOutbound(buf);
            ByteBuf out = ch.readOutbound();
            assertThat(out).isSameAs(buf);
            ch.finishAndReleaseAll();
        }
    }

    @Nested
    class ForHttp2Stream {
        @Test
        void addsAltSvcToResponseHeadersFrame() {
            var ch = new EmbeddedChannel(AltSvc.forHttp2Stream("h3=\":443\"; ma=86400"));
            var headers = new DefaultHttp2Headers().status(HttpResponseStatus.OK.codeAsText());
            ch.writeOutbound(new DefaultHttp2HeadersFrame(headers));
            Http2HeadersFrame out = ch.readOutbound();
            assertThat(out.headers().get(HttpHeaderNames.ALT_SVC)).isEqualTo("h3=\":443\"; ma=86400");
            ch.finishAndReleaseAll();
        }

        @Test
        void doesNotAddAltSvcToTrailerHeadersFrame() {
            var ch = new EmbeddedChannel(AltSvc.forHttp2Stream("h3=\":443\""));
            var trailers = new DefaultHttp2Headers();
            trailers.add("grpc-status", "0");
            ch.writeOutbound(new DefaultHttp2HeadersFrame(trailers));
            Http2HeadersFrame out = ch.readOutbound();
            assertThat(out.headers().get(HttpHeaderNames.ALT_SVC)).isNull();
            ch.finishAndReleaseAll();
        }
    }
}
