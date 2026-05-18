package io.suboptimal.nettymultiprotocol.testutil;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http3.Http3HeadersFrame;
import org.assertj.core.api.AbstractAssert;

public class Http3FrameAssert {
    public static HeadersAssert assertThat(Http3HeadersFrame frame) {
        return new HeadersAssert(frame);
    }

    public static class HeadersAssert extends AbstractAssert<HeadersAssert, Http3HeadersFrame> {
        private HeadersAssert(Http3HeadersFrame frame) {
            super(frame, HeadersAssert.class);
        }

        public HeadersAssert hasStatus(HttpResponseStatus expected) {
            isNotNull();
            if (!actual.headers().status().equals(expected.codeAsText())) {
                failWithMessage("Expected :status <%s> but was <%s>",
                        expected.codeAsText(), actual.headers().status());
            }
            return this;
        }
    }
}
