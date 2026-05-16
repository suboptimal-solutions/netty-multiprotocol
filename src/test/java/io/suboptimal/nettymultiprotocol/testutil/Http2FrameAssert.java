package io.suboptimal.nettymultiprotocol.testutil;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http2.Http2DataFrame;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import org.assertj.core.api.AbstractAssert;

public class Http2FrameAssert {

    public static HeadersAssert assertThat(Http2HeadersFrame frame) {
        return new HeadersAssert(frame);
    }

    public static DataAssert assertThat(Http2DataFrame frame) {
        return new DataAssert(frame);
    }

    public static class HeadersAssert extends AbstractAssert<HeadersAssert, Http2HeadersFrame> {
        private HeadersAssert(Http2HeadersFrame frame) {
            super(frame, HeadersAssert.class);
        }

        public HeadersAssert hasStatus(HttpResponseStatus expected) {
            isNotNull();
            if (!actual.headers().status().equals(expected.codeAsText())) {
                failWithMessage("Expected :status <%s> but was <%s>", expected.codeAsText(), actual.headers().status());
            }
            return this;
        }
    }

    public static class DataAssert extends AbstractAssert<DataAssert, Http2DataFrame> {
        private DataAssert(Http2DataFrame frame) {
            super(frame, DataAssert.class);
        }

        public DataAssert isEndStream() {
            isNotNull();
            if (!actual.isEndStream()) {
                failWithMessage("Expected DATA frame with endStream=true but was false");
            }
            return this;
        }
    }
}
