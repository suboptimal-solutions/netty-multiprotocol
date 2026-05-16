package io.suboptimal.nettymultiprotocol.testutil;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import org.assertj.core.api.AbstractAssert;

public class Http2HeadersFrameAssert extends AbstractAssert<Http2HeadersFrameAssert, Http2HeadersFrame> {
    private Http2HeadersFrameAssert(Http2HeadersFrame frame) {
        super(frame, Http2HeadersFrameAssert.class);
    }

    public static Http2HeadersFrameAssert assertThat(Object actual) {
        return new Http2HeadersFrameAssert((Http2HeadersFrame) actual);
    }

    public Http2HeadersFrameAssert hasStatus(HttpResponseStatus expected) {
        isNotNull();
        if (!actual.headers().status().equals(expected.codeAsText())) {
            failWithMessage("Expected :status <%s> but was <%s>", expected.codeAsText(), actual.headers().status());
        }
        return this;
    }
}
