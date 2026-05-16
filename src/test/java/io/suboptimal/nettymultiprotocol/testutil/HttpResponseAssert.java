package io.suboptimal.nettymultiprotocol.testutil;

import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.assertj.core.api.AbstractAssert;

public class HttpResponseAssert extends AbstractAssert<HttpResponseAssert, HttpResponse> {
    private HttpResponseAssert(HttpResponse response) {
        super(response, HttpResponseAssert.class);
    }

    public static HttpResponseAssert assertThat(Object actual) {
        return new HttpResponseAssert((HttpResponse) actual);
    }

    public HttpResponseAssert hasStatus(HttpResponseStatus expected) {
        isNotNull();
        if (!actual.status().equals(expected)) {
            failWithMessage("Expected status <%s> but was <%s>", expected, actual.status());
        }
        return this;
    }
}
