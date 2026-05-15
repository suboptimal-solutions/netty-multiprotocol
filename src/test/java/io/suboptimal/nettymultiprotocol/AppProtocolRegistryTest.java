package io.suboptimal.nettymultiprotocol;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AppProtocolRegistryTest {
    @Test
    void exactMatchWins() {
        AppProtocolRegistry registry = new AppProtocolRegistry();
        AppProtocol health = new NamedProtocol("health");
        AppProtocol fallback = new NamedProtocol("fallback");
        registry.register("/healthz", health);
        registry.register("/", fallback);

        assertThat(registry.resolve("/healthz")).isSameAs(health);
        assertThat(registry.resolve("/foo")).isSameAs(fallback);
    }

    @Test
    void prefixMatchWalksPathSegments() {
        AppProtocolRegistry registry = new AppProtocolRegistry();
        AppProtocol twirp = new NamedProtocol("twirp");
        registry.register("/twirp/*", twirp);

        assertThat(registry.resolve("/twirp")).isSameAs(twirp);
        assertThat(registry.resolve("/twirp/")).isSameAs(twirp);
        assertThat(registry.resolve("/twirp/svc/Method")).isSameAs(twirp);
        assertThat(registry.resolve("/twirpx")).isNull();
    }

    @Test
    void longestPrefixWins() {
        AppProtocolRegistry registry = new AppProtocolRegistry();
        AppProtocol outer = new NamedProtocol("outer");
        AppProtocol inner = new NamedProtocol("inner");
        registry.register("/a/*", outer);
        registry.register("/a/b/*", inner);

        assertThat(registry.resolve("/a/b/c")).isSameAs(inner);
        assertThat(registry.resolve("/a/b")).isSameAs(inner);
        assertThat(registry.resolve("/a/x")).isSameAs(outer);
    }

    @Test
    void exactBeatsPrefix() {
        AppProtocolRegistry registry = new AppProtocolRegistry();
        AppProtocol exact = new NamedProtocol("exact");
        AppProtocol prefix = new NamedProtocol("prefix");
        registry.register("/api/v1", exact);
        registry.register("/api/*", prefix);

        assertThat(registry.resolve("/api/v1")).isSameAs(exact);
        assertThat(registry.resolve("/api/v1/foo")).isSameAs(prefix);
        assertThat(registry.resolve("/api/v2")).isSameAs(prefix);
    }

    @Test
    void defaultFallback() {
        AppProtocolRegistry registry = new AppProtocolRegistry();
        AppProtocol fallback = new NamedProtocol("fallback");
        registry.register("/", fallback);

        assertThat(registry.resolve("/")).isSameAs(fallback);
        assertThat(registry.resolve("/anything/at/all")).isSameAs(fallback);
    }

    @Test
    void noMatchReturnsNull() {
        AppProtocolRegistry registry = new AppProtocolRegistry();

        assertThat(registry.resolve("/")).isNull();
        assertThat(registry.resolve("/foo")).isNull();
    }

    @Test
    void resolveStripsQueryString() {
        AppProtocolRegistry registry = new AppProtocolRegistry();
        AppProtocol twirp = new NamedProtocol("twirp");
        registry.register("/twirp/*", twirp);

        assertThat(registry.resolve("/twirp/svc/Method?x=1&y=2")).isSameAs(twirp);
    }

    @Test
    void registerRejectsInvalidPattern() {
        AppProtocolRegistry registry = new AppProtocolRegistry();
        AppProtocol p = new NamedProtocol("p");

        assertThatThrownBy(() -> registry.register("", p))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> registry.register("foo", p))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> registry.register("*.do", p))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> registry.register("/foo/*/bar", p))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> registry.register("/foo*", p))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void registerRejectsDuplicate() {
        AppProtocolRegistry registry = new AppProtocolRegistry();
        registry.register("/a", new NamedProtocol("a1"));
        registry.register("/b/*", new NamedProtocol("b1"));
        registry.register("/", new NamedProtocol("d1"));

        assertThatThrownBy(() -> registry.register("/a", new NamedProtocol("a2")))
            .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> registry.register("/b/*", new NamedProtocol("b2")))
            .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> registry.register("/", new NamedProtocol("d2")))
            .isInstanceOf(IllegalStateException.class);
    }

    private record NamedProtocol(String name) implements AppProtocol {
    }
}
