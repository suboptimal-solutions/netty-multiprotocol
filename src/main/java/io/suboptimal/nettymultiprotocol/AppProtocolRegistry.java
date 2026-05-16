package io.suboptimal.nettymultiprotocol;

import org.jspecify.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * Registry of {@link AppProtocol}s keyed by URI pattern.
 *
 * <h2>Pattern syntax</h2>
 *
 * Three pattern kinds are supported, mirroring the Servlet 3.1 spec (section 12.2):
 * <ul>
 *   <li><b>Exact</b> - e.g. {@code "/healthz"}. Matches this path and nothing else.</li>
 *   <li><b>Path prefix</b> - e.g. {@code "/dxlink/*"}. Matches {@code /dxlink},
 *       {@code /dxlink/}, and any path underneath. The longest matching prefix wins.</li>
 *   <li><b>Default</b> - exactly {@code "/"}. Fallback for anything not matched above.</li>
 * </ul>
 *
 * <h2>Resolution order</h2>
 *
 * {@link #resolve(String)} tries, in order: exact match, longest path-prefix match,
 * default. Returns {@code null} if nothing matches.
 */
public class AppProtocolRegistry {
    private final Map<String, AppProtocol> exactMatches = new HashMap<>();
    private final Map<String, AppProtocol> prefixMatches = new HashMap<>();
    private @Nullable AppProtocol defaultProtocol;

    /**
     * Registers a protocol under the given URI pattern.
     *
     * @throws IllegalArgumentException if {@code pattern} is not a valid exact,
     *         path-prefix, or default pattern
     * @throws IllegalStateException if the pattern is already registered
     */
    public void register(String pattern, AppProtocol protocol) {
        if (pattern.isEmpty()) {
            throw new IllegalArgumentException("Pattern must not be empty");
        }
        if (pattern.equals("/")) {
            if (defaultProtocol != null) {
                throw new IllegalStateException("Default pattern \"/\" is already registered");
            }
            defaultProtocol = protocol;
            return;
        }
        if (!pattern.startsWith("/")) {
            throw new IllegalArgumentException("Pattern must start with '/': " + pattern);
        }
        if (pattern.endsWith("/*")) {
            String prefix = pattern.substring(0, pattern.length() - 2);
            if (prefix.indexOf('*') >= 0) {
                throw new IllegalArgumentException("Wildcard '*' is only allowed as the last segment: " + pattern);
            }
            if (prefixMatches.containsKey(prefix)) {
                throw new IllegalStateException("Pattern already registered: " + pattern);
            }
            prefixMatches.put(prefix, protocol);
            return;
        }
        if (pattern.indexOf('*') >= 0) {
            throw new IllegalArgumentException("Only path-prefix patterns ending in '/*' are supported: " + pattern);
        }
        if (exactMatches.containsKey(pattern)) {
            throw new IllegalStateException("Pattern already registered: " + pattern);
        }
        exactMatches.put(pattern, protocol);
    }

    /**
     * Resolves a protocol for the given request URI. The query string, if any, is
     * stripped before matching. See the class javadoc for pattern semantics and
     * resolution order.
     *
     * @return the matching protocol, or {@code null} if none matches
     */
    public @Nullable AppProtocol resolve(String uri) {
        String path = stripQuery(uri);

        AppProtocol exact = exactMatches.get(path);
        if (exact != null) {
            return exact;
        }

        String candidate = path;
        while (true) {
            AppProtocol prefix = prefixMatches.get(candidate);
            if (prefix != null) {
                return prefix;
            }
            int slash = candidate.lastIndexOf('/');
            if (slash < 0) {
                break;
            }
            candidate = candidate.substring(0, slash);
        }

        return defaultProtocol;
    }

    private static String stripQuery(String uri) {
        int idx = uri.indexOf('?');
        return idx >= 0 ? uri.substring(0, idx) : uri;
    }
}
