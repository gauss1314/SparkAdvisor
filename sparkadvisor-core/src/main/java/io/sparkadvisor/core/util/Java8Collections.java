package io.sparkadvisor.core.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Java 8 equivalents for the immutable collection factories used by the Java 21 code.
 */
public final class Java8Collections {

    private Java8Collections() {}

    public static <T> java.util.List<T> listCopy(Collection<? extends T> values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(new ArrayList<T>(values));
    }

    @SafeVarargs
    public static <T> java.util.List<T> listOf(T... values) {
        if (values == null || values.length == 0) {
            return Collections.emptyList();
        }
        ArrayList<T> out = new ArrayList<T>(values.length);
        Collections.addAll(out, values);
        return Collections.unmodifiableList(out);
    }

    public static <K, V> Map<K, V> mapCopy(Map<? extends K, ? extends V> values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<K, V>(values));
    }
}
