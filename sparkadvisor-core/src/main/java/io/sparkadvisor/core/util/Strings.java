package io.sparkadvisor.core.util;

/**
 * Java 8 equivalents for small String APIs introduced after Java 8.
 */
public final class Strings {

    private Strings() {}

    /**
     * Matches the blank-check semantics from newer JDKs.
     */
    public static boolean isBlank(String value) {
        if (value == null) {
            return true;
        }
        int i = 0;
        while (i < value.length()) {
            int codePoint = value.codePointAt(i);
            if (!Character.isWhitespace(codePoint)) {
                return false;
            }
            i += Character.charCount(codePoint);
        }
        return true;
    }
}
