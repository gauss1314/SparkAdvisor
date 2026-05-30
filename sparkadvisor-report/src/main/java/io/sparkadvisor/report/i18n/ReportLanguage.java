package io.sparkadvisor.report.i18n;

import io.sparkadvisor.core.util.Strings;

import java.nio.file.Path;

/**
 * User-facing report language.
 *
 * <p>The CLI accepts {@code --lang auto|zh|en}. In {@code auto} mode the legacy
 * filename convention is preserved: paths containing {@code _zh} render Chinese reports.
 */
public enum ReportLanguage {
    EN,
    ZH;

    public boolean isChinese() {
        return this == ZH;
    }

    public static ReportLanguage fromValue(String value, ReportLanguage defaultLanguage) {
        if (Strings.isBlank(value) || "auto".equalsIgnoreCase(value.trim())) {
            return defaultLanguage == null ? EN : defaultLanguage;
        }
        String normalized = value.trim().toLowerCase();
        if ("zh".equals(normalized) || "cn".equals(normalized)
                || "zh-cn".equals(normalized) || "chinese".equals(normalized)) {
            return ZH;
        }
        if ("en".equals(normalized) || "en-us".equals(normalized)
                || "english".equals(normalized)) {
            return EN;
        }
        return defaultLanguage == null ? EN : defaultLanguage;
    }

    public static ReportLanguage resolve(String value, Path out) {
        if (!Strings.isBlank(value) && !"auto".equalsIgnoreCase(value.trim())) {
            return fromValue(value, EN);
        }
        return fromOutputPath(out);
    }

    public static ReportLanguage fromOutputPath(Path out) {
        if (out == null || out.getFileName() == null) {
            return EN;
        }
        return out.getFileName().toString().contains("_zh") ? ZH : EN;
    }

    public static ReportLanguage resolveForUi(String explicit, String configured, String acceptLanguage) {
        if (!Strings.isBlank(explicit) && !"auto".equalsIgnoreCase(explicit.trim())) {
            return fromValue(explicit, EN);
        }
        if (!Strings.isBlank(configured) && !"auto".equalsIgnoreCase(configured.trim())) {
            return fromValue(configured, EN);
        }
        if (!Strings.isBlank(acceptLanguage)
                && acceptLanguage.toLowerCase().contains("zh")) {
            return ZH;
        }
        return EN;
    }
}
