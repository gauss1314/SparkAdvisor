package io.sparkadvisor.report.html;

/** Small helpers for safe HTML output and human-readable formatting. */
final class Html {

    private Html() {}

    /** Escape text for safe insertion into HTML element content / attributes. */
    static String esc(String s) {
        if (s == null) return "";
        StringBuilder b = new StringBuilder(s.length() + 16);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '&': b.append("&amp;"); break;
                case '<': b.append("&lt;"); break;
                case '>': b.append("&gt;"); break;
                case '"': b.append("&quot;"); break;
                case '\'': b.append("&#39;"); break;
                default: b.append(c); break;
            }
        }
        return b.toString();
    }

    /** 1234567 -> "1.18 MB". Base-1024 units. */
    static String bytes(long v) {
        if (v < 1024) return v + " B";
        double d = v;
        String[] units = {"KB", "MB", "GB", "TB", "PB"};
        int i = -1;
        do {
            d /= 1024.0;
            i++;
        } while (d >= 1024.0 && i < units.length - 1);
        return String.format("%.2f %s", d, units[i]);
    }

    /** 95000 -> "1m 35s"; 4200 -> "4.2s"; 850 -> "850ms". */
    static String duration(long ms) {
        if (ms < 1000) return ms + "ms";
        long totalSec = ms / 1000;
        if (totalSec < 60) return String.format("%.1fs", ms / 1000.0);
        long min = totalSec / 60;
        long sec = totalSec % 60;
        if (min < 60) return min + "m " + sec + "s";
        long hr = min / 60;
        long m = min % 60;
        return hr + "h " + m + "m";
    }

    static String pct(double ratio) {
        return String.format("%.1f%%", ratio * 100.0);
    }

    static String ratio(double r) {
        return String.format("%.1f×", r);
    }
}
