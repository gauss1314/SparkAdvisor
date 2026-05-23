package io.sparkadvisor.core.locate;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts the StatementID from the leading {@code /* StatementID *}{@code /} comment
 * of a SQL statement.
 *
 * <p>Per confirmed environment behavior, the SQL text is available in:
 * <ul>
 *   <li>{@code SparkListenerSQLExecutionStart.description} (primary), and</li>
 *   <li>{@code SparkListenerThriftServerOperationStart.statement} (STS, supplementary)</li>
 * </ul>
 *
 * <p>This class is intentionally dependency-free (no Spark types) so it is fully
 * unit-testable on its own and reusable from both event sources.
 *
 * <p>Matching rules:
 * <ul>
 *   <li>Only the comment at the very front of the (trimmed) text counts — comments
 *       elsewhere in the SQL body are ignored, so we never mis-pick a comment from
 *       inside the query.</li>
 *   <li>Leading whitespace before the comment is tolerated.</li>
 *   <li>ID charset is configurable; default {@code [A-Za-z0-9_-]+}.</li>
 *   <li>Comparison/keeping is case-sensitive (StatementIDs are typically case-sensitive).</li>
 * </ul>
 */
public final class StatementIdExtractor {

    /** Anchored at start (after optional whitespace): /* <id> *​/ */
    private static final Pattern LEADING_COMMENT =
            Pattern.compile("^\\s*/\\*\\s*([A-Za-z0-9_\\-]+)\\s*\\*/");

    private final Pattern pattern;

    public StatementIdExtractor() {
        this.pattern = LEADING_COMMENT;
    }

    /**
     * @param idCharClass a regex character class body, e.g. {@code "A-Za-z0-9_\\-"}
     */
    public StatementIdExtractor(String idCharClass) {
        this.pattern = Pattern.compile("^\\s*/\\*\\s*([" + idCharClass + "]+)\\s*\\*/");
    }

    /**
     * Extract the StatementID from a single SQL text field.
     *
     * @param sqlText description / statement text (may be null)
     * @return the StatementID, or empty if no leading comment is present
     */
    public Optional<String> extract(String sqlText) {
        if (sqlText == null || sqlText.isEmpty()) {
            return Optional.empty();
        }
        Matcher m = pattern.matcher(sqlText);
        if (m.find()) {
            return Optional.of(m.group(1));
        }
        return Optional.empty();
    }

    /**
     * Try the primary source first, then fall back to the supplementary source.
     * Useful when both {@code description} and the Thrift {@code statement} are present.
     *
     * @param primary   SQLExecutionStart.description
     * @param secondary ThriftServerOperationStart.statement (nullable)
     */
    public Optional<String> extractPreferring(String primary, String secondary) {
        Optional<String> fromPrimary = extract(primary);
        if (fromPrimary.isPresent()) {
            return fromPrimary;
        }
        return extract(secondary);
    }
}
