package io.sparkadvisor.core.locate;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StatementIdExtractorTest {

    private final StatementIdExtractor extractor = new StatementIdExtractor();

    @Test
    void extractsLeadingStatementId() {
        Optional<String> id = extractor.extract("/* 20260521_abc123 */ select * from t");
        assertEquals(Optional.of("20260521_abc123"), id);
    }

    @Test
    void toleratesLeadingWhitespaceAndInnerSpaces() {
        Optional<String> id = extractor.extract("   /*   stmt_42   */\nselect 1");
        assertEquals(Optional.of("stmt_42"), id);
    }

    @Test
    void extractsStatementIdContainingSpacesAndUuid() {
        Optional<String> id = extractor.extract(
                "/* DAC c99ddc63-770f-49e6-8e84-3587cd372a82-1779951600017 */ select 1");
        assertEquals(Optional.of("DAC c99ddc63-770f-49e6-8e84-3587cd372a82-1779951600017"), id);
    }

    @Test
    void normalizesWhitespaceInsideStatementId() {
        Optional<String> id = extractor.extract("/*  DAC   c99ddc63  */ select 1");
        assertEquals(Optional.of("DAC c99ddc63"), id);
    }

    @Test
    void returnsEmptyWhenNoComment() {
        assertTrue(extractor.extract("select * from orders where id = 1").isEmpty());
    }

    @Test
    void ignoresCommentInTheMiddleOfSql() {
        // A comment that is NOT at the front must not be picked up.
        Optional<String> id = extractor.extract("select 1 /* not_the_id */ from t");
        assertTrue(id.isEmpty());
    }

    @Test
    void handlesNullAndEmpty() {
        assertTrue(extractor.extract(null).isEmpty());
        assertTrue(extractor.extract("").isEmpty());
    }

    @Test
    void picksFirstWhenMultipleCommentsAndFirstIsLeading() {
        Optional<String> id = extractor.extract("/* first_id */ select /* second */ 1");
        assertEquals(Optional.of("first_id"), id);
    }

    @Test
    void prefersPrimaryDescriptionOverThriftStatement() {
        // description present -> use it
        Optional<String> id = extractor.extractPreferring(
                "/* from_description */ select 1",
                "/* from_thrift */ select 1");
        assertEquals(Optional.of("from_description"), id);
    }

    @Test
    void fallsBackToThriftStatementWhenDescriptionHasNoComment() {
        Optional<String> id = extractor.extractPreferring(
                "select 1",                       // no leading comment
                "/* from_thrift */ select 1");
        assertEquals(Optional.of("from_thrift"), id);
    }

    @Test
    void supportsCustomIdCharClass() {
        // Allow dots in the id
        StatementIdExtractor dotted = new StatementIdExtractor("A-Za-z0-9_.\\-");
        Optional<String> id = dotted.extract("/* job.2026.05 */ select 1");
        assertEquals(Optional.of("job.2026.05"), id);
    }
}
