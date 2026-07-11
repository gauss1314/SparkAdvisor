package io.sparkadvisor.cli;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RuleConfigOptionTest {
    @Test
    void analyzeAcceptsRuleConfig() {
        AnalyzeCommand command = new AnalyzeCommand();
        new CommandLine(command).parseArgs("--path", "hdfs:///eventlog", "--rule-config", "conf.yaml");
        assertEquals("conf.yaml", command.ruleConfig);
    }

    @Test
    void queueReportAcceptsRuleConfig() {
        QueueReportCommand command = new QueueReportCommand();
        new CommandLine(command).parseArgs("--path", "hdfs:///eventlog", "--rule-config", "conf.yaml");
        assertEquals("conf.yaml", command.ruleConfig);
    }
}
