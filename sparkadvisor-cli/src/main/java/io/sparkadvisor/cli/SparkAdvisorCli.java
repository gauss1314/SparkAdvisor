package io.sparkadvisor.cli;

import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Top-level CLI. Subcommands:
 * <ul>
 *   <li>{@code analyze}  — parse an event log and emit an HTML/JSON report</li>
 *   <li>{@code locate}   — list SQL executions matching a StatementID (M1.5)</li>
 *   <li>{@code metrics}  — print hard metrics as JSON (M1.5)</li>
 * </ul>
 */
@Command(
        name = "sparkadvisor",
        mixinStandardHelpOptions = true,
        version = "SparkAdvisor 0.1.0",
        description = "Analyze Spark event logs and produce tuning reports.",
        subcommands = {AnalyzeCommand.class})
public final class SparkAdvisorCli implements Runnable {

    @Override
    public void run() {
        // No subcommand given: show usage.
        CommandLine.usage(this, System.out);
    }

    public static void main(String[] args) {
        int exit = new CommandLine(new SparkAdvisorCli()).execute(args);
        System.exit(exit);
    }
}
