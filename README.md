# SparkAdvisor

Java 21 offline analyzer and tuning advisor for **Apache Spark 3.5.1** event logs.

Reads archived event logs from HDFS, replays them through Spark's own
`ReplayListenerBus` / `JsonProtocol` (the same machinery the History Server uses),
computes the critical path and hard performance metrics, and produces a report.
SQL statements are located by the **StatementID** carried in the leading
`/* StatementID */` comment of the SQL text.

See `SparkAdvisor-design.md` for the full design and `CLAUDE.md` for contributor rules.

## Status (feature-complete: M1–M3 + F4)

Implemented and **verified at runtime** (compiled with JDK 21; Spark-free layers run real tests,
and all offline-checkable committed JUnit tests compile against the product classes):
- **core**: StatementID extraction/locate, incremental quantiles, `MetricAggregator`
  (critical path / ideal time / utilization), `CoreTimeline` (accurate cores from
  ExecutorAdded/Removed, with config fallback)
- **analyzer**: `RuleEngine` + 8 AQE-aware rules (R1 skew, R2 spill, R3 low-parallelism,
  R4 over-parallelism, R5 small-files, R6 GC, R7 broadcast, R8 scheduling)
- **predictor**: shuffle-partition cost model (skew/AQE-aware) + executor-scaling simulation;
  every prediction carries confidence + assumptions
- **report**: `AnalysisResult` contract + JSON + self-contained HTML (render / renderBody / stylesheet)
- **advisor (F4)**: `RuleBasedAdvisor` (default, offline) + `LlmAdvisor` (Anthropic provider via
  JDK HttpClient). **Both consume the structured `AnalysisResult`, never the raw log** — the
  deterministic layers parse and do arithmetic; the LLM only interprets. Graceful fallback on
  any LLM error.
- **ui-plugin (M3)**: History Server tab via `AppHistoryServerPlugin` (ServiceLoader)

Pending first compile on a Maven-enabled host (depend on provided Spark/Hadoop; signatures
checked against Spark 3.5.1 source/JavaDoc, marked `// VERIFY@3.5.1`): core eventlog layer,
`sparkadvisor-cli`, and ui-plugin's Spark-UI-coupled classes.

```bash
bin/sparkadvisor analyze --path hdfs:///.../application_xxx \
  --statement-id 20260521_abc123 --advise rule --format html --out report.html
# --advise llm  (needs ANTHROPIC_API_KEY) sends the structured analysis (not the log) to an LLM
```

See `samples/demo-report.html` for a generated report (5 findings + 9 consolidated
recommendations) and `sparkadvisor-ui-plugin/DEPLOY.md` for History Server installation.

Optional future work: multi-point regression for the cost model; precise per-task memory
budget; `UIUtils.headerSparkPage` framing; a local-model LLM provider.

## Build

> Requires a host with access to Maven Central (this dev sandbox does not have it).

```bash
mvn -q -DskipTests package      # build
mvn -q test                     # run unit tests
```

The CLI fat-jar lands at `sparkadvisor-cli/target/sparkadvisor-cli.jar`
(Spark/Hadoop are `provided`, not bundled).

## Run

On a cluster client node, as root (the launcher does `source bigdata_env` + `kinit`
and adds the JDK 21 module-open flags Spark needs):

```bash
bin/sparkadvisor analyze \
  --path hdfs:///spark2x/eventLog/application_1700000000000_0001 \
  --statement-id 20260521_abc123 \
  --format html --out ./report.html
```

Omit `--statement-id` to analyze the slowest `--top N` SQLs.
