# Deploying the SparkAdvisor History Server tab (M3)

SparkAdvisor integrates with the Spark History Server (SHS) as a **tab**, using Spark's
official extension point `org.apache.spark.status.AppHistoryServerPlugin`. It is discovered
automatically via Java `ServiceLoader` — **no Spark config or launcher changes are needed**,
just the jar on the SHS classpath. This is the same mechanism Spark SQL's own tab uses.

## 1. Build the plugin jar

On a host with Maven Central access:

```bash
mvn -q -DskipTests -pl sparkadvisor-ui-plugin -am package
```

Produces a fat-jar (our engine + jackson + t-digest; Spark/Hadoop stay `provided`):

```
sparkadvisor-ui-plugin/target/sparkadvisor-ui-plugin.jar
```

## 2. Install into the History Server classpath

Copy the jar where the SHS will load it, e.g. into Spark's `jars/` dir or a dir you add to
the SHS classpath:

```bash
cp sparkadvisor-ui-plugin/target/sparkadvisor-ui-plugin.jar  $SPARK_HOME/jars/
```

(On FusionInsight: `SPARK_HOME=/opt/client/Spark2x/spark`.)

If you prefer not to touch `jars/`, add it via the SHS classpath instead:

```bash
export SPARK_DIST_CLASSPATH="$SPARK_DIST_CLASSPATH:/path/to/sparkadvisor-ui-plugin.jar"
```

## 3. JDK 21 module opens

The SHS process must run on JDK 21 with the same `--add-opens` flags SparkAdvisor's CLI uses
(event-log replay uses reflection into `java.base`). Add to the SHS JVM options
(`spark.history.ui.... ` is not where these go — set them on the SHS launch, e.g. via
`SPARK_HISTORY_OPTS` / `SPARK_DAEMON_JAVA_OPTS`):

```bash
export SPARK_DAEMON_JAVA_OPTS="$SPARK_DAEMON_JAVA_OPTS \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
```

## 4. Kerberos

The SHS already authenticates to HDFS to read event logs, so its process holds a valid TGT.
SparkAdvisor re-parses the log via the same Hadoop `FileSystem`/`UserGroupInformation`, which
picks up the existing ticket — **no extra Kerberos setup beyond what the SHS already has**.

## 5. Restart the History Server

```bash
$SPARK_HOME/sbin/stop-history-server.sh
$SPARK_HOME/sbin/start-history-server.sh
```

## 6. Use it

Open any application in the SHS UI. A new **SparkAdvisor** tab appears in the nav bar (after
the built-in Jobs/Stages/.../SQL tabs). On the tab:

- Enter a **StatementID** (from the leading `/* StatementID */` comment) and click *Analyze*.
- Leave it blank to analyze the application's slowest SQL.

The tab re-parses the application's event log with SparkAdvisor's own engine and renders the
full report (hard metrics, critical path, findings, predictions) inline.

URL form: `.../history/<appId>/sparkadvisor/?statementId=<ID>`

## How it works (and why this design)

- **Self-contained parse (strategy B, design §11.1)**: the plugin's `createListeners` returns
  empty — it does not piggy-back on the SHS's internal `AppStatusStore`. Instead `setupUI`
  attaches a tab that re-parses the event log with SparkAdvisor's own pipeline. This reuses
  the fully-tested core/analyzer/predictor/report stack and stays decoupled from SHS
  internals. The log is parsed lazily (only when you open the tab) and cached per application.
- **Robustness**: a failure inside the plugin is caught and logged; it never breaks the rest
  of the History UI for an application.

## Caveats / version notes

- The Spark UI classes used (`SparkUI`, `WebUITab`, `WebUIPage`, `AppHistoryServerPlugin`) are
  Spark developer/internal APIs. Code touching them is marked `// VERIFY@3.5.1`; confirm on
  first compile against Spark 3.5.1 and pin the Spark patch version.
- The tab renders its self-contained HTML body (with inlined CSS) and intentionally does not
  call `UIUtils.headerSparkPage` (its signature is the most version-brittle internal API). The
  body therefore appears without Spark's standard page header frame — an accepted tradeoff for
  robustness. See the note in `SparkAdvisorPage.render`.
