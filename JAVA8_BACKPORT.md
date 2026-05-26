# Java 8 Backport Status (Built with JDK 21)

## 结论

当前主产物已改为 **JDK 21 编译、Java 8 bytecode 运行**：

- 父 POM：`maven.compiler.release=8`
- 测试源码：`maven.compiler.testRelease=21`
- CLI fat-jar 与 SHS 插件 jar：Spark/Hadoop 仍为 `provided`
- LLM HTTP 调用：使用 Apache HttpClient `4.5.14`，不再使用 Java 11 `java.net.http`

## 已修复的问题

- 生产源码中的 `record`/`sealed` 回迁为普通 final 类后，补齐遗漏的 Java 8 语法修复。
- 替换 `var`、text block、`Path.of`、`Files.writeString`、`CompletableFuture.orTimeout`、`URLEncoder.encode(String, Charset)` 等 Java 9+ API/语法。
- 修复上一次提交遗留的规则类语法错误与漏返回值。
- Jackson 改为按字段序列化 POJO，保持 `AnalysisResult` / `QueueAnalysisResult` JSON 契约可输出。
- `bin/sparkadvisor` 改为按 JVM 版本条件添加 `--add-opens`，避免 Java 8 运行时报不支持参数。

## 验证

- `mvn -q clean package`
- `mvn -q test`
- `target/classes` 生产类 major version 全部为 `52`
- `sparkadvisor-cli/target/sparkadvisor-cli.jar` 和 `sparkadvisor-ui-plugin/target/sparkadvisor-ui-plugin.jar` 中非 multi-release class 均不高于 major version `52`
