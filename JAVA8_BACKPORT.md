# Java 8 Backport Analysis (Built with JDK 21)

## 结论（先说结论）

在 **JDK 21 编译环境** 下，不能把一个大量使用 **Java 21 语法**（如 `Record 类型`、switch 表达式 `case x =>` 等）的源码，直接编译成可运行于 Java 8 的产物。

原因是：

1. `--release 8` 会同时约束 **语法** 与 **可用标准库 API**。
2. Java 21 语法在 `--release 8` 下会直接语法报错。
3. 即便只改字节码目标版本，不改源码语法，也无法通过 javac 前端语法检查。

因此要产出 Java 8 运行包，必须做**源码级回迁**，不仅是 POM 调整。

## 当前仓库中的主要阻塞点

项目存在大量 Java 16+ 语法：

- `Record 类型`
- switch 表达式箭头语法（`case ... =>`）

这些语法在 Java 8 目标下都不可用，需改为 Java 8 可编译写法（POJO + 传统 switch 等）。

## 建议迁移路径

1. **先恢复可持续构建（本次已做）**：保持 `maven.compiler.release=21`，确保主干可编译。
2. **分模块回迁源码**（按 `core -> analyzer/predictor -> report -> advisor/monitor -> cli/ui-plugin` 顺序）：
   - `Record 类型` 改为普通类（final 字段 + 构造器 + getter + equals/hashCode/toString）。
   - switch 表达式改为传统 switch 语句。
   - 避免使用 Java 9+ API。
3. 每完成一批回迁，再切一次 `--release 8` 验证，直到全量通过。

## 为什么这次不继续“硬切 release=8”

上一次改成 `--release 8` 后，你已经在实际编译中看到语法报错；这与上述语言规则一致。

所以本次修正把编译目标恢复到 21，避免仓库处于“必然无法编译”的状态。后续如需我继续，我可以直接开始第一批源码回迁（先从 `sparkadvisor-core` 的 value-type 开始）。
