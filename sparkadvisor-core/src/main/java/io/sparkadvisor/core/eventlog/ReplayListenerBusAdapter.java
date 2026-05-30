package io.sparkadvisor.core.eventlog;

import org.apache.spark.SparkConf;
import org.apache.spark.scheduler.SparkListener;

import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Compatibility wrapper around Spark's internal {@code ReplayListenerBus}.
 *
 * <p>The open-source Spark 3.5.1 bytecode exposes a zero-argument constructor, but some
 * runtime distributions ship a binary-incompatible {@code ReplayListenerBus} without
 * {@code <init>()}. Calling {@code new ReplayListenerBus()} directly then fails before we
 * can produce a useful diagnostic. This adapter resolves the constructor and replay method
 * reflectively so SparkAdvisor can run against upstream Spark and vendor builds that
 * expose either a single-argument {@link SparkConf} or JsonProtocol constructor.
 */
final class ReplayListenerBusAdapter {

    private static final String REPLAY_LISTENER_BUS_CLASS =
            "org.apache.spark.scheduler.ReplayListenerBus";
    private static final String REPLAY_LISTENER_BUS_COMPANION =
            "org.apache.spark.scheduler.ReplayListenerBus$";
    private static final String SCALA_FUNCTION1_CLASS = "scala.Function1";
    private static final String JSON_PROTOCOL_CLASS = "org.apache.spark.util.JsonProtocol";
    private static final String EVENT_FIELD = "\"Event\"";
    private static final String SPARK_LISTENER_PREFIX = "SparkListener";
    private static final String SQL_EVENT_PREFIX = "org.apache.spark.sql.execution.ui.SparkListenerSQL";
    private static final String AQE_EVENT =
            "org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate";
    private static final String THRIFT_OP_START =
            "org.apache.spark.sql.hive.thriftserver.SparkListenerThriftServerOperationStart";

    private final Object bus;
    private final Object selectAllFilter;
    private final Method replayMethod;

    private ReplayListenerBusAdapter(Object bus, Object selectAllFilter, Method replayMethod) {
        this.bus = bus;
        this.selectAllFilter = selectAllFilter;
        this.replayMethod = replayMethod;
    }

    /** Create an adapter for the runtime Spark version. */
    static ReplayListenerBusAdapter create() {
        try {
            Class<?> busClass = Class.forName(REPLAY_LISTENER_BUS_CLASS);
            Object bus = newBus(busClass);
            Object filter = replayEventsFilter();
            Method replay = replayMethod(busClass);
            return new ReplayListenerBusAdapter(bus, filter, replay);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Unable to initialize Spark ReplayListenerBus", e);
        }
    }

    /** Register SparkAdvisor's collector with the replay bus. */
    void addListener(SparkListener listener) {
        Method addListener = findAddListener(bus.getClass(), listener);
        try {
            addListener.invoke(bus, listener);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Unable to access ReplayListenerBus.addListener", e);
        } catch (InvocationTargetException e) {
            throw rethrow("ReplayListenerBus.addListener failed", e);
        }
    }

    /** Replay one event-log stream. */
    void replay(InputStream in, String sourceName, boolean maybeTruncated) {
        try {
            // VERIFY@3.5.1: replay(InputStream, String, boolean, scala.Function1) is the stable
            // API shape in Spark 3.x. Reflection also tolerates Spark 2.x returning void.
            replayMethod.invoke(bus, in, sourceName, Boolean.valueOf(maybeTruncated), selectAllFilter);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Unable to access ReplayListenerBus.replay", e);
        } catch (InvocationTargetException e) {
            throw rethrow("ReplayListenerBus.replay failed for " + sourceName, e);
        }
    }

    private static Object newBus(Class<?> busClass) {
        List<Throwable> failures = new ArrayList<Throwable>();
        try {
            Constructor<?> constructor = busClass.getDeclaredConstructor();
            constructor.setAccessible(true);
            return constructor.newInstance();
        } catch (ReflectiveOperationException zeroArgFailure) {
            failures.add(zeroArgFailure);
        }

        try {
            Constructor<?> constructor = busClass.getDeclaredConstructor(SparkConf.class);
            constructor.setAccessible(true);
            return constructor.newInstance(new SparkConf(false));
        } catch (ReflectiveOperationException sparkConfFailure) {
            failures.add(sparkConfFailure);
        }

        try {
            // VERIFY@3.5.1: Some vendor builds expose ReplayListenerBus(JsonProtocol)
            // instead of the upstream zero-argument constructor. JsonProtocol can be a
            // private-constructor class in those builds, so jsonProtocol() resolves a
            // singleton or, as a last resort, allocates and initializes an instance.
            Class<?> jsonProtocolClass = Class.forName(JSON_PROTOCOL_CLASS);
            Constructor<?> constructor = busClass.getDeclaredConstructor(jsonProtocolClass);
            constructor.setAccessible(true);
            return constructor.newInstance(jsonProtocol(jsonProtocolClass));
        } catch (ReflectiveOperationException jsonProtocolFailure) {
            failures.add(jsonProtocolFailure);
        }

        IllegalStateException failure = new IllegalStateException(
                "Runtime Spark ReplayListenerBus exposes no supported constructor. "
                        + "Supported shapes are ReplayListenerBus(), ReplayListenerBus(SparkConf), "
                        + "and ReplayListenerBus(JsonProtocol). Available constructors: "
                        + Arrays.toString(busClass.getDeclaredConstructors()));
        for (int i = 0; i < failures.size(); i++) {
            failure.addSuppressed(failures.get(i));
        }
        throw failure;
    }

    private static Object jsonProtocol(Class<?> jsonProtocolClass) throws ReflectiveOperationException {
        Object instance = singletonFromFields(jsonProtocolClass, null, jsonProtocolClass);
        if (instance != null) {
            return initializedJsonProtocol(instance);
        }

        Class<?> companionClass = null;
        Object companionModule = null;
        try {
            companionClass = Class.forName(JSON_PROTOCOL_CLASS + "$");
            companionModule = singletonField(companionClass, "MODULE$");
            if (jsonProtocolClass.isInstance(companionModule)) {
                return initializedJsonProtocol(companionModule);
            }
        } catch (ClassNotFoundException noCompanion) {
            companionClass = null;
        } catch (NoSuchFieldException noModule) {
            companionModule = null;
        }

        instance = singletonFromFields(companionClass, companionModule, jsonProtocolClass);
        if (instance != null) {
            return initializedJsonProtocol(instance);
        }

        instance = instanceFromFactoryMethods(jsonProtocolClass, null, jsonProtocolClass);
        if (instance != null) {
            return initializedJsonProtocol(instance);
        }

        instance = instanceFromFactoryMethods(companionClass, companionModule, jsonProtocolClass);
        if (instance != null) {
            return initializedJsonProtocol(instance);
        }

        try {
            Constructor<?> constructor = jsonProtocolClass.getDeclaredConstructor();
            constructor.setAccessible(true);
            return initializedJsonProtocol(constructor.newInstance());
        } catch (NoSuchMethodException noZeroArgConstructor) {
            return initializedJsonProtocol(allocateWithoutConstructor(jsonProtocolClass));
        }
    }

    private static Object initializedJsonProtocol(Object instance) throws ReflectiveOperationException {
        if (currentMapper(instance) == null) {
            try {
                runScalaTraitInitializers(instance);
            } catch (RuntimeException initializerFailure) {
                // Fall back to manual mapper installation below.
            } catch (LinkageError initializerFailure) {
                // Fall back to manual mapper installation below.
            }
        }
        Object mapper = currentMapper(instance);
        if (mapper == null) {
            mapper = newObjectMapper();
            setMapper(instance, mapper);
        }
        if (currentMapper(instance) == null) {
            throw new IllegalStateException("Unable to initialize JsonProtocol.mapper for "
                    + instance.getClass().getName());
        }
        return instance;
    }

    private static void runScalaTraitInitializers(Object instance) throws ReflectiveOperationException {
        Class<?> type = instance.getClass();
        invokeTraitInitializer(type, instance);
        Class<?>[] interfaces = type.getInterfaces();
        for (int i = 0; i < interfaces.length; i++) {
            invokeTraitInitializer(interfaces[i], instance);
        }
        Class<?> current = type.getSuperclass();
        while (current != null) {
            invokeTraitInitializer(current, instance);
            current = current.getSuperclass();
        }
    }

    private static void invokeTraitInitializer(Class<?> traitClass, Object instance)
            throws ReflectiveOperationException {
        try {
            Method init = traitClass.getDeclaredMethod("$init$", traitClass);
            if (Modifier.isStatic(init.getModifiers())) {
                init.setAccessible(true);
                init.invoke(null, instance);
            }
        } catch (NoSuchMethodException noScala212Initializer) {
            invokeScala211TraitInitializer(traitClass, instance);
        } catch (InvocationTargetException initializerFailure) {
            throw rethrow(traitClass.getName() + ".$init$ failed", initializerFailure);
        }
    }

    private static void invokeScala211TraitInitializer(Class<?> traitClass, Object instance)
            throws ReflectiveOperationException {
        try {
            Class<?> helperClass = Class.forName(traitClass.getName() + "$class");
            Method init = helperClass.getDeclaredMethod("$init$", traitClass);
            init.setAccessible(true);
            init.invoke(null, instance);
        } catch (ClassNotFoundException noHelperClass) {
            // Not a Scala 2.11-style trait, or no initializer is needed.
        } catch (NoSuchMethodException noInitializer) {
            // No initializer is needed.
        } catch (InvocationTargetException initializerFailure) {
            throw rethrow(traitClass.getName() + "$class.$init$ failed", initializerFailure);
        }
    }

    private static Object currentMapper(Object instance) throws ReflectiveOperationException {
        try {
            Method mapperMethod = instance.getClass().getMethod("mapper");
            mapperMethod.setAccessible(true);
            return mapperMethod.invoke(instance);
        } catch (NoSuchMethodException noMapperMethod) {
            return null;
        } catch (InvocationTargetException mapperFailure) {
            throw rethrow("JsonProtocol.mapper failed", mapperFailure);
        }
    }

    private static Object newObjectMapper() throws ReflectiveOperationException {
        Object sparkMapper = sparkJsonUtilsMapper();
        if (sparkMapper != null) {
            return sparkMapper;
        }
        Class<?> mapperClass = Class.forName("com.fasterxml.jackson.databind.ObjectMapper");
        Constructor<?> constructor = mapperClass.getDeclaredConstructor();
        constructor.setAccessible(true);
        Object mapper = constructor.newInstance();
        registerScalaModuleIfPresent(mapper);
        return mapper;
    }

    private static Object sparkJsonUtilsMapper() throws ReflectiveOperationException {
        try {
            Object mapper = invokeNoArgMethod("org.apache.spark.util.JsonUtils", null, "mapper");
            if (mapper != null) {
                return mapper;
            }
        } catch (LinkageError incompatibleJsonUtils) {
            return null;
        }
        try {
            Class<?> companionClass = Class.forName("org.apache.spark.util.JsonUtils$");
            Object module = singletonField(companionClass, "MODULE$");
            return invokeNoArgMethod(companionClass.getName(), module, "mapper");
        } catch (ClassNotFoundException noCompanion) {
            return null;
        } catch (NoSuchFieldException noModule) {
            return null;
        } catch (LinkageError incompatibleJsonUtils) {
            return null;
        }
    }

    private static Object invokeNoArgMethod(String className, Object target, String methodName)
            throws ReflectiveOperationException {
        try {
            Class<?> ownerClass = Class.forName(className);
            Method method = findNoArgMethod(ownerClass, methodName);
            if (method == null) {
                return null;
            }
            boolean isStatic = Modifier.isStatic(method.getModifiers());
            if (!isStatic && target == null) {
                return null;
            }
            method.setAccessible(true);
            return method.invoke(isStatic ? null : target);
        } catch (ClassNotFoundException noClass) {
            return null;
        } catch (InvocationTargetException invocationFailure) {
            throw rethrow(className + "." + methodName + " failed", invocationFailure);
        }
    }

    private static Method findNoArgMethod(Class<?> ownerClass, String methodName) {
        Class<?> current = ownerClass;
        while (current != null) {
            try {
                Method method = current.getDeclaredMethod(methodName);
                if (method.getParameterTypes().length == 0) {
                    return method;
                }
            } catch (NoSuchMethodException noMethod) {
                // Try public lookup below and then superclass.
            }
            current = current.getSuperclass();
        }
        Method[] methods = ownerClass.getMethods();
        for (int i = 0; i < methods.length; i++) {
            Method method = methods[i];
            if (methodName.equals(method.getName()) && method.getParameterTypes().length == 0) {
                return method;
            }
        }
        return null;
    }

    private static void registerScalaModuleIfPresent(Object mapper) throws ReflectiveOperationException {
        try {
            Class<?> moduleClass = Class.forName("com.fasterxml.jackson.databind.Module");
            Class<?> scalaModuleClass = Class.forName("com.fasterxml.jackson.module.scala.DefaultScalaModule$");
            Object scalaModule = singletonField(scalaModuleClass, "MODULE$");
            Method registerModule = mapper.getClass().getMethod("registerModule", moduleClass);
            registerModule.invoke(mapper, scalaModule);
        } catch (ClassNotFoundException noScalaModule) {
            // Spark distributions that need Jackson's Scala module normally expose JsonUtils.mapper();
            // if the module is absent, keep the plain ObjectMapper fallback for non-Scala event types.
        } catch (NoSuchMethodException noRegisterModule) {
            // Older or shaded ObjectMapper variant; keep the mapper usable for basic JSON parsing.
        } catch (NoSuchFieldException noModule) {
            // Unexpected Scala module shape; keep the mapper usable for basic JSON parsing.
        } catch (InvocationTargetException registerFailure) {
            // Version-skewed Spark classpaths may expose a Scala module that refuses the
            // available jackson-databind version. This fallback mapper is best-effort; the
            // primary fix is to use Spark's own Jackson jars by not shading ours into runtime jars.
        } catch (LinkageError incompatibleScalaModule) {
            // Same best-effort fallback for binary-incompatible Jackson/Scala module pairs.
        }
    }

    private static void setMapper(Object instance, Object mapper) throws IllegalAccessException {
        Field field = mapperField(instance.getClass(), mapper.getClass());
        if (field == null) {
            throw new IllegalStateException("Unable to find JsonProtocol mapper field on "
                    + instance.getClass().getName());
        }
        field.setAccessible(true);
        field.set(instance, mapper);
    }

    private static Field mapperField(Class<?> type, Class<?> mapperClass) {
        Class<?> current = type;
        while (current != null) {
            try {
                Field field = current.getDeclaredField("mapper");
                if (field.getType().isAssignableFrom(mapperClass)) {
                    return field;
                }
            } catch (NoSuchFieldException ignored) {
                // Try type-based lookup below and then the superclass.
            }
            Field[] fields = current.getDeclaredFields();
            for (int i = 0; i < fields.length; i++) {
                if (fields[i].getType().isAssignableFrom(mapperClass)) {
                    return fields[i];
                }
            }
            current = current.getSuperclass();
        }
        return null;
    }

    private static Object singletonField(Class<?> ownerClass, String fieldName)
            throws ReflectiveOperationException {
        Field field = ownerClass.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(null);
    }

    private static Object singletonFromFields(Class<?> ownerClass, Object owner, Class<?> expectedType)
            throws IllegalAccessException {
        if (ownerClass == null) {
            return null;
        }
        Field[] fields = ownerClass.getDeclaredFields();
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            if (expectedType.isAssignableFrom(field.getType())) {
                boolean isStatic = Modifier.isStatic(field.getModifiers());
                if (isStatic || owner != null) {
                    field.setAccessible(true);
                    Object value = field.get(isStatic ? null : owner);
                    if (expectedType.isInstance(value)) {
                        return value;
                    }
                }
            }
        }
        return null;
    }

    private static Object instanceFromFactoryMethods(Class<?> ownerClass, Object owner, Class<?> expectedType)
            throws ReflectiveOperationException {
        if (ownerClass == null) {
            return null;
        }
        Method[] methods = ownerClass.getDeclaredMethods();
        for (int i = 0; i < methods.length; i++) {
            Method method = methods[i];
            if (method.getParameterTypes().length == 0 && expectedType.isAssignableFrom(method.getReturnType())) {
                boolean isStatic = Modifier.isStatic(method.getModifiers());
                if (isStatic || owner != null) {
                    method.setAccessible(true);
                    Object value = method.invoke(isStatic ? null : owner);
                    if (expectedType.isInstance(value)) {
                        return value;
                    }
                }
            }
        }
        return null;
    }

    private static Object allocateWithoutConstructor(Class<?> type) throws ReflectiveOperationException {
        // Last resort for Spark distributions where JsonProtocol is a stateless implementation
        // class with no Java-visible constructor. Keep this isolated so normal paths never rely
        // on Unsafe, but still avoid hand-parsing Spark events.
        Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
        Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
        unsafeField.setAccessible(true);
        Object unsafe = unsafeField.get(null);
        Method allocateInstance = unsafeClass.getMethod("allocateInstance", Class.class);
        return allocateInstance.invoke(unsafe, type);
    }

    private static Object replayEventsFilter() throws ReflectiveOperationException {
        final Object selectAll = selectAllFilter(Class.forName(REPLAY_LISTENER_BUS_CLASS));
        Class<?> function1Class = Class.forName(SCALA_FUNCTION1_CLASS);
        return Proxy.newProxyInstance(function1Class.getClassLoader(), new Class<?>[]{function1Class},
                new InvocationHandler() {
                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                        String name = method.getName();
                        if ("apply".equals(name) && args != null && args.length == 1) {
                            Object line = args[0];
                            if (line instanceof String && !shouldReplayBeforeJsonProtocol((String) line)) {
                                return Boolean.FALSE;
                            }
                            return invokeFunction1(selectAll, method, args);
                        }
                        if ("toString".equals(name) && (args == null || args.length == 0)) {
                            return "SparkAdvisorReplayEventsFilter";
                        }
                        if ("hashCode".equals(name) && (args == null || args.length == 0)) {
                            return Integer.valueOf(System.identityHashCode(proxy));
                        }
                        if ("equals".equals(name) && args != null && args.length == 1) {
                            return Boolean.valueOf(proxy == args[0]);
                        }
                        return invokeFunction1(selectAll, method, args);
                    }
                });
    }

    private static Object invokeFunction1(Object function, Method interfaceMethod, Object[] args) throws Throwable {
        try {
            Method targetMethod = function.getClass().getMethod(interfaceMethod.getName(), interfaceMethod.getParameterTypes());
            targetMethod.setAccessible(true);
            return targetMethod.invoke(function, args);
        } catch (NoSuchMethodException noExactMethod) {
            Method apply = function.getClass().getMethod("apply", Object.class);
            apply.setAccessible(true);
            return apply.invoke(function, args);
        } catch (InvocationTargetException invocationFailure) {
            Throwable cause = invocationFailure.getCause();
            if (cause != null) {
                throw cause;
            }
            throw invocationFailure;
        }
    }

    private static boolean shouldReplayBeforeJsonProtocol(String line) {
        String event = eventName(line);
        if (event == null) {
            return true;
        }
        if (event.startsWith(SQL_EVENT_PREFIX) || AQE_EVENT.equals(event) || THRIFT_OP_START.equals(event)) {
            return true;
        }
        if (!event.startsWith(SPARK_LISTENER_PREFIX)) {
            return false;
        }
        return "SparkListenerApplicationStart".equals(event)
                || "SparkListenerApplicationEnd".equals(event)
                || "SparkListenerEnvironmentUpdate".equals(event)
                || "SparkListenerExecutorAdded".equals(event)
                || "SparkListenerExecutorRemoved".equals(event)
                || "SparkListenerJobStart".equals(event)
                || "SparkListenerJobEnd".equals(event)
                || "SparkListenerStageSubmitted".equals(event)
                || "SparkListenerStageCompleted".equals(event)
                || "SparkListenerTaskEnd".equals(event);
    }

    private static String eventName(String line) {
        int field = line.indexOf(EVENT_FIELD);
        if (field < 0) {
            return null;
        }
        int colon = line.indexOf(':', field + EVENT_FIELD.length());
        if (colon < 0) {
            return null;
        }
        int startQuote = line.indexOf('"', colon + 1);
        if (startQuote < 0) {
            return null;
        }
        int endQuote = line.indexOf('"', startQuote + 1);
        if (endQuote < 0) {
            return null;
        }
        return line.substring(startQuote + 1, endQuote);
    }

    private static Object selectAllFilter(Class<?> busClass) throws ReflectiveOperationException {
        try {
            Method staticForwarder = busClass.getMethod("SELECT_ALL_FILTER");
            return staticForwarder.invoke(null);
        } catch (NoSuchMethodException noStaticForwarder) {
            Class<?> companionClass = Class.forName(REPLAY_LISTENER_BUS_COMPANION);
            Object module = companionClass.getField("MODULE$").get(null);
            Method companionMethod = companionClass.getMethod("SELECT_ALL_FILTER");
            return companionMethod.invoke(module);
        }
    }

    private static Method replayMethod(Class<?> busClass) throws ReflectiveOperationException {
        Class<?> function1Class = Class.forName(SCALA_FUNCTION1_CLASS);
        Method method = busClass.getMethod("replay",
                InputStream.class, String.class, Boolean.TYPE, function1Class);
        method.setAccessible(true);
        return method;
    }

    private static Method findAddListener(Class<?> busClass, SparkListener listener) {
        Method[] methods = busClass.getMethods();
        for (int i = 0; i < methods.length; i++) {
            Method method = methods[i];
            Class<?>[] parameterTypes = method.getParameterTypes();
            if ("addListener".equals(method.getName())
                    && parameterTypes.length == 1
                    && parameterTypes[0].isInstance(listener)) {
                method.setAccessible(true);
                return method;
            }
        }
        throw new IllegalStateException("Unable to find compatible ReplayListenerBus.addListener "
                + "method for listener type " + listener.getClass().getName());
    }

    private static RuntimeException rethrow(String message, InvocationTargetException e) {
        Throwable cause = e.getCause();
        if (cause instanceof RuntimeException) {
            return (RuntimeException) cause;
        }
        if (cause instanceof Error) {
            throw (Error) cause;
        }
        return new IllegalStateException(message, cause);
    }
}
