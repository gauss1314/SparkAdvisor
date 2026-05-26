package io.sparkadvisor.core.util;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Objects;

/**
 * Restores Java record-like value semantics for Java 8 POJOs.
 */
public final class ValueObjects {

    private ValueObjects() {}

    public static boolean equalFields(Object self, Object other) {
        if (self == other) {
            return true;
        }
        if (other == null || self.getClass() != other.getClass()) {
            return false;
        }
        for (Field field : fields(self.getClass())) {
            if (!Objects.deepEquals(value(field, self), value(field, other))) {
                return false;
            }
        }
        return true;
    }

    public static int hashFields(Object self) {
        int result = 1;
        for (Field field : fields(self.getClass())) {
            result = 31 * result + Objects.hashCode(value(field, self));
        }
        return result;
    }

    public static String toString(Object self) {
        StringBuilder sb = new StringBuilder(self.getClass().getSimpleName()).append("[");
        Field[] fields = fields(self.getClass());
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(fields[i].getName()).append("=").append(value(fields[i], self));
        }
        return sb.append("]").toString();
    }

    private static Field[] fields(Class<?> type) {
        Field[] declared = type.getDeclaredFields();
        java.util.List<Field> out = new java.util.ArrayList<Field>(declared.length);
        for (Field field : declared) {
            int modifiers = field.getModifiers();
            if (!Modifier.isStatic(modifiers) && !field.isSynthetic()) {
                field.setAccessible(true);
                out.add(field);
            }
        }
        return out.toArray(new Field[out.size()]);
    }

    private static Object value(Field field, Object target) {
        try {
            return field.get(target);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Cannot read field " + field.getName(), e);
        }
    }
}
