package io.sparkadvisor.core.finding;

import io.sparkadvisor.core.util.ValueObjects;

import java.util.Objects;

public final class Recommendation {
    /**
     * Action type used by rules.md. The two legacy values remain for JSON compatibility with
     * reports produced before the stable S/Q/DQ rule ids were introduced.
     */
    public enum Type {
        SESSION_SET,
        RESTART_CONF,
        REWRITE,
        GOVERNANCE,
        SQL_REWRITE,
        SPARK_CONF
    }
    private final Type type; private final String action,rationale,expectedImpact;
    public Recommendation(Type type,String action,String rationale,String expectedImpact){this.type=type;this.action=action;this.rationale=rationale;this.expectedImpact=expectedImpact;}
    public Type type(){return type;} public String action(){return action;} public String rationale(){return rationale;} public String expectedImpact(){return expectedImpact;}
    public static Recommendation conf(String action,String rationale,String expectedImpact){return new Recommendation(Type.SPARK_CONF,action,rationale,expectedImpact);} public static Recommendation sql(String action,String rationale,String expectedImpact){return new Recommendation(Type.SQL_REWRITE,action,rationale,expectedImpact);}
    public static Recommendation session(String action,String rationale,String expectedImpact){return new Recommendation(Type.SESSION_SET,action,rationale,expectedImpact);}
    public static Recommendation restart(String action,String rationale,String expectedImpact){return new Recommendation(Type.RESTART_CONF,action,rationale,expectedImpact);}
    public static Recommendation rewrite(String action,String rationale,String expectedImpact){return new Recommendation(Type.REWRITE,action,rationale,expectedImpact);}
    public static Recommendation governance(String action,String rationale,String expectedImpact){return new Recommendation(Type.GOVERNANCE,action,rationale,expectedImpact);}
    @Override public boolean equals(Object o){if(this==o)return true; if(!(o instanceof Recommendation))return false; Recommendation that=(Recommendation)o; return type==that.type&&Objects.equals(action,that.action)&&Objects.equals(rationale,that.rationale)&&Objects.equals(expectedImpact,that.expectedImpact);} @Override public int hashCode(){return Objects.hash(type,action,rationale,expectedImpact);}
    @Override public String toString(){return ValueObjects.toString(this);} }
