package io.sparkadvisor.core.finding;

import java.util.Objects;

public final class Recommendation {
    public enum Type { SQL_REWRITE, SPARK_CONF }
    private final Type type; private final String action,rationale,expectedImpact;
    public Recommendation(Type type,String action,String rationale,String expectedImpact){this.type=type;this.action=action;this.rationale=rationale;this.expectedImpact=expectedImpact;}
    public Type type(){return type;} public String action(){return action;} public String rationale(){return rationale;} public String expectedImpact(){return expectedImpact;}
    public static Recommendation conf(String action,String rationale,String expectedImpact){return new Recommendation(Type.SPARK_CONF,action,rationale,expectedImpact);} public static Recommendation sql(String action,String rationale,String expectedImpact){return new Recommendation(Type.SQL_REWRITE,action,rationale,expectedImpact);} 
    @Override public boolean equals(Object o){if(this==o)return true; if(!(o instanceof Recommendation))return false; Recommendation that=(Recommendation)o; return type==that.type&&Objects.equals(action,that.action)&&Objects.equals(rationale,that.rationale)&&Objects.equals(expectedImpact,that.expectedImpact);} @Override public int hashCode(){return Objects.hash(type,action,rationale,expectedImpact);} }
