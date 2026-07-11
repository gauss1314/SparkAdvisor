package io.sparkadvisor.analyzer.v2;

import io.sparkadvisor.core.util.Java8Collections;
import io.sparkadvisor.core.util.ValueObjects;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Spark-free, pre-aggregated input consumed by the stable S/Q/DQ rules.
 *
 * <p>Keys intentionally mirror rules.md formulas. Missing values are different from zero:
 * {@link #has(String)} must be true before a rule may use a metric.</p>
 */
public final class MetricsContext {
    private final RuleScope scope;
    private final Long executionId;
    private final Integer stageId;
    private final Map<String, Double> numbers;
    private final Map<String, String> attributes;
    private final Set<Capability> capabilities;
    private final List<String> unavailableReasons;
    private final boolean partial;

    private MetricsContext(Builder builder) {
        this.scope = builder.scope;
        this.executionId = builder.executionId;
        this.stageId = builder.stageId;
        this.numbers = Java8Collections.mapCopy(builder.numbers);
        this.attributes = Java8Collections.mapCopy(builder.attributes);
        this.capabilities = java.util.Collections.unmodifiableSet(EnumSet.copyOf(builder.capabilities));
        this.unavailableReasons = Java8Collections.listCopy(builder.unavailableReasons);
        this.partial = builder.partial;
    }

    public RuleScope scope(){return scope;}
    public Long executionId(){return executionId;}
    public Integer stageId(){return stageId;}
    public boolean has(String key){return numbers.containsKey(key);}
    public double number(String key){Double value=numbers.get(key);return value==null?0.0:value.doubleValue();}
    public String attribute(String key){String value=attributes.get(key);return value==null?"":value;}
    public boolean attributeIs(String key,String value){return value!=null&&value.equalsIgnoreCase(attribute(key));}
    public Map<String,Double> numbers(){return numbers;}
    public Map<String,String> attributes(){return attributes;}
    public Set<Capability> capabilities(){return capabilities;}
    public boolean hasCapability(Capability capability){return capabilities.contains(capability);}
    public List<String> unavailableReasons(){return unavailableReasons;}
    public boolean partial(){return partial;}

    public static Builder builder(RuleScope scope){return new Builder(scope);}

    public static final class Builder {
        private final RuleScope scope;
        private Long executionId;
        private Integer stageId;
        private final Map<String,Double> numbers=new LinkedHashMap<String,Double>();
        private final Map<String,String> attributes=new LinkedHashMap<String,String>();
        private final EnumSet<Capability> capabilities=EnumSet.noneOf(Capability.class);
        private final List<String> unavailableReasons=new ArrayList<String>();
        private boolean partial;

        private Builder(RuleScope scope){if(scope==null)throw new IllegalArgumentException("scope");this.scope=scope;}
        public Builder executionId(long value){this.executionId=Long.valueOf(value);return this;}
        public Builder stageId(int value){this.stageId=Integer.valueOf(value);return this;}
        public Builder number(String key,double value){if(key==null)throw new IllegalArgumentException("key");numbers.put(key,Double.valueOf(value));return this;}
        public Builder attribute(String key,String value){if(key==null)throw new IllegalArgumentException("key");attributes.put(key,value==null?"":value);return this;}
        public Builder capability(Capability value){capabilities.add(value);return this;}
        public Builder capabilities(Capability... values){if(values!=null)for(Capability value:values)capabilities.add(value);return this;}
        public Builder unavailable(String reason){if(reason!=null&&!reason.trim().isEmpty())unavailableReasons.add(reason);return this;}
        public Builder partial(boolean value){this.partial=value;return this;}
        public MetricsContext build(){return new MetricsContext(this);}
    }

    @Override public boolean equals(Object o){return ValueObjects.equalFields(this,o);}
    @Override public int hashCode(){return ValueObjects.hashFields(this);}
    @Override public String toString(){return ValueObjects.toString(this);}
}
