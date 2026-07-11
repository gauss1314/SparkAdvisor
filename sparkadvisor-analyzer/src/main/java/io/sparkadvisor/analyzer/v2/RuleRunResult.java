package io.sparkadvisor.analyzer.v2;

import io.sparkadvisor.core.finding.Finding;
import io.sparkadvisor.core.util.Java8Collections;
import io.sparkadvisor.core.util.ValueObjects;

import java.util.List;
import java.util.Map;
import java.util.Set;

/** Findings plus rules that were not evaluated because required evidence was unavailable. */
public final class RuleRunResult {
    private final List<Finding> findings;
    private final Map<String,Set<Capability>> unavailableRules;
    public RuleRunResult(List<Finding> findings,Map<String,Set<Capability>> unavailableRules){this.findings=Java8Collections.listCopy(findings);this.unavailableRules=Java8Collections.mapCopy(unavailableRules);}
    public List<Finding> findings(){return findings;}
    public Map<String,Set<Capability>> unavailableRules(){return unavailableRules;}
    @Override public boolean equals(Object o){return ValueObjects.equalFields(this,o);}
    @Override public int hashCode(){return ValueObjects.hashFields(this);}
    @Override public String toString(){return ValueObjects.toString(this);}
}
