package io.sparkadvisor.analyzer.v2;

import java.time.LocalDate;

/** Rule/object suppression. Findings remain in the contract with suppressed=true. */
public final class Suppression {
    private final String ruleId,fingerprint,statementId,table,reason;
    private final LocalDate until;

    public Suppression(String ruleId,String fingerprint,String statementId,String table,String reason,LocalDate until){
        this.ruleId=ruleId;this.fingerprint=fingerprint;this.statementId=statementId;this.table=table;this.reason=reason==null?"":reason;this.until=until;
    }
    public boolean matches(String id,MetricsContext context,LocalDate today){
        if(ruleId==null||!ruleId.equals(id))return false;
        if(until!=null&&today!=null&&today.isAfter(until))return false;
        if(fingerprint!=null&&!fingerprint.equals(context.attribute("fingerprint")))return false;
        if(statementId!=null&&!statementId.equals(context.attribute("statement_id")))return false;
        if(table!=null&&!table.equals(context.attribute("table")))return false;
        return true;
    }
    public String reason(){return reason;}
}
