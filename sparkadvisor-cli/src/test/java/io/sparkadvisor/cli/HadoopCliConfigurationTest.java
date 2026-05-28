package io.sparkadvisor.cli;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.HadoopKerberosName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HadoopCliConfigurationTest {

    @Test
    void authToLocalOverrideMapsTicketPrincipalToShortName() throws Exception {
        Path dir = Files.createTempDirectory("sparkadvisor-hadoop-conf");
        Files.write(dir.resolve("core-site.xml"), xml("").getBytes(StandardCharsets.UTF_8));

        String rules = "RULE:[1:$1@$0](.*@HADOOP.COM)s/@.*// DEFAULT";
        Configuration conf = HadoopCliConfiguration.load(dir.toString(), rules);

        assertEquals(rules, conf.get("hadoop.security.auth_to_local"));
        assertEquals("ossuser", new HadoopKerberosName("ossuser@HADOOP.COM").getShortName());
    }

    @Test
    void explicitHadoopConfDirLoadsCoreSiteBeforeUgiInitialization() throws Exception {
        Path dir = Files.createTempDirectory("sparkadvisor-hadoop-conf");
        String rules = "RULE:[1:$1@$0](.*@HADOOP.COM)s/@.*// DEFAULT";
        Files.write(dir.resolve("core-site.xml"), xml(rules).getBytes(StandardCharsets.UTF_8));

        Configuration conf = HadoopCliConfiguration.load(dir.toString(), null);

        assertEquals(rules, conf.get("hadoop.security.auth_to_local"));
        assertEquals("ossuser", new HadoopKerberosName("ossuser@HADOOP.COM").getShortName());
    }

    private static String xml(String authToLocal) {
        StringBuilder b = new StringBuilder();
        b.append("<?xml version=\"1.0\"?>\n");
        b.append("<configuration>\n");
        if (authToLocal != null && authToLocal.length() > 0) {
            b.append("  <property>\n");
            b.append("    <name>hadoop.security.auth_to_local</name>\n");
            b.append("    <value>").append(authToLocal).append("</value>\n");
            b.append("  </property>\n");
        }
        b.append("</configuration>\n");
        return b.toString();
    }
}
