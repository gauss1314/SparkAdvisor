package io.sparkadvisor.cli;

import io.sparkadvisor.core.util.Strings;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

final class HadoopCliConfiguration {

    private static final String HADOOP_CONF_DIR = "HADOOP_CONF_DIR";
    private static final String HADOOP_HOME = "HADOOP_HOME";
    private static final String AUTH_TO_LOCAL = "hadoop.security.auth_to_local";
    private static final String AUTH_TO_LOCAL_ENV = "SPARKADVISOR_AUTH_TO_LOCAL";
    private static final String[] SITE_FILES = {
            "core-site.xml",
            "hdfs-site.xml",
            "mapred-site.xml",
            "yarn-site.xml"
    };

    private HadoopCliConfiguration() {
    }

    static Configuration load(String hadoopConfDir, String authToLocal) throws IOException {
        Configuration conf = new Configuration();
        for (String dir : resolveConfDirs(hadoopConfDir)) {
            addSiteResources(conf, dir);
        }

        String rules = !Strings.isBlank(authToLocal)
                ? authToLocal
                : System.getenv(AUTH_TO_LOCAL_ENV);
        if (!Strings.isBlank(rules)) {
            conf.set(AUTH_TO_LOCAL, rules.trim(), "SparkAdvisor CLI override");
        }

        // Hadoop UGI keeps Kerberos name rules in static state. Initialize it with the
        // same Configuration used by FileSystem before the ticket-cache login is attempted.
        UserGroupInformation.setConfiguration(conf);
        return conf;
    }

    private static List<String> resolveConfDirs(String overrideDir) {
        List<String> dirs = new ArrayList<>();
        if (!Strings.isBlank(overrideDir)) {
            addSplitDirs(dirs, overrideDir);
            return dirs;
        }

        addSplitDirs(dirs, System.getenv(HADOOP_CONF_DIR));

        String hadoopHome = System.getenv(HADOOP_HOME);
        if (!Strings.isBlank(hadoopHome)) {
            dirs.add(new File(hadoopHome, "etc/hadoop").getAbsolutePath());
        }
        return dirs;
    }

    private static void addSplitDirs(List<String> dirs, String value) {
        if (Strings.isBlank(value)) {
            return;
        }
        String[] parts = value.split(java.util.regex.Pattern.quote(File.pathSeparator));
        for (String part : parts) {
            if (!Strings.isBlank(part)) {
                dirs.add(part.trim());
            }
        }
    }

    private static void addSiteResources(Configuration conf, String dir) throws IOException {
        File base = new File(dir);
        for (String name : SITE_FILES) {
            File site = new File(base, name);
            if (site.isFile()) {
                URL url = site.toURI().toURL();
                conf.addResource(url);
            }
        }
    }
}
