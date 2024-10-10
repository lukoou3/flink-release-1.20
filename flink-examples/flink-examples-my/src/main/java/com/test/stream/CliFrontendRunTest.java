package com.test.stream;

import org.apache.flink.client.cli.CliFrontend;

/**
 * 参数：
 *   run -t yarn-per-job -d -Djobmanager.memory.process.size=1g -Dtaskmanager.memory.process.size=1g -Dtaskmanager.memory.managed.size=0m -Dyarn.provided.lib.dirs=/lfc/flinklib-1.20 -p 2 -c com.test.stream.SimpleEtlTest D:\WorkSpace\flink-release-1.20\flink-examples\flink-examples-my\target\flink-examples-my-1.20-SNAPSHOT.jar aa bb
 *
 * 环境变量：
 *   FLINK_CONF_DIR=D:\java-apps\flink-1.20.0\conf
 *
 * config.yaml:
 *   yarn.flink-dist-jar: /aa/flink-dist.jar
 */
public class CliFrontendRunTest {

    public static void main(String[] args) {
        CliFrontend.main(args);
    }

}
