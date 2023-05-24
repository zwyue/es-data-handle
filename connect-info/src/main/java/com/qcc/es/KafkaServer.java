package com.qcc.es;

import lombok.Getter;

@Getter
public enum KafkaServer {
    NONE("", "", "",""),
    ALIYUN_SERVER(
        "aliyun-kafka-001.ld-hadoop.com:9092," +
            "aliyun-kafka-002.ld-hadoop.com:9092," +
            "aliyun-kafka-003.ld-hadoop.com:9092," +
            "aliyun-kafka-004.ld-hadoop.com:9092," +
            "aliyun-kafka-005.ld-hadoop.com:9092",
        "",
        "",""
    ),
    HADOOP_SERVER(
        "kafka-100.ld-hadoop.com:9092," +
            "kafka-101.ld-hadoop.com:9092," +
            "kafka-102.ld-hadoop.com:9092," +
            "kafka-103.ld-hadoop.com:9092," +
            "kafka-104.ld-hadoop.com:9092," +
            "kafka-105.ld-hadoop.com:9092",
        "",
        "",""),
    ;

    private final String server;

    private final String password;

    private final String username;

    private final String userKey ;

    public static final String SASL_JAAS_CONFIG_FORMAT = "org.apache.kafka.common.security.scram" +
        ".ScramLoginModule required username=\"%s\" password=\"%s\";";

    KafkaServer(String server, String username, String password, String userKey) {
        this.server = server;
        this.username = username;
        this.password = password;
        this.userKey = userKey;
    }

    public static KafkaServer getByName(String serverName) {
        for (KafkaServer server : KafkaServer.values()) {
            if(serverName.equalsIgnoreCase(server.name())) {
                return server ;
            }
        }
        return NONE ;
    }
}
