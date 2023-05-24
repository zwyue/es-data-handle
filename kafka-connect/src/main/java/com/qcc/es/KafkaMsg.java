package com.qcc.es;

import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;

import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import cn.hutool.http.HttpUtil;
import cn.hutool.json.JSON;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

public class KafkaMsg {

    public static void main(String[] args) {
        record Mail(long id,String content) {}


        List<Mail> mailList = new ArrayList<>() ;

        for (int i = 0; i < 5; i++) {
            mailList.add(new Mail(i,i+1+""));
        }

        mailList.forEach(System.out::println);
    }

    public static void connectKafkaMiddle() {

        String body = "{\"topic\":\"es_building\",\"time\":[\"2023-05-18T01:56:10.005Z\"," +
            "\"2023-05-18T01:56:40.005Z\"],\"filter\":\"none\",\"valueDeserializer\":\"String\"," +
            "\"partition\":-1,\"startTime\":1684374970005,\"endTime\":\"2023-05-18T01:56:40" +
            ".005Z\"}" ;

        HttpResponse result = HttpRequest.post("http://aliyun-kafka-001.ld-hadoop" +
            ".com:7766/message/search/time").header("X-Cluster-Info-Id","1").body(body).execute() ;

        JSONObject resultJson = JSONUtil.parseObj(result.body());


        resultJson.getJSONObject("data").getJSONArray("data").forEach(a->{
            JSONObject partition = (JSONObject) a ;
            String detailBody =
                "{\"topic\":\"es_building\",\"partition\":"+partition.getInt("partition")+"," +
                "\"offset" +
                "\":"+partition.getInt("offset")+"," +
                "\"timestamp\":"+partition.getInt("timestamp")+",\"keyDeserializer\":\"String\"," +
                "\"valueDeserializer\":\"String\"}" ;

            HttpResponse detailResult = HttpRequest.post("http://aliyun-kafka-001.ld-hadoop.com:7766/message/search/detail")
                .header("X-Cluster-Info-Id","1").body(detailBody).execute() ;

            JSONObject jsonDetail = JSONUtil.parseObj(detailResult.body());
            String value = jsonDetail.getJSONObject("data").get("value",String.class);
            JSONObject jsonValue = JSONUtil.parseObj(value);

            if(Objects.equals(jsonValue.getStr("esId"),"5c5aa3ef34894722b952e15a09319ac3")) {
                System.out.println("partition:"+partition.getInt("partition"));
                System.out.println(jsonValue);
            }

            if(Objects.equals(jsonValue.getStr("esId"),"49401bd66401460aa7cea472be9e2e18")) {
                System.out.println("partition:"+partition.getInt("partition"));
                System.out.println(jsonValue);
            }
        });
    }
    //5c5aa3ef34894722b952e15a09319ac3 1684373493 1684374970 1684374970 1684374970

    public static void findMsg(String[] args) {


        Properties properties = new Properties();

        //配置Kafka实例的连接地址
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
            KafkaServer.ALIYUN_SERVER.getServer());
        properties.put(CommonClientConfigs.GROUP_ID_CONFIG, "es_building_test");
        properties.put("enable.auto.commit", "false");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
//        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        properties.setProperty("key.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer");

        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        properties.put(SASL_MECHANISM, "SCRAM-SHA-256");
        properties.put(SASL_JAAS_CONFIG,
            "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"admin\" password=\"admin\";");


        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {

//            TopicPartition topicPartition = new TopicPartition("es_building", 4);
//            // 5.给Consumer指定消费的Topic和Partition(十分重要)
//            // 如果无对应topic或者partition,则会抛出异常IllegalArgumentException
//            // 如果此consumer之前已经有过订阅行为且未解除之前所有的订阅,则会抛出异常IllegalStateException
//            consumer.assign(Collections.singletonList(topicPartition));
//            // 6.覆盖原始的Consumer-Topic-Partition对应的Offset,将其设置为指定Offset值
//            consumer.seek(topicPartition, 5566472);
//            while (true) {
//                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
//                for (ConsumerRecord<String, String> record : records) {
//                    System.out.println(record.toString());
//                }
//                Thread.sleep(1000);
//            }

            // 获取topic的partition信息
            List<PartitionInfo> partitionInfos = consumer.partitionsFor("es_building");
            List<TopicPartition> topicPartitions = new ArrayList<>();

            Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
            Date now = new Date();

            System.out.println("当前时间: " + df.format(now));
            long fetchDataTime = 1684374000;  // 计算30分钟之前的时间戳

            for (PartitionInfo partitionInfo : partitionInfos) {
                topicPartitions.add(
                    new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
                timestampsToSearch.put(
                    new TopicPartition(partitionInfo.topic(), partitionInfo.partition()),
                    fetchDataTime);
            }

            consumer.assign(topicPartitions);

            // 获取每个partition一个小时之前的偏移量
            Map<TopicPartition, OffsetAndTimestamp> map =
                consumer.offsetsForTimes(timestampsToSearch);

            OffsetAndTimestamp offsetTimestamp = null;
            System.out.println("开始设置各分区初始偏移量...");
            for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : map.entrySet()) {
                // 如果设置的查询偏移量的时间点大于最大的索引记录时间，那么value就为空
                offsetTimestamp = entry.getValue();
                if (offsetTimestamp != null) {
                    int partition = entry.getKey().partition();
                    long timestamp = offsetTimestamp.timestamp();
                    long offset = offsetTimestamp.offset();
                    System.out.println("partition = " + partition +
                        ", time = " + df.format(new Date(timestamp)) +
                        ", offset = " + offset);
                    // 设置读取消息的偏移量
                    consumer.seek(entry.getKey(), offset);
                }
            }
            System.out.println("设置各分区初始偏移量结束...");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(
                        "partition = " + record.partition() + ", offset = " + record.offset());
                    if (Objects.equals("5c5aa3ef34894722b952e15a09319ac3", record.key()) ||
                        Objects.equals("49401bd66401460aa7cea472be9e2e18", record.key())
                    ) {
                        System.out.println("value == > " + record.value());
                    }
                }
                Thread.sleep(1000);
            }

        } catch (ConcurrentModificationException | WakeupException e) {

            System.out.println("Wake up exception!");
            // we ignore this as this is an expected exception when closing a consumer
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            // this will also commit the offsets if need be.
            System.out.println("The consumer is now gracefully closed.");
        }


    }
}
