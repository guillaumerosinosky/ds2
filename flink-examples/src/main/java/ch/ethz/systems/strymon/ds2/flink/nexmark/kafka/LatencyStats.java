package ch.ethz.systems.strymon.ds2.flink.nexmark.kafka;

import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

public class LatencyStats {

    public static void main(String[] args) throws Exception {
        Properties config = new Properties();
        config.put("group.id", "foo");
        config.put("bootstrap.servers", "kafka-edge1:9092");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            while (true) {
                int count = 0;
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    count ++;
                }
                if (count >= 100000) {
                    break;
                }
            }
        }

    }
}
