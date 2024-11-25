package be.uclouvain.gepiciad;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class QueryCompiler {

    public static void main(String[] args) {
        StreamExecutionEnvironment settings = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment env = StreamTableEnvironment.create(settings);
        env.executeSql(createSource());
        env.executeSql(createSink());
        System.out.println(env.compilePlanSql(getQuery()));
    }

    public static String getQuery() {
        return "INSERT INTO sink SELECT seller FROM auction GROUP BY seller";
    }

    public static String createSource() {
        return "CREATE TABLE auction(\n" +
                "    id  BIGINT,\n" +
                "    itemName  VARCHAR,\n" +
                "    description  VARCHAR,\n" +
                "    initialBid  BIGINT,\n" +
                "    reserve  BIGINT,\n" +
                "    dateTime  TIMESTAMP(3),\n" +
                "    expires  TIMESTAMP(3),\n" +
                "    seller  BIGINT,\n" +
                "    category  BIGINT,\n" +
                "    extra  VARCHAR,\n" +
                "    WATERMARK FOR dateTime AS dateTime - INTERVAL '4' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'auction',\n" +
                "    'properties.bootstrap.servers' = 'kafka-edge1:9092',\n" +
                "    'properties.group.id' = 'nexmark',\n" +
                "    'properties.fetch.max.wait.ms' = '500',\n" +
                "    'properties.fetch.min.bytes' = '100000',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'sink.partitioner' = 'round-robin',\n" +
                "    'format' = 'json'\n" +
                ");";
    }

    public static String createSink() {
        return "CREATE TABLE sink(seller BIGINT) WITH ('connector' = 'blackhole')";
    }
}
