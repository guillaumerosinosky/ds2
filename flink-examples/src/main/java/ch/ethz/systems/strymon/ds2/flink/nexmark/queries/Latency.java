package ch.ethz.systems.strymon.ds2.flink.nexmark.queries;

import ch.ethz.systems.strymon.ds2.flink.nexmark.sources.GenericJsonDeserializationSchema;
import ch.ethz.systems.strymon.ds2.flink.nexmark.sources.LatencyDeserializationSchema;
import com.google.common.primitives.Longs;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;

public class Latency {

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final int srcRate = params.getInt("srcRate", 100000);
        final float exchangeRate = params.getFloat("exchange-rate", 0.82F);

        StreamExecutionEnvironment env;
        String remoteAddress = params.get("jobmanager.rpc.address");
        if (remoteAddress == null) {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        } else {
            env = StreamExecutionEnvironment.createRemoteEnvironment(remoteAddress.split(":")[0], Integer.parseInt(remoteAddress.split(":")[1]), "flink-examples/target/flink-examples-1.0-SNAPSHOT.jar");
        }
        String kafkaAddress = params.get("kafkaAddress", "kafka-edge1:9092,localhost:9094");
        String kafkaSinkAddress = params.get("kafkaSinkAddress", "");
        String kafkaSinkTopic = params.get("kafkaSinkTopic", "result");
        String kafkaStartingOffset = params.get("kafkaStartOffset", "latest");
        OffsetsInitializer offsetsInitializer;
        if (kafkaStartingOffset.equals("latest")) {
            offsetsInitializer = OffsetsInitializer.latest();
        } else {
            offsetsInitializer = OffsetsInitializer.earliest();
        }
        final int parallelism = params.getInt("parallelism", 1);

        final int fetchMaxWaitMs = params.getInt("fetchMaxWaitMs", 500);
        final int fetchMinBytes = params.getInt("fetchMinBytes", 1);
        // fetch.max.bytes default: 55Mb max.message.bytes default: 1Mb // see if necessary

        // We keep operator chaining
        //env.disableOperatorChaining();
        env.setParallelism(parallelism);
        // enable latency tracking
        env.getConfig().setLatencyTrackingInterval(5000);

        KafkaSource<String> src = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaAddress)
                 .setTopics("auction")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setGroupId("auction")
                .setStartingOffsets(offsetsInitializer)
                .setProperty("fetch.max.wait.ms", Integer.toString(fetchMaxWaitMs))
                .setProperty("fetch.min.bytes", Integer.toString(fetchMinBytes))
                .setDeserializer(new LatencyDeserializationSchema())
                .build();

        DataStream<String> events = env.fromSource(src, WatermarkStrategy.noWatermarks(), "Auction source", TypeInformation.of(String.class));
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaSinkAddress)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(kafkaSinkTopic)
                        .setKafkaValueSerializer(StringSerializer.class)
                        .build())
                .build();
        events.sinkTo(sink);
        env.execute("Latency Job");
    }
}
