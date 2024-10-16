package ch.ethz.systems.strymon.ds2.flink.nexmark.generator;

import ch.ethz.systems.strymon.ds2.flink.nexmark.models.*;
import ch.ethz.systems.strymon.ds2.flink.nexmark.sources.BidSourceFunction;
import ch.ethz.systems.strymon.ds2.flink.nexmark.sources.GenericJsonDeserializationSchema;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AlloyMapper {

    public static void main(String[] args) throws Exception {
        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        StreamExecutionEnvironment env;
        String remoteAddress = params.get("jobmanager.rpc.address");
        if (remoteAddress == null) {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        } else {
            env = StreamExecutionEnvironment.createRemoteEnvironment(remoteAddress.split(":")[0], Integer.parseInt(remoteAddress.split(":")[1]), "flink-examples/target/flink-examples-1.0-SNAPSHOT.jar");
        }
        String query = params.get("query", "Q1");
        switch (query) {
            case "Q1":
                Q1(env, params);
                break;
            case "Q2":
                Q2(env, params);
                break;
            case "Q3":
                Q3(env, params);
                break;
            default:
                Q1(env, params);
                break;
        }
    }

    public static void Q1(StreamExecutionEnvironment env, MultipleParameterTool params) throws Exception {
        String kafkaAddress = params.get("kafkaAddress", "kafka-edge1:9092,localhost:9094");
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

        DataStream<BidDT> bids;
        KafkaSource<BidDT> source = KafkaSource.<BidDT>builder()
                .setBootstrapServers(kafkaAddress)
                .setTopics("bid")
                .setGroupId("bid")
                .setStartingOffsets(offsetsInitializer)
                .setDeserializer(new GenericJsonDeserializationSchema<BidDT>(BidDT.class))
                .setProperty("fetch.max.wait.ms", Integer.toString(fetchMaxWaitMs))
                .setProperty("fetch.min.bytes", Integer.toString(fetchMinBytes))
                .build();

        bids = env.fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), "bid kafka");
        KafkaSink<BidDT> sink = KafkaSink.<BidDT>builder()
                .setBootstrapServers(kafkaAddress)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("bid_q1")
                        .setValueSerializationSchema(new JsonSerializationSchema<BidDT>()).build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        bids.sinkTo(sink);
        env.execute("Nexmark Query1");
    }

    public static void Q2(StreamExecutionEnvironment env, MultipleParameterTool params) throws Exception {
        String kafkaAddress = params.get("kafkaAddress", "kafka-edge1:9092,localhost:9094");
        final int parallelism = params.getInt("parallelism", 1);

        final int fetchMaxWaitMs = params.getInt("fetchMaxWaitMs", 500);
        final int fetchMinBytes = params.getInt("fetchMinBytes", 1);
        // fetch.max.bytes default: 55Mb max.message.bytes default: 1Mb // see if necessary

        // We keep operator chaining
        //env.disableOperatorChaining();
        env.setParallelism(parallelism);
        // enable latency tracking
        env.getConfig().setLatencyTrackingInterval(5000);

        DataStream<BidDT> bids;
        KafkaSource<BidDT> source = KafkaSource.<BidDT>builder()
                .setBootstrapServers(kafkaAddress)
                .setTopics("bid")
                .setGroupId("bid")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(new GenericJsonDeserializationSchema<BidDT>(BidDT.class))
                .setProperty("fetch.max.wait.ms", Integer.toString(fetchMaxWaitMs))
                .setProperty("fetch.min.bytes", Integer.toString(fetchMinBytes))
                .build();

        bids = env.fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), "bid kafka");
        DataStream<BidDT> converted = bids.filter(new FilterFunction<BidDT>() {
                    @Override
                    public boolean filter(BidDT bid) throws Exception {
                        return bid.auction % 1007 == 0 || bid.auction % 1020 == 0 || bid.auction % 2001 == 0 || bid.auction % 2019 == 0 || bid.auction % 2087 == 0;
                    }
                }).setParallelism(params.getInt("p-flatMap", 1));

        KafkaSink<BidDT> sink = KafkaSink.<BidDT>builder()
                .setBootstrapServers(kafkaAddress)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("bid_q2")
                        .setValueSerializationSchema(new JsonSerializationSchema<BidDT>()).build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        converted.sinkTo(sink);
        env.execute("Nexmark Query2");
    }

    public static void Q3(StreamExecutionEnvironment env, MultipleParameterTool params) throws Exception {
        String kafkaAddress = params.get("kafkaAddress", "kafka-edge1:9092,localhost:9094");
        final int parallelism = params.getInt("parallelism", 1);

        final int fetchMaxWaitMs = params.getInt("fetchMaxWaitMs", 500);
        final int fetchMinBytes = params.getInt("fetchMinBytes", 1);
        // fetch.max.bytes default: 55Mb max.message.bytes default: 1Mb // see if necessary
        String kafkaStartingOffset = params.get("kafkaStartOffset", "latest");
        OffsetsInitializer offsetsInitializer;
        if (kafkaStartingOffset.equals("latest")) {
            offsetsInitializer = OffsetsInitializer.latest();
        } else {
            offsetsInitializer = OffsetsInitializer.earliest();
        }
        // We keep operator chaining
        //env.disableOperatorChaining();
        env.setParallelism(parallelism);
        // enable latency tracking
        env.getConfig().setLatencyTrackingInterval(5000);

        KafkaSource<AuctionDT> auctionSource = KafkaSource.<AuctionDT>builder()
                .setBootstrapServers(kafkaAddress)
                .setTopics("auction")
                .setGroupId("auction")
                .setStartingOffsets(offsetsInitializer)
                .setDeserializer(new GenericJsonDeserializationSchema<AuctionDT>(AuctionDT.class))
                .setProperty("fetch.max.wait.ms", Integer.toString(fetchMaxWaitMs))
                .setProperty("fetch.min.bytes", Integer.toString(fetchMinBytes))
                .build();

        KafkaSource<PersonDT> personSource = KafkaSource.<PersonDT>builder()
                .setBootstrapServers(kafkaAddress)
                .setTopics("person")
                .setGroupId("person")
                .setStartingOffsets(offsetsInitializer)
                .setDeserializer(new GenericJsonDeserializationSchema<PersonDT>(PersonDT.class))
                .setProperty("fetch.max.wait.ms", Integer.toString(fetchMaxWaitMs))
                .setProperty("fetch.min.bytes", Integer.toString(fetchMinBytes))
                .build();

        DataStream<AuctionQ3> auctions = env.fromSource(auctionSource, WatermarkStrategy.forMonotonousTimestamps(), "auctions kafka").filter(new FilterFunction<AuctionDT>() {
            @Override
            public boolean filter(AuctionDT auction) throws Exception {
                return auction.category == 10;
            }
        }).setParallelism(params.getInt("p-flatMap", 1))
                .map((MapFunction<AuctionDT, AuctionQ3>) auctionDT -> new AuctionQ3(auctionDT.id, auctionDT.seller));

        DataStream<PersonQ3> persons = env.fromSource(personSource, WatermarkStrategy.forMonotonousTimestamps(), "persons kafka").filter(new FilterFunction<PersonDT>() {
            @Override
            public boolean filter(PersonDT person) throws Exception {
                return person.state.equals("CA") || person.state.equals("ID") || person.state.equals("OR") ;
            }
        }).setParallelism(params.getInt("p-flatMap", 1))
                .map((MapFunction<PersonDT, PersonQ3>) personDT -> new PersonQ3(personDT.id, personDT.name, personDT.city, personDT.state));

        KafkaSink<AuctionQ3> auctionSink = KafkaSink.<AuctionQ3>builder()
                .setBootstrapServers(kafkaAddress)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("auction_q3")
                        .setValueSerializationSchema(new JsonSerializationSchema<AuctionQ3>()).build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        KafkaSink<PersonQ3> personSink = KafkaSink.<PersonQ3>builder()
                .setBootstrapServers(kafkaAddress)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("person_q3")
                        .setValueSerializationSchema(new JsonSerializationSchema<PersonQ3>()).build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        auctions.keyBy(a -> a.seller).sinkTo(auctionSink);
        persons.keyBy(p -> p.id).sinkTo(personSink);
        env.execute("Nexmark Query3");
    }
}
