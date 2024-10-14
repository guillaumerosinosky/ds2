package ch.ethz.systems.strymon.ds2.flink.nexmark.generator;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Properties;

import ch.ethz.systems.strymon.ds2.flink.nexmark.models.AuctionDT;
import ch.ethz.systems.strymon.ds2.flink.nexmark.models.BidDT;
import ch.ethz.systems.strymon.ds2.flink.nexmark.models.PersonDT;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import ch.ethz.systems.strymon.ds2.flink.nexmark.sources.BidSourceFunction;
import ch.ethz.systems.strymon.ds2.flink.nexmark.sources.AuctionSourceFunction;
import ch.ethz.systems.strymon.ds2.flink.nexmark.sources.PersonSourceFunction;
public class Generator {
    public static void main(String[] args) throws Exception {
        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        StreamExecutionEnvironment env;
        String remoteAddress = params.get("jobmanager.rpc.address");
        if (remoteAddress == null) {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        } else {
            env = StreamExecutionEnvironment.createRemoteEnvironment(remoteAddress.split(":")[0], Integer.parseInt(remoteAddress.split(":")[1]), "flink-examples/target/flink-examples-1.0-SNAPSHOT.jar");
        }
        String kafkaProducerAddress = params.get("kafkaProducerAddress", "kafka-edge1:9092,localhost:9094");

        final int bidRate = params.getInt("bidRate", 0);
        final int auctionRate = params.getInt("auctionRate", 0);
        final int personRate = params.getInt("personRate", 0);


        Integer parallelism = params.getInt("parallelism", 1);
        env.setParallelism(parallelism);
        //env.disableOperatorChaining();

        KafkaSink<BidDT> bidSink = KafkaSink.<BidDT>builder()
                .setBootstrapServers(kafkaProducerAddress)
                .setProperty("properties.retries", "5")
                .setProperty("metadata.max.age.ms", "3600000")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("bid")
                        .setKeySerializationSchema(new SerializationSchema<BidDT>() {
                            @Override
                            public byte[] serialize(BidDT bid) {
                                return Long.toString(bid.auction).getBytes(StandardCharsets.UTF_8);
                            }
                        })
                        .setValueSerializationSchema(new JsonSerializationSchema<>())
                        .build())
                .build();

        // Define KafkaSink for Auction
        KafkaSink<AuctionDT> auctionSink = KafkaSink.<AuctionDT>builder()
                .setBootstrapServers(kafkaProducerAddress)
                .setProperty("properties.retries", "5")
                .setProperty("metadata.max.age.ms", "3600000")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("auction")
                        .setKeySerializationSchema(new SerializationSchema<AuctionDT>() {
                            @Override
                            public byte[] serialize(AuctionDT auction) {
                                return Long.toString(auction.id).getBytes(StandardCharsets.UTF_8);
                            }
                        })
                        .setValueSerializationSchema(new JsonSerializationSchema<>())
                        .build())
                .build();
        KafkaSink<PersonDT> personSink = KafkaSink.<PersonDT>builder()
                .setBootstrapServers(kafkaProducerAddress)
                .setProperty("properties.retries", "5")
                .setProperty("metadata.max.age.ms", "3600000")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("person")
                        .setKeySerializationSchema(new SerializationSchema<PersonDT>() {
                            @Override
                            public byte[] serialize(PersonDT person) {
                                return Long.toString(person.id).getBytes(StandardCharsets.UTF_8);
                            }
                        })
                        .setValueSerializationSchema(new JsonSerializationSchema<>())
                        .build())
                .build();

        if (bidRate > 0) {
            DataStream<Bid> bidStream = env.addSource(new BidSourceFunction(bidRate))
                    .setParallelism(params.getInt("p-bid-source", 1))
                    .name("Bids Source")
                    .uid("Bids-Source")
                    .slotSharingGroup("bid");

            bidStream.map(new BidMapFunction()).sinkTo(bidSink);
        }

        if (auctionRate > 0) {
            DataStream<Auction> auctionStream = env.addSource(new AuctionSourceFunction(auctionRate))
                    .name("Custom Source: Auctions")
                    .setParallelism(params.getInt("p-auction-source", 1))
                    .slotSharingGroup("auction");

            auctionStream.map(new AuctionMapFunction()).sinkTo(auctionSink);
        }

        if (personRate > 0) {
            DataStream<Person> personStream = env.addSource(new PersonSourceFunction(personRate))
                    .name("Persons source")
                    .setParallelism(params.getInt("p-person-source", 1))
                    .slotSharingGroup("person");

            personStream.map(new PersonMapFunction()).sinkTo(personSink);
        }



        env.execute("Nexmark Kafka generator");
    }

    private static class PersonMapFunction implements MapFunction<Person, PersonDT> {

        @Override
        public PersonDT map(Person person) throws Exception {
            return new PersonDT(person.id, person.name, person.emailAddress, person.creditCard, person.city, person.state, Timestamp.from(Instant.ofEpochMilli(person.dateTime)).toString(), person.extra);
        }
    }

    private static class AuctionMapFunction implements MapFunction<Auction, AuctionDT> {

        @Override
        public AuctionDT map(Auction auction) throws Exception {
            return new AuctionDT(auction.id, auction.itemName, auction.description, auction.initialBid, auction.reserve, Timestamp.from(Instant.ofEpochMilli(auction.dateTime)).toString(), Timestamp.from(Instant.ofEpochMilli(auction.expires)).toString(), auction.seller, auction.category, auction.extra);
        }
    }

    private static class BidMapFunction implements MapFunction<Bid, BidDT> {

        @Override
        public BidDT map(Bid bid) throws Exception {
            return new BidDT(bid.auction, bid.bidder, bid.price, Timestamp.from(Instant.ofEpochMilli(bid.dateTime)).toString(), bid.extra);
        }
    }
}
