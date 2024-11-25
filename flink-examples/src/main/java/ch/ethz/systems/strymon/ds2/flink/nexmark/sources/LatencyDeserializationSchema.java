package ch.ethz.systems.strymon.ds2.flink.nexmark.sources;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class LatencyDeserializationSchema implements KafkaRecordDeserializationSchema<String> {
    ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<String> out) throws IOException {
        out.collect(String.valueOf(System.currentTimeMillis() - record.timestamp()));
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeExtractor.getForClass(String.class);
    }
}