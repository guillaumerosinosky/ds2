package ch.ethz.systems.strymon.ds2.flink.nexmark.sources;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class GenericJsonDeserializer<T> implements KafkaDeserializationSchema<T> {
    private final Class<T> targetType;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public GenericJsonDeserializer(Class<T> targetType) {
        this.targetType = targetType;
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    @Override
    public T deserialize(ConsumerRecord<byte[], byte[]> record) throws IOException {
        return objectMapper.readValue(record.value(), targetType);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeExtractor.getForClass(targetType);
    }
}