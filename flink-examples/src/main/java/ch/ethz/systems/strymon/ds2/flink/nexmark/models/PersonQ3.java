package ch.ethz.systems.strymon.ds2.flink.nexmark.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.nexmark.model.KnownSize;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

public class PersonQ3 implements KnownSize, Serializable {private static final Coder<Long> LONG_CODER = VarLongCoder.of();
    private static final Coder<String> STRING_CODER = StringUtf8Coder.of();
    public static final Coder<PersonQ3> CODER = new CustomCoder<PersonQ3>() {
        public void encode(PersonQ3 value, OutputStream outStream) throws CoderException, IOException {
            LONG_CODER.encode(value.id, outStream);
            STRING_CODER.encode(value.name, outStream);
            STRING_CODER.encode(value.city, outStream);
            STRING_CODER.encode(value.state, outStream);
        }

        public PersonQ3 decode(InputStream inStream) throws CoderException, IOException {
            long id = (Long) LONG_CODER.decode(inStream);
            String name = (String) STRING_CODER.decode(inStream);
            String city = (String) STRING_CODER.decode(inStream);
            String state = (String) STRING_CODER.decode(inStream);
            return new PersonQ3(id, name, city, state);
        }

        public void verifyDeterministic() throws Coder.NonDeterministicException {
        }
    };
    @JsonProperty
    public final long id;
    @JsonProperty
    public final String name;
    @JsonProperty
    public final String city;
    @JsonProperty
    public final String state;

    private PersonQ3() {
        this.id = 0L;
        this.name = null;
        this.city = null;
        this.state = null;
    }

    public PersonQ3(long id, String name, String city, String state) {
        this.id = id;
        this.name = name;
        this.city = city;
        this.state = state;
    }

    @Override
    public long sizeInBytes() {
        return (long) (9 + this.name.length() + 1 + this.city.length()) + 1 + this.state.length() + 1;
    }
}
