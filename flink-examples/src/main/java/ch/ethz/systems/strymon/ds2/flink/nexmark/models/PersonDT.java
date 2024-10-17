package ch.ethz.systems.strymon.ds2.flink.nexmark.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.KnownSize;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

public class PersonDT implements KnownSize, Serializable {
    private static final Coder<Long> LONG_CODER = VarLongCoder.of();
    private static final Coder<String> STRING_CODER = StringUtf8Coder.of();
    public static final Coder<PersonDT> CODER = new CustomCoder<PersonDT>() {
        public void encode(PersonDT value, OutputStream outStream) throws CoderException, IOException {
            PersonDT.LONG_CODER.encode(value.id, outStream);
            PersonDT.STRING_CODER.encode(value.name, outStream);
            PersonDT.STRING_CODER.encode(value.emailAddress, outStream);
            PersonDT.STRING_CODER.encode(value.creditCard, outStream);
            PersonDT.STRING_CODER.encode(value.city, outStream);
            PersonDT.STRING_CODER.encode(value.state, outStream);
            PersonDT.STRING_CODER.encode(value.dateTime, outStream);
            PersonDT.STRING_CODER.encode(value.extra, outStream);
        }

        public PersonDT decode(InputStream inStream) throws CoderException, IOException {
            long id = (Long) PersonDT.LONG_CODER.decode(inStream);
            String name = (String) PersonDT.STRING_CODER.decode(inStream);
            String emailAddress = (String) PersonDT.STRING_CODER.decode(inStream);
            String creditCard = (String) PersonDT.STRING_CODER.decode(inStream);
            String city = (String) PersonDT.STRING_CODER.decode(inStream);
            String state = (String) PersonDT.STRING_CODER.decode(inStream);
            String dateTime = (String) PersonDT.STRING_CODER.decode(inStream);
            String extra = (String) PersonDT.STRING_CODER.decode(inStream);
            return new PersonDT(id, name, emailAddress, creditCard, city, state, dateTime, extra);
        }

        public void verifyDeterministic() throws Coder.NonDeterministicException {
        }
    };
    @JsonProperty
    public final long id;
    @JsonProperty
    public final String name;
    @JsonProperty
    public final String emailAddress;
    @JsonProperty
    public final String creditCard;
    @JsonProperty
    public final String city;
    @JsonProperty
    public final String state;
    @JsonProperty
    public final String dateTime;
    @JsonProperty
    public final String extra;

    private PersonDT() {
        this.id = 0L;
        this.name = null;
        this.emailAddress = null;
        this.creditCard = null;
        this.city = null;
        this.state = null;
        this.dateTime = null;
        this.extra = null;
    }

    public PersonDT(long id, String name, String emailAddress, String creditCard, String city, String state, String dateTime, String extra) {
        this.id = id;
        this.name = name;
        this.emailAddress = emailAddress;
        this.creditCard = creditCard;
        this.city = city;
        this.state = state;
        this.dateTime = dateTime;
        this.extra = extra;
    }

    public PersonDT withAnnotation(String annotation) {
        return new PersonDT(this.id, this.name, this.emailAddress, this.creditCard, this.city, this.state, this.dateTime, annotation + ": " + this.extra);
    }

    public boolean hasAnnotation(String annotation) {
        return this.extra.startsWith(annotation + ": ");
    }

    public PersonDT withoutAnnotation(String annotation) {
        return this.hasAnnotation(annotation) ? new PersonDT(this.id, this.name, this.emailAddress, this.creditCard, this.city, this.state, this.dateTime, this.extra.substring(annotation.length() + 2)) : this;
    }

    public long sizeInBytes() {
        return (long)(8 + this.name.length() + 1 + this.emailAddress.length() + 1 + this.creditCard.length() + 1 + this.city.length() + 1 + this.state.length() + 8 + 1 + this.extra.length() + 1);
    }

    public String toString() {
        try {
            return NexmarkUtils.MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException var2) {
            throw new RuntimeException(var2);
        }
    }
}
