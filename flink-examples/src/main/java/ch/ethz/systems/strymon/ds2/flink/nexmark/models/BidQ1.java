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

public class BidQ1 implements KnownSize, Serializable {
    private static final Coder<Long> LONG_CODER = VarLongCoder.of();
    private static final Coder<String> STRING_CODER = StringUtf8Coder.of();
    public static final Coder<BidQ1> CODER = new CustomCoder<BidQ1>() {
        public void encode(BidQ1 value, OutputStream outStream) throws CoderException, IOException {
            BidQ1.LONG_CODER.encode(value.auction, outStream);
            BidQ1.LONG_CODER.encode(value.bidder, outStream);
            BidQ1.LONG_CODER.encode(value.price, outStream);
            BidQ1.STRING_CODER.encode(value.dateTime, outStream);
            BidQ1.STRING_CODER.encode(value.extra, outStream);
        }

        public BidQ1 decode(InputStream inStream) throws CoderException, IOException {
            long auction = (Long) BidQ1.LONG_CODER.decode(inStream);
            long bidder = (Long) BidQ1.LONG_CODER.decode(inStream);
            long price = (Long) BidQ1.LONG_CODER.decode(inStream);
            String dateTime = (String) BidQ1.STRING_CODER.decode(inStream);
            String extra = (String) BidQ1.STRING_CODER.decode(inStream);
            return new BidQ1(auction, bidder, price, dateTime, extra);
        }

        public void verifyDeterministic() throws NonDeterministicException {
        }
    };
    @JsonProperty
    public final long auction;
    @JsonProperty
    public final long bidder;
    @JsonProperty
    public final long price;
    @JsonProperty
    public final String dateTime;
    @JsonProperty
    public final String extra;

    private BidQ1() {
        this.auction = 0L;
        this.bidder = 0L;
        this.price = 0L;
        this.dateTime = null;
        this.extra = null;
    }

    public BidQ1(long auction, long bidder, long price, String dateTime, String extra) {
        this.auction = auction;
        this.bidder = bidder;
        this.price = price;
        this.dateTime = dateTime;
        this.extra = extra;
    }

    public BidQ1 withAnnotation(String annotation) {
        return new BidQ1(this.auction, this.bidder, this.price, this.dateTime, annotation + ": " + this.extra);
    }

    public boolean hasAnnotation(String annotation) {
        return this.extra.startsWith(annotation + ": ");
    }

    public BidQ1 withoutAnnotation(String annotation) {
        return this.hasAnnotation(annotation) ? new BidQ1(this.auction, this.bidder, this.price, this.dateTime, this.extra.substring(annotation.length() + 2)) : this;
    }

    public long sizeInBytes() {
        return (long)(32 + this.extra.length() + 1);
    }

    public String toString() {
        try {
            return NexmarkUtils.MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException var2) {
            throw new RuntimeException(var2);
        }
    }
}
