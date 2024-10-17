package ch.ethz.systems.strymon.ds2.flink.nexmark.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.KnownSize;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

public class AuctionDT implements KnownSize, Serializable {
    private static final Coder<Long> LONG_CODER = VarLongCoder.of();
    private static final Coder<String> STRING_CODER = StringUtf8Coder.of();
    public static final Coder<AuctionDT> CODER = new CustomCoder<AuctionDT>() {
        public void encode(AuctionDT value, OutputStream outStream) throws CoderException, IOException {
            AuctionDT.LONG_CODER.encode(value.id, outStream);
            AuctionDT.STRING_CODER.encode(value.itemName, outStream);
            AuctionDT.STRING_CODER.encode(value.description, outStream);
            AuctionDT.LONG_CODER.encode(value.initialBid, outStream);
            AuctionDT.LONG_CODER.encode(value.reserve, outStream);
            AuctionDT.STRING_CODER.encode(value.dateTime, outStream);
            AuctionDT.STRING_CODER.encode(value.expires, outStream);
            AuctionDT.LONG_CODER.encode(value.seller, outStream);
            AuctionDT.LONG_CODER.encode(value.category, outStream);
            AuctionDT.STRING_CODER.encode(value.extra, outStream);
        }

        public AuctionDT decode(InputStream inStream) throws CoderException, IOException {
            long id = (Long)AuctionDT.LONG_CODER.decode(inStream);
            String itemName = (String)AuctionDT.STRING_CODER.decode(inStream);
            String description = (String)AuctionDT.STRING_CODER.decode(inStream);
            long initialBid = (Long)AuctionDT.LONG_CODER.decode(inStream);
            long reserve = (Long)AuctionDT.LONG_CODER.decode(inStream);
            String dateTime = (String)AuctionDT.STRING_CODER.decode(inStream);
            String expires = (String)AuctionDT.STRING_CODER.decode(inStream);
            long seller = (Long)AuctionDT.LONG_CODER.decode(inStream);
            long category = (Long)AuctionDT.LONG_CODER.decode(inStream);
            String extra = (String)AuctionDT.STRING_CODER.decode(inStream);
            return new AuctionDT(id, itemName, description, initialBid, reserve, dateTime, expires, seller, category, extra);
        }
    };
    @JsonProperty
    public final long id;
    @JsonProperty
    public final String itemName;
    @JsonProperty
    public final String description;
    @JsonProperty
    public final long initialBid;
    @JsonProperty
    public final long reserve;
    @JsonProperty
    public final String dateTime;
    @JsonProperty
    public final String expires;
    @JsonProperty
    public final long seller;
    @JsonProperty
    public final long category;
    @JsonProperty
    public final String extra;

    private AuctionDT() {
        this.id = 0L;
        this.itemName = null;
        this.description = null;
        this.initialBid = 0L;
        this.reserve = 0L;
        this.dateTime = null;
        this.expires = null;
        this.seller = 0L;
        this.category = 0L;
        this.extra = null;
    }

    public AuctionDT(long id, String itemName, String description, long initialBid, long reserve, String dateTime, String expires, long seller, long category, String extra) {
        this.id = id;
        this.itemName = itemName;
        this.description = description;
        this.initialBid = initialBid;
        this.reserve = reserve;
        this.dateTime = dateTime;
        this.expires = expires;
        this.seller = seller;
        this.category = category;
        this.extra = extra;
    }

    public AuctionDT withAnnotation(String annotation) {
        return new AuctionDT(this.id, this.itemName, this.description, this.initialBid, this.reserve, this.dateTime, this.expires, this.seller, this.category, annotation + ": " + this.extra);
    }

    public boolean hasAnnotation(String annotation) {
        return this.extra.startsWith(annotation + ": ");
    }

    public AuctionDT withoutAnnotation(String annotation) {
        return this.hasAnnotation(annotation) ? new AuctionDT(this.id, this.itemName, this.description, this.initialBid, this.reserve, this.dateTime, this.expires, this.seller, this.category, this.extra.substring(annotation.length() + 2)) : this;
    }

    public long sizeInBytes() {
        return (long)(8 + this.itemName.length() + 1 + this.description.length() + 1 + 8 + 8 + 8 + 8 + 8 + 8 + this.extra.length() + 1);
    }

    public String toString() {
        try {
            return NexmarkUtils.MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException var2) {
            throw new RuntimeException(var2);
        }
    }
}
