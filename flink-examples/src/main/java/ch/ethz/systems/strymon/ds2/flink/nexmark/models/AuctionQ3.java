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

public class AuctionQ3 implements KnownSize, Serializable {
    private static final Coder<Long> LONG_CODER = VarLongCoder.of();
    public static final Coder<AuctionQ3> CODER = new CustomCoder<AuctionQ3>() {
        public void encode(AuctionQ3 value, OutputStream outStream) throws CoderException, IOException {
            LONG_CODER.encode(value.id, outStream);
            LONG_CODER.encode(value.seller, outStream);
        }

        public AuctionQ3 decode(InputStream inStream) throws CoderException, IOException {
            long id = (Long) LONG_CODER.decode(inStream);
            long seller = (Long) LONG_CODER.decode(inStream);
            return new AuctionQ3(id, seller);
        }
    };
    @JsonProperty
    public final long id;
    @JsonProperty
    public final long seller;

    private AuctionQ3() {
        this.id = 0;
        this.seller = 0L;
    }

    public AuctionQ3(long id, long seller) {
        this.id = id;
        this.seller = seller;
    }

    public long sizeInBytes() {
        return 8 + 8;
    }

    public String toString() {
        try {
            return NexmarkUtils.MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException var2) {
            throw new RuntimeException(var2);
        }
    }
}
