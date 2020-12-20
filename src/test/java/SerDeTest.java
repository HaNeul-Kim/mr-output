import com.google.flatbuffers.FlatBufferBuilder;
import com.google.protobuf.InvalidProtocolBufferException;
import com.tistory.hskimsky.mapreduce.flatbuf.YellowFlat;
import com.tistory.hskimsky.mapreduce.protobuf.YellowProto;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hskimsky
 * @version 1.0
 * @since 2020-12-21
 */
public class SerDeTest {

  private static final Logger logger = LoggerFactory.getLogger(SerDeTest.class);

  @Test
  public void serializeTest() throws InvalidProtocolBufferException {
    List<String> lines = new ArrayList<>();
    lines.add("1,2020-03-01 00:31:13,2020-03-01 01:01:42,1,4.70,1,N,88,255,1,22,3,0.5,2,0,0.3,27.8,2.5");
    lines.add("2,2020-03-01 00:08:22,2020-03-01 00:08:49,1,.00,1,N,193,193,2,2.5,0.5,0.5,0,0,0.3,3.8,0");
    lines.add("1,2020-03-01 00:52:18,2020-03-01 00:59:16,1,1.10,1,N,246,90,1,6,3,0.5,1.95,0,0.3,11.75,2.5");
    lines.add("2,2020-03-01 00:47:53,2020-03-01 00:50:57,2,.87,1,N,151,238,1,5,0.5,0.5,1.76,0,0.3,10.56,2.5");
    lines.add("1,2020-03-01 00:43:19,2020-03-01 00:58:27,0,4.40,1,N,79,261,1,16.5,3,0.5,4.05,0,0.3,24.35,2.5");
    lines.add("1,2020-03-01 00:04:43,2020-03-01 00:23:17,1,3.50,1,Y,113,142,1,15,3,0.5,3.75,0,0.3,22.55,2.5");
    lines.add("1,2020-03-01 00:43:21,2020-03-01 01:14:36,1,14.10,1,Y,237,14,1,40.5,3,0.5,8.85,0,0.3,53.15,2.5");
    lines.add("1,2020-03-01 00:51:35,2020-03-01 01:00:17,1,1.00,1,N,234,114,1,7,3,0.5,1.3,0,0.3,12.1,2.5");
    lines.add("1,2020-03-01 00:13:42,2020-03-01 00:23:00,4,1.10,1,N,148,211,1,7.5,3,0.5,2,0,0.3,13.3,2.5");
    for (String line : lines) {
      Text value = new Text(line);
      String[] tokens = value.toString().split(",", -1);
      byte[] flatBytes = this.getFlatBytes(tokens);
      byte[] protoBytes = this.getProtoBytes(tokens);

      BytesWritable flatBytesWritable = new BytesWritable(flatBytes);
      BytesWritable protoBytesWritable = new BytesWritable(protoBytes);
      logger.debug("flat  {} -> {}", value.getLength(), flatBytesWritable.getLength());
      logger.debug("proto {} -> {}", value.getLength(), protoBytesWritable.getLength());

      byte[] flatCopyBytes = flatBytesWritable.copyBytes();
      byte[] protoCopyBytes = protoBytesWritable.copyBytes();
      YellowFlat yellowFlat = YellowFlat.getRootAsYellowFlat(ByteBuffer.wrap(flatCopyBytes));
      YellowProto.Yellow yellowProto = YellowProto.Yellow.parseFrom(protoCopyBytes);
      logger.debug("yellowFlat .tpepPickupDatetime = {}", yellowFlat.tpepPickupDatetime());
      logger.debug("yellowProto.tpepPickupDatetime = {}", yellowProto.getTpepPickupDatetime());
    }
  }

  private byte[] getProtoBytes(String[] tokens) {
    int passengerCount = tokens[3].length() > 0 ? Integer.parseInt(tokens[3]) : 0;
    float congestionSurcharge = tokens.length > 17 && tokens[17].length() > 0 ? Float.parseFloat(tokens[17]) : 0;
    YellowProto.Yellow yellow = YellowProto.Yellow.newBuilder().
      setVendorID(tokens[0]).
      setTpepPickupDatetime(tokens[1]).
      setTpepDropoffDatetime(tokens[2]).
      setPassengerCount(passengerCount).
      setTripDistance(Float.parseFloat(tokens[4])).
      setRateCodeID(tokens[5]).
      setStoreAndFwdFlag(tokens[6]).
      setPuLocationID(tokens[7]).
      setDoLocationID(tokens[8]).
      setPaymentType(tokens[9]).
      setFareAmount(Float.parseFloat(tokens[10])).
      setExtra(Float.parseFloat(tokens[11])).
      setMtaTax(Float.parseFloat(tokens[12])).
      setTipAmount(Float.parseFloat(tokens[13])).
      setTollsAmount(Float.parseFloat(tokens[14])).
      setImprovementSurcharge(Float.parseFloat(tokens[15])).
      setTotalAmount(Float.parseFloat(tokens[16])).
      setCongestionSurcharge(congestionSurcharge).
      build();
    return yellow.toByteArray();
  }

  private byte[] getFlatBytes(String[] tokens) {
    int passengerCount = tokens[3].length() > 0 ? Integer.parseInt(tokens[3]) : 0;
    float congestionSurcharge = tokens.length > 17 && tokens[17].length() > 0 ? Float.parseFloat(tokens[17]) : 0;

    FlatBufferBuilder flatBufferBuilder = new FlatBufferBuilder();
    int vendorIdOffset = flatBufferBuilder.createString(tokens[0]);
    int tpepPickupDatetimeOffset = flatBufferBuilder.createString(tokens[1]);
    int tpepDropoffDatetimeOffset = flatBufferBuilder.createString(tokens[2]);
    int rateCodeIDOffset = flatBufferBuilder.createString(tokens[5]);
    int storeAndFwdFlagOffset = flatBufferBuilder.createString(tokens[6]);
    int puLocationIDOffset = flatBufferBuilder.createString(tokens[7]);
    int doLocationIDOffset = flatBufferBuilder.createString(tokens[8]);
    int paymentTypeOffset = flatBufferBuilder.createString(tokens[9]);

    YellowFlat.startYellowFlat(flatBufferBuilder);
    YellowFlat.addVendorID(flatBufferBuilder, vendorIdOffset);
    YellowFlat.addTpepPickupDatetime(flatBufferBuilder, tpepPickupDatetimeOffset);
    YellowFlat.addTpepDropoffDatetime(flatBufferBuilder, tpepDropoffDatetimeOffset);
    YellowFlat.addPassengerCount(flatBufferBuilder, passengerCount);
    YellowFlat.addTripDistance(flatBufferBuilder, Float.parseFloat(tokens[4]));
    YellowFlat.addRateCodeID(flatBufferBuilder, rateCodeIDOffset);
    YellowFlat.addStoreAndFwdFlag(flatBufferBuilder, storeAndFwdFlagOffset);
    YellowFlat.addPuLocationID(flatBufferBuilder, puLocationIDOffset);
    YellowFlat.addDoLocationID(flatBufferBuilder, doLocationIDOffset);
    YellowFlat.addPaymentType(flatBufferBuilder, paymentTypeOffset);
    YellowFlat.addFareAmount(flatBufferBuilder, Float.parseFloat(tokens[10]));
    YellowFlat.addExtra(flatBufferBuilder, Float.parseFloat(tokens[11]));
    YellowFlat.addMtaTax(flatBufferBuilder, Float.parseFloat(tokens[12]));
    YellowFlat.addTipAmount(flatBufferBuilder, Float.parseFloat(tokens[13]));
    YellowFlat.addTollsAmount(flatBufferBuilder, Float.parseFloat(tokens[14]));
    YellowFlat.addImprovementSurcharge(flatBufferBuilder, Float.parseFloat(tokens[15]));
    YellowFlat.addTotalAmount(flatBufferBuilder, Float.parseFloat(tokens[16]));
    YellowFlat.addCongestionSurcharge(flatBufferBuilder, congestionSurcharge);

    int endOffset = YellowFlat.endYellowFlat(flatBufferBuilder);
    YellowFlat.finishYellowFlatBuffer(flatBufferBuilder, endOffset);

    return flatBufferBuilder.sizedByteArray();
  }
}
