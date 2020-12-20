package com.tistory.hskimsky.mapreduce.flatbuf;

import com.google.flatbuffers.FlatBufferBuilder;
import com.tistory.hskimsky.mapreduce.AbstractMapper;

/**
 * @author hskimsky
 * @version 1.0
 * @since 2020-12-21
 */
public class FlatbufMapper extends AbstractMapper {

  @Override
  protected byte[] getBytes(String[] tokens) {
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
