package com.tistory.hskimsky.mapreduce.protobuf;

import com.tistory.hskimsky.mapreduce.AbstractMapper;

/**
 * @author hskimsky
 * @version 1.0
 * @since 2020-12-21
 */
public class ProtobufMapper extends AbstractMapper {

  @Override
  protected byte[] getBytes(String[] tokens) {
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
}
