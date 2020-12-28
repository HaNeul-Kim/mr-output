package com.tistory.hskimsky.mapreduce.writable;

import com.google.common.collect.ComparisonChain;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author hskimsky
 * @version 1.0
 * @since 2020-12-28
 */
public class YellowWritable implements WritableComparable<YellowWritable> {

  private String vendorID;
  private String tpepPickupDatetime;
  private String tpepDropoffDatetime;
  private Integer passengerCount;
  private Float tripDistance;
  private String rateCodeID;
  private String storeAndFwdFlag;
  private String puLocationID;
  private String doLocationID;
  private String paymentType;
  private Float fareAmount;
  private Float extra;
  private Float mtaTax;
  private Float tipAmount;
  private Float tollsAmount;
  private Float improvementSurcharge;
  private Float totalAmount;
  private Float congestionSurcharge;

  public String getVendorID() {
    return vendorID;
  }

  public void setVendorID(String vendorID) {
    this.vendorID = vendorID;
  }

  public String getTpepPickupDatetime() {
    return tpepPickupDatetime;
  }

  public void setTpepPickupDatetime(String tpepPickupDatetime) {
    this.tpepPickupDatetime = tpepPickupDatetime;
  }

  public String getTpepDropoffDatetime() {
    return tpepDropoffDatetime;
  }

  public void setTpepDropoffDatetime(String tpepDropoffDatetime) {
    this.tpepDropoffDatetime = tpepDropoffDatetime;
  }

  public Integer getPassengerCount() {
    return passengerCount;
  }

  public void setPassengerCount(Integer passengerCount) {
    this.passengerCount = passengerCount;
  }

  public Float getTripDistance() {
    return tripDistance;
  }

  public void setTripDistance(Float tripDistance) {
    this.tripDistance = tripDistance;
  }

  public String getRateCodeID() {
    return rateCodeID;
  }

  public void setRateCodeID(String rateCodeID) {
    this.rateCodeID = rateCodeID;
  }

  public String getStoreAndFwdFlag() {
    return storeAndFwdFlag;
  }

  public void setStoreAndFwdFlag(String storeAndFwdFlag) {
    this.storeAndFwdFlag = storeAndFwdFlag;
  }

  public String getPuLocationID() {
    return puLocationID;
  }

  public void setPuLocationID(String puLocationID) {
    this.puLocationID = puLocationID;
  }

  public String getDoLocationID() {
    return doLocationID;
  }

  public void setDoLocationID(String doLocationID) {
    this.doLocationID = doLocationID;
  }

  public String getPaymentType() {
    return paymentType;
  }

  public void setPaymentType(String paymentType) {
    this.paymentType = paymentType;
  }

  public Float getFareAmount() {
    return fareAmount;
  }

  public void setFareAmount(Float fareAmount) {
    this.fareAmount = fareAmount;
  }

  public Float getExtra() {
    return extra;
  }

  public void setExtra(Float extra) {
    this.extra = extra;
  }

  public Float getMtaTax() {
    return mtaTax;
  }

  public void setMtaTax(Float mtaTax) {
    this.mtaTax = mtaTax;
  }

  public Float getTipAmount() {
    return tipAmount;
  }

  public void setTipAmount(Float tipAmount) {
    this.tipAmount = tipAmount;
  }

  public Float getTollsAmount() {
    return tollsAmount;
  }

  public void setTollsAmount(Float tollsAmount) {
    this.tollsAmount = tollsAmount;
  }

  public Float getImprovementSurcharge() {
    return improvementSurcharge;
  }

  public void setImprovementSurcharge(Float improvementSurcharge) {
    this.improvementSurcharge = improvementSurcharge;
  }

  public Float getTotalAmount() {
    return totalAmount;
  }

  public void setTotalAmount(Float totalAmount) {
    this.totalAmount = totalAmount;
  }

  public Float getCongestionSurcharge() {
    return congestionSurcharge;
  }

  public void setCongestionSurcharge(Float congestionSurcharge) {
    this.congestionSurcharge = congestionSurcharge;
  }

  @Override
  public int compareTo(YellowWritable o) {
    return ComparisonChain.start().
      compare(vendorID, o.vendorID).
      compare(tpepPickupDatetime, o.tpepPickupDatetime).
      compare(tpepDropoffDatetime, o.tpepDropoffDatetime).
      compare(passengerCount, o.passengerCount).
      compare(tripDistance, o.tripDistance).
      compare(rateCodeID, o.rateCodeID).
      compare(storeAndFwdFlag, o.storeAndFwdFlag).
      compare(puLocationID, o.puLocationID).
      compare(doLocationID, o.doLocationID).
      compare(paymentType, o.paymentType).
      compare(fareAmount, o.fareAmount).
      compare(extra, o.extra).
      compare(mtaTax, o.mtaTax).
      compare(tipAmount, o.tipAmount).
      compare(tollsAmount, o.tollsAmount).
      compare(improvementSurcharge, o.improvementSurcharge).
      compare(totalAmount, o.totalAmount).
      compare(congestionSurcharge, o.congestionSurcharge).result();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeString(out, vendorID);
    WritableUtils.writeString(out, tpepPickupDatetime);
    WritableUtils.writeString(out, tpepDropoffDatetime);
    out.writeInt(passengerCount);
    out.writeFloat(tripDistance);
    WritableUtils.writeString(out, rateCodeID);
    WritableUtils.writeString(out, storeAndFwdFlag);
    WritableUtils.writeString(out, puLocationID);
    WritableUtils.writeString(out, doLocationID);
    WritableUtils.writeString(out, paymentType);
    out.writeFloat(fareAmount);
    out.writeFloat(extra);
    out.writeFloat(mtaTax);
    out.writeFloat(tipAmount);
    out.writeFloat(tollsAmount);
    out.writeFloat(improvementSurcharge);
    out.writeFloat(totalAmount);
    out.writeFloat(congestionSurcharge);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    vendorID = WritableUtils.readString(in);
    tpepPickupDatetime = WritableUtils.readString(in);
    tpepDropoffDatetime = WritableUtils.readString(in);
    passengerCount = in.readInt();
    tripDistance = in.readFloat();
    rateCodeID = WritableUtils.readString(in);
    storeAndFwdFlag = WritableUtils.readString(in);
    puLocationID = WritableUtils.readString(in);
    doLocationID = WritableUtils.readString(in);
    paymentType = WritableUtils.readString(in);
    fareAmount = in.readFloat();
    extra = in.readFloat();
    mtaTax = in.readFloat();
    tipAmount = in.readFloat();
    tollsAmount = in.readFloat();
    improvementSurcharge = in.readFloat();
    totalAmount = in.readFloat();
    congestionSurcharge = in.readFloat();
  }
}
