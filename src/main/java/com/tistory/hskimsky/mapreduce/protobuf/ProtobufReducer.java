package com.tistory.hskimsky.mapreduce.protobuf;

import com.google.protobuf.InvalidProtocolBufferException;
import com.tistory.hskimsky.mapreduce.AbstractReducer;

/**
 * @author hskimsky
 * @version 1.0
 * @since 2020-12-21
 */
public class ProtobufReducer extends AbstractReducer<YellowProto.Yellow> {

  @Override
  protected YellowProto.Yellow getObject(byte[] value) {
    try {
      return YellowProto.Yellow.parseFrom(value);
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
      return null;
    }
  }
}
