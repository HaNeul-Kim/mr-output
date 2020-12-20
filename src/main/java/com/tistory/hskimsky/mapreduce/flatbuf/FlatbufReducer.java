package com.tistory.hskimsky.mapreduce.flatbuf;

import com.tistory.hskimsky.mapreduce.AbstractReducer;

import java.nio.ByteBuffer;

/**
 * @author hskimsky
 * @version 1.0
 * @since 2020-12-21
 */
public class FlatbufReducer extends AbstractReducer<YellowFlat> {

  @Override
  protected YellowFlat getObject(byte[] value) {
    return YellowFlat.getRootAsYellowFlat(ByteBuffer.wrap(value));
  }
}
