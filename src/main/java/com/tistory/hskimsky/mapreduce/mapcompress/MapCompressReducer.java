package com.tistory.hskimsky.mapreduce.mapcompress;

import com.tistory.hskimsky.mapreduce.AbstractReducer;

/**
 * @author hskimsky
 * @version 1.0
 * @since 2020-12-28
 */
public class MapCompressReducer extends AbstractReducer<String> {

  @Override
  protected String getObject(byte[] value) {
    return new String(value);
  }
}
