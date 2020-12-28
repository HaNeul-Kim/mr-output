package com.tistory.hskimsky.mapreduce.plain;

import com.tistory.hskimsky.mapreduce.AbstractMapper;

import java.nio.charset.StandardCharsets;

/**
 * @author hskimsky
 * @version 1.0
 * @since 2020-12-28
 */
public class PlainMapper extends AbstractMapper {

  @Override
  protected byte[] getBytes(String[] tokens) {
    int passengerCount = tokens[3].length() > 0 ? Integer.parseInt(tokens[3]) : 0;
    float congestionSurcharge = tokens.length > 17 && tokens[17].length() > 0 ? Float.parseFloat(tokens[17]) : 0;

    String string = tokens[0] + "^" +
      tokens[1] + "^" +
      tokens[2] + "^" +
      passengerCount + "^" +
      tokens[4] + "^" +
      tokens[5] + "^" +
      tokens[6] + "^" +
      tokens[7] + "^" +
      tokens[8] + "^" +
      tokens[9] + "^" +
      tokens[10] + "^" +
      tokens[11] + "^" +
      tokens[12] + "^" +
      tokens[13] + "^" +
      tokens[14] + "^" +
      tokens[15] + "^" +
      tokens[16] + "^" +
      congestionSurcharge;
    return string.getBytes(StandardCharsets.UTF_8);
  }
}
