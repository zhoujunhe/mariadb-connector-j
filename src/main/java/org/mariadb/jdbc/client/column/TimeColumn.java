// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.client.column;

import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

import java.sql.SQLDataException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;

/** Column metadata definition */
public class TimeColumn extends ColumnDefinitionPacket implements ColumnDecoder {

  public TimeColumn(
      ReadableByteBuf buf,
      int charset,
      long length,
      DataType dataType,
      byte decimals,
      int flags,
      int[] stringPos,
      String extTypeName,
      String extTypeFormat) {
    super(buf, charset, length, dataType, decimals, flags, stringPos, extTypeName, extTypeFormat);
  }

  @Override
  public boolean decodeBooleanText(ReadableByteBuf buf, int length)
          throws SQLDataException {
    buf.skip(length);
    throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Boolean", getType()));
  }

  @Override
  public boolean decodeBooleanBinary(ReadableByteBuf buf, int length)
          throws SQLDataException {
    buf.skip(length);
    throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Boolean", getType()));
  }

  @Override
  public String decodeStringText(ReadableByteBuf buf, int length, Calendar cal)
      throws SQLDataException {
    return buf.readString(length);
  }

  @Override
  public String decodeStringBinary(ReadableByteBuf buf, int length, Calendar cal)
      throws SQLDataException {
    long tDays = 0;
    int tHours = 0;
    int tMinutes = 0;
    int tSeconds = 0;
    long tMicroseconds = 0;
    if (length == 0) {
      StringBuilder zeroValue = new StringBuilder("00:00:00");
      if (getDecimals() > 0) {
        zeroValue.append(".");
        for (int i = 0; i < getDecimals(); i++) zeroValue.append("0");
      }
      return zeroValue.toString();
    }
    boolean negate = buf.readByte() == 0x01;
    if (length > 4) {
      tDays = buf.readUnsignedInt();
      if (length > 7) {
        tHours = buf.readByte();
        tMinutes = buf.readByte();
        tSeconds = buf.readByte();
        if (length > 8) {
          tMicroseconds = buf.readInt();
        }
      }
    }
    int totalHour = (int) (tDays * 24 + tHours);
    String stTime =
            (negate ? "-" : "")
                    + (totalHour < 10 ? "0" : "")
                    + totalHour
                    + ":"
                    + (tMinutes < 10 ? "0" : "")
                    + tMinutes
                    + ":"
                    + (tSeconds < 10 ? "0" : "")
                    + tSeconds;
    if (getDecimals() == 0) {
      if (tMicroseconds == 0) return stTime;
      // possible for Xpand that doesn't send some metadata
      // https://jira.mariadb.org/browse/XPT-273
      StringBuilder stMicro = new StringBuilder(String.valueOf(tMicroseconds));
      while (stMicro.length() < 6) {
        stMicro.insert(0, "0");
      }
      return stTime + "." + stMicro;
    }
    StringBuilder stMicro = new StringBuilder(String.valueOf(tMicroseconds));
    while (stMicro.length() < getDecimals()) {
      stMicro.insert(0, "0");
    }
    return stTime + "." + stMicro;

  }

  @Override
  public short decodeShortText(ReadableByteBuf buf, int length) throws SQLDataException {
    buf.skip(length);
    throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Short", getType()));
  }

  @Override
  public short decodeShortBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    buf.skip(length);
    throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Short", getType()));
  }

  @Override
  public int decodeIntText(ReadableByteBuf buf, int length) throws SQLDataException {
    buf.skip(length);
    throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Integer", getType()));
  }

  @Override
  public int decodeIntBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    buf.skip(length);
    throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Integer", getType()));
  }

  @Override
  public long decodeLongText(ReadableByteBuf buf, int length) throws SQLDataException {
    buf.skip(length);
    throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Integer", getType()));
  }

  @Override
  public long decodeLongBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    buf.skip(length);
    throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Integer", getType()));
  }
  @Override
  public float decodeFloatText(ReadableByteBuf buf, int length) throws SQLDataException {
    buf.skip(length);
    throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Float", getType()));
  }

  @Override
  public float decodeFloatBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    buf.skip(length);
    throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Float", getType()));
  }

  @Override
  public double decodeDoubleText(ReadableByteBuf buf, int length) throws SQLDataException {
    buf.skip(length);
    throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Double", getType()));
  }

  @Override
  public double decodeDoubleBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    buf.skip(length);
    throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Double", getType()));
  }
}
