// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.client.column;

import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;
import org.mariadb.jdbc.plugin.codec.ByteCodec;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.SQLDataException;
import java.util.Calendar;

/** Column metadata definition */
public class BlobColumn extends ColumnDefinitionPacket implements ColumnDecoder {

  public BlobColumn(
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
    if (isBinary()) {
      buf.skip(length);
      throw new SQLDataException(
              String.format("Data type %s cannot be decoded as Boolean", getType()));
    }
    String s = buf.readAscii(length);
    return !"0".equals(s);
  }

  @Override
  public boolean decodeBooleanBinary(ReadableByteBuf buf, int length)
          throws SQLDataException {
    return decodeBooleanText(buf, length);
  }

  @Override
  public byte decodeByteText(ReadableByteBuf buf, int length)
          throws SQLDataException {
    long result;
    if (!isBinary()) {
      // TEXT column
      String str2 = buf.readString(length);
      try {
        result = new BigDecimal(str2).setScale(0, RoundingMode.DOWN).longValue();
      } catch (NumberFormatException nfe) {
        throw new SQLDataException(
                String.format("value '%s' (%s) cannot be decoded as Byte", str2, getType()));
      }
      if ((byte) result != result) {
        throw new SQLDataException("byte overflow");
      }

      return (byte) result;
    }
    if (length > 0) {
      byte b = buf.readByte();
      buf.skip(length - 1);
      return b;
    }
    throw new SQLDataException("empty String value cannot be decoded as Byte");
  }

  @Override
  public byte decodeByteBinary(ReadableByteBuf buf, int length)
          throws SQLDataException {
    return decodeByteText(buf, length);
  }

  @Override
  public String decodeStringText(ReadableByteBuf buf, int length, Calendar cal)
      throws SQLDataException {
    return buf.readString(length);
  }

  @Override
  public String decodeStringBinary(ReadableByteBuf buf, int length, Calendar cal)
      throws SQLDataException {
    return buf.readString(length);
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
    if (isBinary()) {
      buf.skip(length);
      throw new SQLDataException(
              String.format("Data type %s cannot be decoded as Integer", getType()));
    }
    long result;
    String str = buf.readString(length);
    try {
      result = new BigDecimal(str).setScale(0, RoundingMode.DOWN).longValueExact();
    } catch (NumberFormatException | ArithmeticException nfe) {
      throw new SQLDataException(String.format("value '%s' cannot be decoded as Integer", str));
    }

    int res = (int) result;
    if (res != result || (result < 0 && !isSigned())) {
      throw new SQLDataException("integer overflow");
    }
    return res;
  }

  @Override
  public int decodeIntBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    return decodeIntText(buf,length);
  }

  @Override
  public long decodeLongText(ReadableByteBuf buf, int length) throws SQLDataException {
    if (isBinary()) {
      buf.skip(length);
      throw new SQLDataException(
              String.format("Data type %s cannot be decoded as Long", getType()));
    }
    String str = buf.readString(length);
    try {
      return new BigInteger(str).longValueExact();
    } catch (NumberFormatException nfe) {
      throw new SQLDataException(String.format("value '%s' cannot be decoded as Long", str));
    }
  }

  @Override
  public long decodeLongBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    if (isBinary()) {
      buf.skip(length);
      throw new SQLDataException(
              String.format("Data type %s cannot be decoded as Long", getType()));
    }
    String str = buf.readString(length);
    try {
      return new BigDecimal(str).setScale(0, RoundingMode.DOWN).longValueExact();
    } catch (NumberFormatException | ArithmeticException nfe) {
      throw new SQLDataException(String.format("value '%s' cannot be decoded as Long", str));
    }
  }

  @Override
  public float decodeFloatText(ReadableByteBuf buf, int length) throws SQLDataException {
    if (isBinary()) {
      buf.skip(length);
      throw new SQLDataException(
              String.format("Data type %s cannot be decoded as Float", getType()));
    }
    String val = buf.readString(length);
    try {
      return Float.parseFloat(val);
    } catch (NumberFormatException nfe) {
      throw new SQLDataException(String.format("value '%s' cannot be decoded as Float", val));
    }
  }

  @Override
  public float decodeFloatBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    if (isBinary()) {
      buf.skip(length);
      throw new SQLDataException(
              String.format("Data type %s cannot be decoded as Float", getType()));
    }

    String str2 = buf.readString(length);
    try {
      return Float.parseFloat(str2);
    } catch (NumberFormatException nfe) {
      throw new SQLDataException(String.format("value '%s' cannot be decoded as Float", str2));
    }
  }

  @Override
  public double decodeDoubleText(ReadableByteBuf buf, int length) throws SQLDataException {
    if (isBinary()) {
      buf.skip(length);
      throw new SQLDataException(
              String.format("Data type %s cannot be decoded as Double", getType()));
    }
    String str2 = buf.readString(length);
    try {
      return Double.parseDouble(str2);
    } catch (NumberFormatException nfe) {
      throw new SQLDataException(String.format("value '%s' cannot be decoded as Double", str2));
    }
  }

  @Override
  public double decodeDoubleBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    return decodeDoubleText(buf, length);
  }
}
