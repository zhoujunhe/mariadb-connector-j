// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.client.column;

import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;
import org.mariadb.jdbc.plugin.codec.ByteCodec;

import java.math.BigInteger;
import java.sql.SQLDataException;
import java.util.Calendar;

/** Column metadata definition */
public class BitColumn extends ColumnDefinitionPacket implements ColumnDecoder {

  public BitColumn(
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
  public Object getDefaultText(ReadableByteBuf buf, int length)
          throws SQLDataException {
    //
  }

  @Override
  public Object getDefaultBinary(ReadableByteBuf buf, int length)
          throws SQLDataException {
    if (isSigned() || (buf.getByte(buf.pos() + 7) & 0x80) == 0) {
      return buf.readLong();
    } else {
      // error too big to return a long
      byte[] bb = new byte[8];
      for (int i = 7; i >= 0; i--) {
        bb[i] = buf.readByte();
      }
      BigInteger val = new BigInteger(1, bb);
      try {
        return val.longValueExact();
      } catch (ArithmeticException ae) {
        throw new SQLDataException(String.format("value '%s' cannot be decoded as Long", val));
      }
    }
  }
  @Override
  public boolean decodeBooleanText(ReadableByteBuf buf, int length)
          throws SQLDataException {
    return ByteCodec.parseBit(buf, length) != 0;
  }

  @Override
  public boolean decodeBooleanBinary(ReadableByteBuf buf, int length)
          throws SQLDataException {
    return ByteCodec.parseBit(buf, length) != 0;
  }

  @Override
  public byte decodeByteText(ReadableByteBuf buf, int length)
          throws SQLDataException {
    byte val = buf.readByte();
    if (length > 1) buf.skip(length - 1);
    return val;
  }

  @Override
  public byte decodeByteBinary(ReadableByteBuf buf, int length)
          throws SQLDataException {
    return decodeByteText(buf, length);
  }

  @Override
  public String decodeStringText(ReadableByteBuf buf, int length, Calendar cal)
      throws SQLDataException {
    byte[] bytes = new byte[length];
    buf.readBytes(bytes);
    StringBuilder sb = new StringBuilder(bytes.length * Byte.SIZE + 3);
    sb.append("b'");
    boolean firstByteNonZero = false;
    for (int i = 0; i < Byte.SIZE * bytes.length; i++) {
      boolean b = (bytes[i / Byte.SIZE] & 1 << (Byte.SIZE - 1 - (i % Byte.SIZE))) > 0;
      if (b) {
        sb.append('1');
        firstByteNonZero = true;
      } else if (firstByteNonZero) {
        sb.append('0');
      }
    }
    sb.append("'");
    return sb.toString();
  }

  @Override
  public String decodeStringBinary(ReadableByteBuf buf, int length, Calendar cal)
      throws SQLDataException {
    byte[] bytes = new byte[length];
    buf.readBytes(bytes);
    StringBuilder sb = new StringBuilder(bytes.length * Byte.SIZE + 3);
    sb.append("b'");
    boolean firstByteNonZero = false;
    for (int i = 0; i < Byte.SIZE * bytes.length; i++) {
      boolean b = (bytes[i / Byte.SIZE] & 1 << (Byte.SIZE - 1 - (i % Byte.SIZE))) > 0;
      if (b) {
        sb.append('1');
        firstByteNonZero = true;
      } else if (firstByteNonZero) {
        sb.append('0');
      }
    }
    sb.append("'");
    return sb.toString();
  }

  @Override
  public short decodeShortText(ReadableByteBuf buf, int length) throws SQLDataException {
    long result = 0;
    for (int i = 0; i < length; i++) {
      byte b = buf.readByte();
      result = (result << 8) + (b & 0xff);
    }
    if ((short) result != result || (result < 0 && !isSigned())) {
      throw new SQLDataException("Short overflow");
    }
    return (short) result;
  }

  @Override
  public short decodeShortBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    return decodeShortText(buf, length);
  }

  @Override
  public int decodeIntText(ReadableByteBuf buf, int length) throws SQLDataException {
    long result = 0;
    for (int i = 0; i < length; i++) {
      byte b = buf.readByte();
      result = (result << 8) + (b & 0xff);
    }
    int res = (int) result;
    if (res != result || (result < 0 && !isSigned())) {
      throw new SQLDataException("integer overflow");
    }
    return res;
  }

  @Override
  public int decodeIntBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    long result = 0;
    for (int i = 0; i < length; i++) {
      byte b = buf.readByte();
      result = (result << 8) + (b & 0xff);
    }

    int res = (int) result;
    if (res != result) {
      throw new SQLDataException("integer overflow");
    }

    return res;
  }

  @Override
  public long decodeLongText(ReadableByteBuf buf, int length) throws SQLDataException {
    long result = 0;
    for (int i = 0; i < length; i++) {
      byte b = buf.readByte();
      result = (result << 8) + (b & 0xff);
    }
    return result;
  }

  @Override
  public long decodeLongBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    return decodeLongText(buf, length);
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
