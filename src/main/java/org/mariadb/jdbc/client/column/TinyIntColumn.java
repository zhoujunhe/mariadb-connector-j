// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.client.column;

import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

import java.sql.SQLDataException;
import java.util.Calendar;

/** Column metadata definition */
public class TinyIntColumn extends ColumnDefinitionPacket implements ColumnDecoder {

  public TinyIntColumn(
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
    String s = buf.readAscii(length);
    return !"0".equals(s);
  }

  @Override
  public boolean decodeBooleanBinary(ReadableByteBuf buf, int length)
          throws SQLDataException {
    return buf.readByte() != 0;
  }

  @Override
  public byte decodeByteText(ReadableByteBuf buf, int length)
          throws SQLDataException {
    if (isSigned())
      return buf.readByte();
    long result = buf.atoi(length);

    if ((byte) result != result) {
      throw new SQLDataException("byte overflow");
    }
    return (byte) result;
  }

  @Override
  public byte decodeByteBinary(ReadableByteBuf buf, int length)
          throws SQLDataException {
    if (isSigned())
      return buf.readByte();
    long result = buf.readUnsignedByte();

    if ((byte) result != result) {
      throw new SQLDataException("byte overflow");
    }
    return (byte) result;
  }

  @Override
  public String decodeStringText(ReadableByteBuf buf, int length, Calendar cal)
      throws SQLDataException {
    return buf.readString(length);
  }

  @Override
  public String decodeStringBinary(ReadableByteBuf buf, int length, Calendar cal)
      throws SQLDataException {
    if (!isSigned()) {
      return String.valueOf(buf.readUnsignedByte());
    }
    return String.valueOf(buf.readByte());
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
    return (int) buf.atoi(length);
  }

  @Override
  public int decodeIntBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    return (isSigned() ? buf.readByte() : buf.readUnsignedByte());
  }

  @Override
  public long decodeLongText(ReadableByteBuf buf, int length) throws SQLDataException {
    return buf.atoi(length);
  }

  @Override
  public long decodeLongBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    if (!isSigned()) {
      return buf.readUnsignedByte();
    }
    return buf.readByte();
  }

  @Override
  public float decodeFloatText(ReadableByteBuf buf, int length) throws SQLDataException {
    return Float.parseFloat(buf.readAscii(length));
  }

  @Override
  public float decodeFloatBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    if (!isSigned()) {
      return buf.readUnsignedByte();
    }
    return buf.readByte();
  }

  @Override
  public double decodeDoubleText(ReadableByteBuf buf, int length) throws SQLDataException {
    return Double.parseDouble(buf.readAscii(length));
  }

  @Override
  public double decodeDoubleBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    if (!isSigned()) {
      return buf.readUnsignedByte();
    }
    return buf.readByte();
  }
}
