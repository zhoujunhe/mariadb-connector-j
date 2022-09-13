// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.client.column;

import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.SQLDataException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;

/** Column metadata definition */
public class DateColumn extends ColumnDefinitionPacket implements ColumnDecoder {

  public DateColumn(
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
  public byte decodeByteText(ReadableByteBuf buf, int length)
          throws SQLDataException {
    buf.skip(length);
    throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Byte", getType()));
  }

  @Override
  public byte decodeByteBinary(ReadableByteBuf buf, int length)
          throws SQLDataException {
    buf.skip(length);
    throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Byte", getType()));
  }

  @Override
  public String decodeStringText(ReadableByteBuf buf, int length, Calendar cal)
      throws SQLDataException {
    return buf.readString(length);
  }

  @Override
  public String decodeStringBinary(ReadableByteBuf buf, int length, Calendar cal)
      throws SQLDataException {
    if (length == 0) return "0000-00-00";
    int dateYear = buf.readUnsignedShort();
    int dateMonth = buf.readByte();
    int dateDay = buf.readByte();
    return LocalDate.of(dateYear, dateMonth, dateDay).toString();
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
