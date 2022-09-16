// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.unit.util;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.util.ClientParser;

@SuppressWarnings("ConstantConditions")
public class ClientParserTest {
  private static final String sql =
      "select seq, 'abcdefghijabcdefghijabcdefghijaa' from seq_1_to_1000";

  @Test
  public void tt() throws Throwable {
    try (Connection con = DriverManager.getConnection("jdbc:mariadb://localhost/testj?user=root")) {
      for (int j = 0; j < 1000000; j++) {
        try (PreparedStatement st = con.prepareStatement(sql)) {
          ResultSet rs = st.executeQuery();
          long i = 0;
          while (rs.next()) {
            i = rs.getLong(1);
            rs.getString(2);
          }
        }
      }
    }
  }

  private void parse(String sql, String[] expected, String[] expectedNoBackSlash) {
    ClientParser parser = ClientParser.parameterParts(sql, false);
    assertEquals(expected.length, parser.getParamCount() + 1, displayErr(parser, expected));

    int pos = 0;
    int paramPos = parser.getQuery().length;
    for (int i = 0; i < parser.getParamCount(); i++) {
      paramPos = parser.getParamPositions().get(i);
      assertEquals(expected[i], new String(parser.getQuery(), pos, paramPos - pos));
      pos = paramPos + 1;
    }
    assertEquals(expected[expected.length - 1], new String(parser.getQuery(), pos, paramPos - pos));

    parser = ClientParser.parameterParts(sql, true);
    assertEquals(
        expectedNoBackSlash.length, parser.getParamCount() + 1, displayErr(parser, expected));
    pos = 0;
    paramPos = parser.getQuery().length;
    for (int i = 0; i < parser.getParamCount(); i++) {
      paramPos = parser.getParamPositions().get(i);
      assertEquals(expectedNoBackSlash[i], new String(parser.getQuery(), pos, paramPos - pos));
      pos = paramPos + 1;
    }
    assertEquals(
        expectedNoBackSlash[expectedNoBackSlash.length - 1],
        new String(parser.getQuery(), pos, paramPos - pos));
  }

  private String displayErr(ClientParser parser, String[] exp) {
    StringBuilder sb = new StringBuilder();
    sb.append("is:\n");

    int pos = 0;
    int paramPos = parser.getQuery().length;
    for (int i = 0; i < parser.getParamCount(); i++) {
      paramPos = parser.getParamPositions().get(i);
      sb.append(new String(parser.getQuery(), pos, paramPos - pos, StandardCharsets.UTF_8))
          .append("\n");
      pos = paramPos + 1;
    }
    sb.append(new String(parser.getQuery(), pos, paramPos - pos));

    sb.append("but was:\n");
    for (String s : exp) {
      sb.append(s).append("\n");
    }
    return sb.toString();
  }

  @Test
  public void ClientParser() {
    parse(
        "SELECT '\\\\test' /*test* #/ ;`*/",
        new String[] {"SELECT '\\\\test' /*test* #/ ;`*/"},
        new String[] {"SELECT '\\\\test' /*test* #/ ;`*/"});
    parse(
        "DO '\\\"', \"\\'\"",
        new String[] {"DO '\\\"', \"\\'\""},
        new String[] {"DO '\\\"', \"\\'\""});
  }
}
