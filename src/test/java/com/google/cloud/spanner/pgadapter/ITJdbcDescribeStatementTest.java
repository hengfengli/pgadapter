// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.cloud.spanner.pgadapter;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.ByteArray;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.jdbc.PgStatement;

@Category({IntegrationTest.class})
@RunWith(JUnit4.class)
public class ITJdbcDescribeStatementTest implements IntegrationTest {
  private static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();
  private static Database database;

  @BeforeClass
  public static void setup() throws ClassNotFoundException {
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");

    testEnv.setUp();
    database = testEnv.createDatabase(PgAdapterTestEnv.DEFAULT_DATA_MODEL);
    testEnv.startPGAdapterServerWithDefaultDatabase(database.getId(), Collections.emptyList());
  }

  @AfterClass
  public static void teardown() {
    testEnv.stopPGAdapterServer();
    testEnv.cleanUp();
  }

  @Before
  public void insertTestData() {
    String databaseId = database.getId().getDatabase();
    testEnv.write(databaseId, Collections.singleton(Mutation.delete("all_types", KeySet.all())));
    testEnv.write(
        databaseId,
        Collections.singleton(
            Mutation.newInsertBuilder("all_types")
                .set("col_bigint")
                .to(1L)
                .set("col_bool")
                .to(true)
                .set("col_bytea")
                .to(ByteArray.copyFrom("test"))
                .set("col_float4")
                .to(3.14f)
                .set("col_float8")
                .to(3.14d)
                .set("col_int")
                .to(1)
                .set("col_numeric")
                .to(new BigDecimal("3.14"))
                .set("col_timestamptz")
                .to(Timestamp.parseTimestamp("2022-01-27T17:51:30+01:00"))
                .set("col_date")
                .to(com.google.cloud.Date.parseDate("2022-04-29"))
                .set("col_varchar")
                .to("test")
                .set("col_jsonb")
                .to("{\"key\": \"value\"}")
                .build()));
  }

  private String getConnectionUrl() {
    return String.format("jdbc:postgresql://%s/", testEnv.getPGAdapterHostAndPort());
  }

  @Test
  public void testQuery() throws SQLException {
    String sql =
        "select col_bigint, col_bool, col_bytea, col_float4, col_float8, col_int, col_numeric, "
            + "col_timestamptz, col_date, col_varchar "
            + "from all_types "
            + "where col_bigint=? "
            + "and col_bool=? "
            + "and col_bytea=? "
            + "and col_float4=? "
            + "and col_float8=? "
            + "and col_int=? "
            + "and col_numeric=? "
            + "and col_timestamptz=? "
            + "and col_date=? "
            + "and col_varchar=?";
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (PreparedStatement statement = connection.prepareStatement(sql)) {
        ParameterMetaData metadata = statement.getParameterMetaData();
        assertEquals(10, metadata.getParameterCount());
        for (int index = 1; index <= metadata.getParameterCount(); index++) {
          assertEquals(ParameterMetaData.parameterModeIn, metadata.getParameterMode(index));
          assertEquals(ParameterMetaData.parameterNullableUnknown, metadata.isNullable(index));
        }
        int index = 0;
        assertEquals(sql, Types.BIGINT, metadata.getParameterType(++index));
        assertEquals(Types.BIT, metadata.getParameterType(++index));
        assertEquals(Types.BINARY, metadata.getParameterType(++index));
        assertEquals(Types.REAL, metadata.getParameterType(++index));
        assertEquals(Types.DOUBLE, metadata.getParameterType(++index));
        assertEquals(Types.BIGINT, metadata.getParameterType(++index));
        assertEquals(Types.NUMERIC, metadata.getParameterType(++index));
        assertEquals(Types.TIMESTAMP, metadata.getParameterType(++index));
        assertEquals(Types.DATE, metadata.getParameterType(++index));
        assertEquals(Types.VARCHAR, metadata.getParameterType(++index));
      }
    }
  }

  @Test
  public void testParameterMetaData() throws SQLException {
    for (String sql :
        new String[] {
          "select col_bigint, col_bool, col_bytea, col_float4, col_float8, col_int, col_numeric, "
              + "col_timestamptz, col_date, col_varchar, col_jsonb "
              + "from all_types "
              + "where col_bigint=? "
              + "and col_bool=? "
              + "and col_bytea=? "
              + "and col_float4=? "
              + "and col_float8=? "
              + "and col_int=? "
              + "and col_numeric=? "
              + "and col_timestamptz=? "
              + "and col_date=? "
              + "and col_varchar=? "
              + "and col_jsonb::text=?",
          "insert into all_types "
              + "(col_bigint, col_bool, col_bytea, col_float4, col_float8, col_int, col_numeric, "
              + "col_timestamptz, col_date, col_varchar, col_jsonb) "
              + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
          "insert into all_types "
              + "(col_bigint, col_bool, col_bytea, col_float4, col_float8, col_int, col_numeric, "
              + "col_timestamptz, col_date, col_varchar, col_jsonb) "
              + "select col_bigint, col_bool, col_bytea, col_float4, col_float8, col_int, col_numeric, "
              + "col_timestamptz, col_date, col_varchar, col_jsonb "
              + "from all_types "
              + "where col_bigint=? "
              + "and col_bool=? "
              + "and col_bytea=? "
              + "and col_float4=? "
              + "and col_float8=? "
              + "and col_int=? "
              + "and col_numeric=? "
              + "and col_timestamptz=? "
              + "and col_date=? "
              + "and col_varchar=? "
              + "and col_jsonb::text=?",
          "insert into all_types " + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
          "insert into all_types "
              + "select col_bigint, col_bool, col_bytea, col_float4, col_float8, col_int, col_numeric, "
              + "col_timestamptz, col_date, col_varchar, col_jsonb "
              + "from all_types "
              + "where col_bigint=? "
              + "and col_bool=? "
              + "and col_bytea=? "
              + "and col_float4=? "
              + "and col_float8=? "
              + "and col_int=? "
              + "and col_numeric=? "
              + "and col_timestamptz=? "
              + "and col_date=? "
              + "and col_varchar=? "
              + "and col_jsonb::text=?",
          "update all_types set "
              + "col_bool=?, "
              + "col_bytea=?, "
              + "col_float4=?, "
              + "col_float8=?, "
              + "col_int=?, "
              + "col_numeric=?, "
              + "col_timestamptz=?, "
              + "col_date=?, "
              + "col_varchar=?, "
              + "col_jsonb=?",
          "update all_types set "
              + "col_bool=null, "
              + "col_bytea=null, "
              + "col_float4=null, "
              + "col_float8=null, "
              + "col_int=null, "
              + "col_numeric=null, "
              + "col_timestamptz=null, "
              + "col_date=null, "
              + "col_varchar=null, "
              + "col_jsonb=null "
              + "where col_bigint=? "
              + "and col_bool=? "
              + "and col_bytea=? "
              + "and col_float4=? "
              + "and col_float8=? "
              + "and col_int=? "
              + "and col_numeric=? "
              + "and col_timestamptz=? "
              + "and col_date=? "
              + "and col_varchar=? "
              + "and col_jsonb::text=?",
          "delete "
              + "from all_types "
              + "where col_bigint=? "
              + "and col_bool=? "
              + "and col_bytea=? "
              + "and col_float4=? "
              + "and col_float8=? "
              + "and col_int=? "
              + "and col_numeric=? "
              + "and col_timestamptz=? "
              + "and col_date=? "
              + "and col_varchar=? "
              + "and col_jsonb::text=?"
        }) {
      try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
          ParameterMetaData metadata = statement.getParameterMetaData();
          if (sql.startsWith("update all_types set col_bool=?,")) {
            assertEquals(sql, 10, metadata.getParameterCount());
          } else {
            assertEquals(sql, 11, metadata.getParameterCount());
          }
          for (int index = 1; index <= metadata.getParameterCount(); index++) {
            assertEquals(ParameterMetaData.parameterModeIn, metadata.getParameterMode(index));
            assertEquals(ParameterMetaData.parameterNullableUnknown, metadata.isNullable(index));
          }
          int index = 0;
          if (metadata.getParameterCount() == 11) {
            assertEquals(sql, Types.BIGINT, metadata.getParameterType(++index));
          }
          assertEquals(sql, Types.BIT, metadata.getParameterType(++index));
          assertEquals(sql, Types.BINARY, metadata.getParameterType(++index));
          assertEquals(sql, Types.REAL, metadata.getParameterType(++index));
          assertEquals(sql, Types.DOUBLE, metadata.getParameterType(++index));
          assertEquals(sql, Types.BIGINT, metadata.getParameterType(++index));
          assertEquals(sql, Types.NUMERIC, metadata.getParameterType(++index));
          assertEquals(sql, Types.TIMESTAMP, metadata.getParameterType(++index));
          assertEquals(sql, Types.DATE, metadata.getParameterType(++index));
          assertEquals(sql, Types.VARCHAR, metadata.getParameterType(++index));
          // jsonb does not support the '=' operator, which means that when a jsonb parameter is
          // used for comparison, we must cast it to text. That changes the parameter type to
          // Types.VARCHAR.
          if (sql.contains("col_jsonb::text=?")) {
            assertEquals(sql, Types.VARCHAR, metadata.getParameterType(++index));
          } else {
            assertEquals(sql, Types.OTHER, metadata.getParameterType(++index));
          }
        } catch (SQLException e) {
          throw new SQLException("Error for statement: " + sql, e);
        }
      }
    }
  }

  @Test
  public void testManualParameters() throws SQLException {
    String sql = "select * from (select ?, ?, ?, ?, ?, ?) p";
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (PreparedStatement statement = connection.prepareStatement(sql)) {
        statement.setNull(1, Types.BIGINT);
        statement.setNull(4, Types.BIGINT);
        ParameterMetaData metadata = statement.getParameterMetaData();
        assertEquals(Types.BIGINT, metadata.getParameterType(1));
        assertEquals(Types.VARCHAR, metadata.getParameterType(2));
        assertEquals(Types.VARCHAR, metadata.getParameterType(3));
        assertEquals(Types.BIGINT, metadata.getParameterType(4));
        assertEquals(Types.VARCHAR, metadata.getParameterType(5));
        assertEquals(Types.VARCHAR, metadata.getParameterType(6));
      }
    }
  }

  @Test
  public void testParameterMetaDataInLimit() throws SQLException {
    String sql = "select * from all_types order by col_varchar limit ? offset ?";
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (PreparedStatement statement = connection.prepareStatement(sql)) {
        ParameterMetaData metadata = statement.getParameterMetaData();
        assertEquals(2, metadata.getParameterCount());
        assertEquals(Types.BIGINT, metadata.getParameterType(1));
        assertEquals(Types.BIGINT, metadata.getParameterType(2));
      }
    }
  }

  @Test
  public void testMoreThan50Parameters() throws SQLException {
    String sql =
        "select * from all_types where "
            + IntStream.range(0, 51)
                .mapToObj(i -> "col_varchar=?")
                .collect(Collectors.joining(" or "));
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (PreparedStatement statement = connection.prepareStatement(sql)) {
        ParameterMetaData metadata = statement.getParameterMetaData();
        assertEquals(51, metadata.getParameterCount());
        for (int i = 1; i < metadata.getParameterCount(); i++) {
          assertEquals(Types.VARCHAR, metadata.getParameterType(i));
        }
      }
    }
  }

  @Test
  public void testDescribeInvalidStatements() throws SQLException {
    for (String sql :
        new String[] {
          "select borked "
              + "from all_types "
              + "where col_bigint=? "
              + "and col_bool=? "
              + "and col_bytea=? "
              + "and col_float4=? "
              + "and col_float8=? "
              + "and col_int=? "
              + "and col_numeric=? "
              + "and col_timestamptz=? "
              + "and col_date=? "
              + "and col_varchar=?",
          "insert into all_types "
              + "(col_bigint, col_bool, col_bytea, col_float4, col_float8, col_int, col_numeric, "
              + "col_timestamptz, col_date, col_varchar, borked) "
              + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
          "update all_types set col_bigint=?, "
              + "col_bool=?, "
              + "col_bytea=?, "
              + "col_float4=?, "
              + "col_float8=?, "
              + "col_int=?, "
              + "col_numeric=?, "
              + "col_timestamptz=?, "
              + "col_date=?, "
              + "col_varchar=?, borked='really borked'",
          "delete "
              + "from all_types "
              + "where col_bigint=? "
              + "and col_bool=? "
              + "and col_bytea=? "
              + "and col_float4=? "
              + "and col_float8=? "
              + "and col_int=? "
              + "and col_numeric=? "
              + "and col_timestamptz=? "
              + "and col_date=? "
              + "and col_varchar=? "
              + "and borked='really borked'"
        }) {
      try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
          SQLException exception =
              assertThrows(SQLException.class, statement::getParameterMetaData);
          assertTrue(exception.getMessage(), exception.getMessage().contains("borked"));
        }
      }
    }
  }

  @Test
  public void testResultSetMetaData() throws SQLException {
    ImmutableList<String> columnNames =
        ImmutableList.of(
            "col_bigint",
            "col_bool",
            "col_bytea",
            "col_float4",
            "col_float8",
            "col_int",
            "col_numeric",
            "col_timestamptz",
            "col_date",
            "col_varchar");
    ImmutableList<Integer> types =
        ImmutableList.of(
            Types.BIGINT,
            Types.BIT,
            Types.BINARY,
            Types.REAL,
            Types.DOUBLE,
            Types.BIGINT,
            Types.NUMERIC,
            Types.TIMESTAMP,
            Types.DATE,
            Types.VARCHAR);
    String sql =
        String.format(
            "select %s "
                + "from all_types "
                + "where col_bigint=? "
                + "and col_bool=? "
                + "and col_bytea=? "
                + "and col_float4=? "
                + "and col_float8=? "
                + "and col_int=? "
                + "and col_numeric=? "
                + "and col_timestamptz=? "
                + "and col_date=? "
                + "and col_varchar=?",
            String.join(", ", columnNames));

    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (PreparedStatement statement = connection.prepareStatement(sql)) {
        ResultSetMetaData metadata = statement.getMetaData();
        assertEquals(10, metadata.getColumnCount());
        for (int index = 1; index <= metadata.getColumnCount(); index++) {
          assertEquals(types.get(index - 1).intValue(), metadata.getColumnType(index));
          assertEquals(ResultSetMetaData.columnNullableUnknown, metadata.isNullable(index));
          assertEquals(columnNames.get(index - 1), metadata.getColumnName(index));
        }
      }
    }
  }

  @Test
  public void testSelectWithParameters() throws SQLException {
    String sql =
        "select col_bigint, col_bool, col_bytea, col_float4, col_float8, col_int, col_numeric, "
            + "col_timestamptz, col_date, col_varchar "
            + "from all_types "
            + "where col_bigint=? "
            + "and col_bool=? "
            + "and col_bytea=? "
            + "and col_float4=? "
            + "and col_float8=? "
            + "and col_int=? "
            + "and col_numeric=? "
            + "and col_timestamptz=? "
            + "and col_date=? "
            + "and col_varchar=?";

    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (PreparedStatement statement = connection.prepareStatement(sql)) {
        // This forces the PG JDBC driver to use binary transfer mode for the results, and will
        // also cause it to send a DescribeStatement message.
        statement.unwrap(PgStatement.class).setPrepareThreshold(-1);

        int index = 0;
        statement.setLong(++index, 1);
        statement.setBoolean(++index, true);
        statement.setBytes(++index, "test".getBytes(StandardCharsets.UTF_8));
        statement.setFloat(++index, 3.14f);
        statement.setDouble(++index, 3.14d);
        statement.setInt(++index, 1);
        statement.setBigDecimal(++index, new BigDecimal("3.14"));
        statement.setTimestamp(
            ++index, Timestamp.parseTimestamp("2022-01-27T17:51:30+01:00").toSqlTimestamp());
        statement.setDate(++index, Date.valueOf("2022-04-29"));
        statement.setString(++index, "test");

        try (ResultSet resultSet = statement.executeQuery()) {
          assertTrue(resultSet.next());

          index = 0;
          assertEquals(1, resultSet.getLong(++index));
          assertTrue(resultSet.getBoolean(++index));
          assertArrayEquals("test".getBytes(StandardCharsets.UTF_8), resultSet.getBytes(++index));
          assertEquals(3.14f, resultSet.getFloat(++index), 0.0f);
          assertEquals(3.14d, resultSet.getDouble(++index), 0.0d);
          assertEquals(1, resultSet.getInt(++index));
          assertEquals(new BigDecimal("3.14"), resultSet.getBigDecimal(++index));
          assertEquals(
              Timestamp.parseTimestamp("2022-01-27T17:51:30+01:00").toSqlTimestamp(),
              resultSet.getTimestamp(++index));
          assertEquals(Date.valueOf("2022-04-29"), resultSet.getDate(++index));
          assertEquals("test", resultSet.getString(++index));

          assertFalse(resultSet.next());
        }
      }
    }
  }

  @Test
  public void testInsertWithParameters() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (PreparedStatement statement =
          connection.prepareStatement(
              "insert into all_types "
                  + "(col_bigint, col_bool, col_bytea, col_float4, col_float8, col_int, col_numeric, "
                  + "col_timestamptz, col_date, col_varchar) "
                  + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
        // This forces the PG JDBC driver to use binary transfer mode for the results, and will
        // also cause it to send a DescribeStatement message.
        statement.unwrap(PgStatement.class).setPrepareThreshold(-1);

        int index = 0;
        statement.setLong(++index, 2);
        statement.setBoolean(++index, true);
        statement.setBytes(++index, "bytes_test".getBytes(StandardCharsets.UTF_8));
        statement.setFloat(++index, 10.1f);
        statement.setDouble(++index, 10.1);
        statement.setInt(++index, 100);
        statement.setBigDecimal(++index, new BigDecimal("6.626"));
        statement.setTimestamp(
            ++index, Timestamp.parseTimestamp("2022-02-11T13:45:00.123456+01:00").toSqlTimestamp());
        statement.setDate(++index, Date.valueOf("2022-04-29"));
        statement.setString(++index, "string_test");

        assertEquals(1, statement.executeUpdate());
      }

      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select * from all_types where col_bigint=2")) {
        assertTrue(resultSet.next());

        int index = 0;
        assertEquals(2, resultSet.getLong(++index));
        assertTrue(resultSet.getBoolean(++index));
        assertArrayEquals(
            "bytes_test".getBytes(StandardCharsets.UTF_8), resultSet.getBytes(++index));
        assertEquals(10.1f, resultSet.getFloat(++index), 0.0f);
        assertEquals(10.1d, resultSet.getDouble(++index), 0.0d);
        assertEquals(100, resultSet.getInt(++index));
        assertEquals(new BigDecimal("6.626"), resultSet.getBigDecimal(++index));
        assertEquals(
            Timestamp.parseTimestamp("2022-02-11T13:45:00.123456+01:00").toSqlTimestamp(),
            resultSet.getTimestamp(++index));
        assertEquals(Date.valueOf("2022-04-29"), resultSet.getDate(++index));
        assertEquals("string_test", resultSet.getString(++index));

        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testUpdateWithParameters() throws SQLException {
    String sql =
        "update all_types set "
            + "col_bool=?, "
            + "col_bytea=?, "
            + "col_float4=?, "
            + "col_float8=?, "
            + "col_int=?, "
            + "col_numeric=?, "
            + "col_timestamptz=?, "
            + "col_date=?, "
            + "col_varchar=? "
            + "where col_bigint=?";

    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (PreparedStatement statement = connection.prepareStatement(sql)) {
        // This forces the PG JDBC driver to use binary transfer mode for the results, and will
        // also cause it to send a DescribeStatement message.
        statement.unwrap(PgStatement.class).setPrepareThreshold(-1);

        int index = 0;
        statement.setBoolean(++index, false);
        statement.setBytes(++index, "updated".getBytes(StandardCharsets.UTF_8));
        statement.setDouble(++index, 3.14f * 2f);
        statement.setDouble(++index, 3.14d * 2d);
        statement.setInt(++index, 2);
        statement.setBigDecimal(++index, new BigDecimal("10.0"));
        // Note that PostgreSQL does not support nanosecond precision, so the JDBC driver therefore
        // truncates this value before it is sent to PG.
        statement.setTimestamp(
            ++index,
            Timestamp.parseTimestamp("2022-02-11T14:04:59.123456789+01:00").toSqlTimestamp());
        statement.setDate(++index, Date.valueOf("2000-02-29"));
        statement.setString(++index, "updated");
        statement.setLong(++index, 1);

        assertEquals(1, statement.executeUpdate());
      }

      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select * from all_types where col_bigint=1")) {
        assertTrue(resultSet.next());

        int index = 0;
        assertEquals(1, resultSet.getLong(++index));
        assertFalse(resultSet.getBoolean(++index));
        assertArrayEquals("updated".getBytes(StandardCharsets.UTF_8), resultSet.getBytes(++index));
        assertEquals(3.14f * 2f, resultSet.getFloat(++index), 0.0f);
        assertEquals(3.14d * 2d, resultSet.getDouble(++index), 0.0d);
        assertEquals(2, resultSet.getInt(++index));
        assertEquals(new BigDecimal("10.0"), resultSet.getBigDecimal(++index));
        // Note: The JDBC driver already truncated the timestamp value before it was sent to PG.
        // So here we read back the truncated value.
        assertEquals(
            Timestamp.parseTimestamp("2022-02-11T14:04:59.123457+01:00").toSqlTimestamp(),
            resultSet.getTimestamp(++index));
        assertEquals(Date.valueOf("2000-02-29"), resultSet.getDate(++index));
        assertEquals("updated", resultSet.getString(++index));

        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testNullValues() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (PreparedStatement statement =
          connection.prepareStatement(
              "insert into all_types "
                  + "(col_bigint, col_bool, col_bytea, col_float4, col_float8, col_int, col_numeric, "
                  + "col_timestamptz, col_date, col_varchar) "
                  + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
        // This forces the PG JDBC driver to use binary transfer mode for the results, and will
        // also cause it to send a DescribeStatement message.
        statement.unwrap(PgStatement.class).setPrepareThreshold(-1);

        int index = 0;
        statement.setLong(++index, 2);
        statement.setNull(++index, Types.BOOLEAN);
        statement.setNull(++index, Types.BINARY);
        statement.setNull(++index, Types.REAL);
        statement.setNull(++index, Types.DOUBLE);
        statement.setNull(++index, Types.INTEGER);
        statement.setNull(++index, Types.NUMERIC);
        statement.setNull(++index, Types.TIMESTAMP_WITH_TIMEZONE);
        statement.setNull(++index, Types.DATE);
        statement.setNull(++index, Types.VARCHAR);

        assertEquals(1, statement.executeUpdate());
      }

      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select * from all_types where col_bigint=2")) {
        assertTrue(resultSet.next());

        int index = 0;
        assertEquals(2, resultSet.getLong(++index));

        // Note: JDBC returns the zero-value for primitive types if the value is NULL, and you have
        // to call wasNull() to determine whether the value was NULL or zero.
        assertFalse(resultSet.getBoolean(++index));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getBytes(++index));
        assertTrue(resultSet.wasNull());
        assertEquals(0f, resultSet.getFloat(++index), 0.0f);
        assertTrue(resultSet.wasNull());
        assertEquals(0d, resultSet.getDouble(++index), 0.0d);
        assertTrue(resultSet.wasNull());
        assertEquals(0, resultSet.getInt(++index));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getBigDecimal(++index));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getTimestamp(++index));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getDate(++index));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getString(++index));
        assertTrue(resultSet.wasNull());

        assertFalse(resultSet.next());
      }
    }
  }
}
