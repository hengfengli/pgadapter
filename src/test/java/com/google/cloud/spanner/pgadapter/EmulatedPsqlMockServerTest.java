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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.MockSpannerServiceImpl.SimulatedExecutionTime;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement;
import com.google.cloud.spanner.pgadapter.utils.ClientAutoDetector;
import com.google.cloud.spanner.pgadapter.utils.ClientAutoDetector.WellKnownClient;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.rpc.ResourceInfo;
import com.google.spanner.admin.database.v1.Database;
import com.google.spanner.admin.database.v1.DatabaseDialect;
import com.google.spanner.admin.database.v1.ListDatabasesResponse;
import com.google.spanner.admin.instance.v1.Instance;
import com.google.spanner.admin.instance.v1.ListInstanceConfigsResponse;
import com.google.spanner.admin.instance.v1.ListInstancesResponse;
import com.google.spanner.v1.DatabaseName;
import com.google.spanner.v1.ExecuteBatchDmlRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ExecuteSqlRequest.QueryMode;
import com.google.spanner.v1.SessionName;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.lite.ProtoLiteUtils;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.util.PSQLException;

@RunWith(JUnit4.class)
public class EmulatedPsqlMockServerTest extends AbstractMockServerTest {

  private static final String INSERT1 = "insert into foo values (1)";
  private static final String INSERT2 = "insert into foo values (2)";

  @BeforeClass
  public static void loadPgJdbcDriver() throws Exception {
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");
  }

  @BeforeClass
  public static void setDetectClient() {
    ClientAutoDetector.FORCE_DETECT_CLIENT.set(WellKnownClient.PSQL);
  }

  @AfterClass
  public static void clearDetectClient() {
    ClientAutoDetector.FORCE_DETECT_CLIENT.set(null);
  }

  @BeforeClass
  public static void startMockSpannerAndPgAdapterServers() throws Exception {
    // Start PGAdapter without a default database.
    doStartMockSpannerAndPgAdapterServers(null, builder -> {});

    mockSpanner.putStatementResults(
        StatementResult.update(Statement.of(INSERT1), 1L),
        StatementResult.update(Statement.of(INSERT2), 1L));
  }

  @After
  public void removeExecutionTimes() {
    mockSpanner.removeAllExecutionTimes();
  }

  /**
   * Creates a JDBC connection string that instructs the PG JDBC driver to use the default simple
   * mode for queries and DML statements. This makes the JDBC driver behave in (much) the same way
   * as psql.
   */
  private String createUrl(String database) {
    return String.format(
        "jdbc:postgresql://localhost:%d/%s?preferQueryMode=simple",
        pgServer.getLocalPort(), database);
  }

  @Test
  public void testConnectToDifferentDatabases() throws SQLException {
    final ImmutableList<String> databases = ImmutableList.of("db1", "db2");
    for (String database : databases) {
      try (Connection connection = DriverManager.getConnection(createUrl(database))) {
        connection.createStatement().execute(INSERT1);
      }
    }

    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(databases.size(), requests.size());
    for (int i = 0; i < requests.size(); i++) {
      assertEquals(databases.get(i), SessionName.parse(requests.get(i).getSession()).getDatabase());
    }
  }

  @Test
  public void testConnectToFullDatabasePath() throws Exception {
    String databaseName =
        "projects/full-path-test-project/instances/full-path-test-instance/databases/full-path-test-database";
    // Note that we need to URL encode the database name as it contains multiple forward slashes.
    try (Connection connection =
        DriverManager.getConnection(
            createUrl(URLEncoder.encode(databaseName, StandardCharsets.UTF_8.name())))) {
      connection.createStatement().execute(INSERT1);
    }

    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(1, requests.size());
    SessionName sessionName = SessionName.parse(requests.get(0).getSession());
    DatabaseName gotDatabaseName =
        DatabaseName.of(
            sessionName.getProject(), sessionName.getInstance(), sessionName.getDatabase());
    assertEquals(DatabaseName.parse(databaseName), gotDatabaseName);
  }

  @Ignore(
      "Skipped because of a bug in the gRPC server implementation that causes random NullPointerExceptions")
  @Test
  public void testConnectToNonExistingDatabase() {
    try {
      mockSpanner.setBatchCreateSessionsExecutionTime(
          SimulatedExecutionTime.stickyDatabaseNotFoundException("non-existing-db"));
      // The Connection API calls listInstanceConfigs(..) once first when the connection is a
      // localhost connection. It does so to verify that the connection is valid and to quickly
      // return an error if someone is for example trying to connect to the emulator while the
      // emulator is not running. This does not happen when you connect to a remote host. We
      // therefore need to add a response for the listInstanceConfigs as well.
      mockInstanceAdmin.addResponse(ListInstanceConfigsResponse.getDefaultInstance());
      mockInstanceAdmin.addResponse(
          Instance.newBuilder()
              .setName("projects/p/instances/i")
              .setConfig("projects/p/instanceConfigs/ic")
              .build());
      mockDatabaseAdmin.addResponse(
          ListDatabasesResponse.newBuilder()
              .addDatabases(
                  Database.newBuilder()
                      .setName("projects/p/instances/i/databases/d")
                      .setDatabaseDialect(DatabaseDialect.POSTGRESQL)
                      .build())
              .addDatabases(
                  Database.newBuilder()
                      .setName("projects/p/instances/i/databases/google-sql-db")
                      .setDatabaseDialect(DatabaseDialect.GOOGLE_STANDARD_SQL)
                      .build())
              .build());

      SQLException exception =
          assertThrows(
              SQLException.class, () -> DriverManager.getConnection(createUrl("non-existing-db")));
      assertTrue(exception.getMessage(), exception.getMessage().contains("NOT_FOUND"));
      assertTrue(
          exception.getMessage(),
          exception
              .getMessage()
              .contains(
                  "These PostgreSQL databases are available on instance projects/p/instances/i:"));
      assertTrue(
          exception.getMessage(),
          exception.getMessage().contains("\tprojects/p/instances/i/databases/d\n"));
      assertFalse(
          exception.getMessage(),
          exception.getMessage().contains("\tprojects/p/instances/i/databases/google-sql-db\n"));
    } finally {
      closeSpannerPool(true);
    }
  }

  @Test
  public void testConnectToNonExistingInstance() {
    for (boolean isPsql : new boolean[] {true, false}) {
      try {
        if (isPsql) {
          setDetectClient();
        } else {
          clearDetectClient();
        }
        mockSpanner.setExecuteStreamingSqlExecutionTime(
            SimulatedExecutionTime.ofStickyException(
                newStatusResourceNotFoundException(
                    "i",
                    "type.googleapis.com/google.spanner.admin.instance.v1.Instance",
                    "projects/p/instances/i")));
        // The Connection API calls listInstanceConfigs(..) once first when the connection is a
        // localhost connection. It does so to verify that the connection is valid and to quickly
        // return an error if someone is for example trying to connect to the emulator while the
        // emulator is not running. This does not happen when you connect to a remote host. We
        // therefore need to add a response for the listInstanceConfigs as well.
        mockInstanceAdmin.addResponse(ListInstanceConfigsResponse.getDefaultInstance());
        mockInstanceAdmin.addResponse(
            ListInstancesResponse.newBuilder()
                .addInstances(
                    Instance.newBuilder()
                        .setConfig("projects/p/instanceConfigs/ic")
                        .setName("projects/p/instances/i")
                        .build())
                .build());

        SQLException exception =
            assertThrows(
                SQLException.class,
                () -> DriverManager.getConnection(createUrl("non-existing-db")));
        assertTrue(exception.getMessage(), exception.getMessage().contains("NOT_FOUND"));

        assertEquals(
            exception.getMessage(),
            isPsql,
            exception.getMessage().contains("These instances are available in project p:"));
        assertEquals(
            exception.getMessage(),
            isPsql,
            exception.getMessage().contains("\tprojects/p/instances/i\n"));
      } finally {
        closeSpannerPool(true);
        setDetectClient();
      }
    }
  }

  @Test
  public void testConnectFailed() {
    try {
      mockSpanner.setExecuteStreamingSqlExecutionTime(
          SimulatedExecutionTime.ofStickyException(
              Status.INVALID_ARGUMENT.withDescription("test error").asRuntimeException()));
      SQLException exception =
          assertThrows(
              SQLException.class, () -> DriverManager.getConnection(createUrl("non-existing-db")));
      assertTrue(exception.getMessage(), exception.getMessage().contains("test error"));

    } finally {
      closeSpannerPool(true);
    }
  }

  @Test
  public void testTwoInserts() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl("my-db"))) {
      connection.createStatement().execute(String.format("%s; %s", INSERT1, INSERT2));
    }

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    ExecuteBatchDmlRequest request =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class).get(0);
    assertEquals(2, request.getStatementsCount());
    assertEquals(INSERT1, request.getStatements(0).getSql());
    assertEquals(INSERT2, request.getStatements(1).getSql());
  }

  @Test
  public void testNestedBlockComment() throws SQLException {
    String sql1 =
        "/* This block comment surrounds a query which itself has a block comment...\n"
            + "SELECT /* embedded single line */ 'embedded' AS x2;\n"
            + "*/\n"
            + "SELECT 1";
    String sql2 = "-- This is a line comment\n SELECT 2";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql1), SELECT1_RESULTSET));
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql2), SELECT2_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl("my-db"))) {
      try (java.sql.Statement statement = connection.createStatement()) {
        assertTrue(statement.execute(String.format("%s;%s;", sql1, sql2)));
        try (ResultSet resultSet = statement.getResultSet()) {
          assertTrue(resultSet.next());
          assertEquals(1L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }
        assertTrue(statement.getMoreResults());
        try (ResultSet resultSet = statement.getResultSet()) {
          assertTrue(resultSet.next());
          assertEquals(2L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }
        assertFalse(statement.getMoreResults());
      }
    }
  }

  @Test
  public void testPrepareExecuteDeallocate() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl("my-db"))) {
      connection.createStatement().execute("prepare my_prepared_statement as SELECT 1");

      assertEquals(1, pgServer.getNumberOfConnections());
      ConnectionHandler connectionHandler = pgServer.getConnectionHandlers().get(0);
      IntermediateStatement preparedStatement =
          connectionHandler.getStatement("my_prepared_statement");
      assertNotNull(preparedStatement);
      assertEquals("SELECT 1", preparedStatement.getStatement());

      try (java.sql.Statement statement = connection.createStatement()) {
        assertTrue(statement.execute("execute my_prepared_statement"));
        try (ResultSet resultSet = statement.getResultSet()) {
          assertTrue(resultSet.next());
          assertEquals(1L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }
      }

      connection.createStatement().execute("deallocate my_prepared_statement");
      PGException exception =
          assertThrows(
              PGException.class, () -> connectionHandler.getStatement("my_prepared_statement"));
      assertEquals(
          "prepared statement my_prepared_statement does not exist", exception.getMessage());

      SQLException sqlException =
          assertThrows(
              SQLException.class,
              () -> connection.createStatement().execute("execute my_prepared_statement"));
      assertEquals(
          "ERROR: prepared statement my_prepared_statement does not exist",
          sqlException.getMessage());
    }
  }

  @Test
  public void testPrepareInvalidStatement() throws SQLException {
    // Register an error for an invalid statement.
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of("SELECT"),
            Status.INVALID_ARGUMENT
                .withDescription("Statement must produce at least one output column")
                .asRuntimeException()));

    try (Connection connection = DriverManager.getConnection(createUrl("my-db"))) {
      // Try to create a prepared statement using the invalid SELECT statement.
      PSQLException exception =
          assertThrows(
              PSQLException.class,
              () ->
                  connection.createStatement().execute("prepare my_prepared_statement as SELECT"));
      assertTrue(
          exception.getMessage().contains("Statement must produce at least one output column"));

      // Verify that we can create a prepared statement with the same name without having to drop it
      // first.
      connection.createStatement().execute("prepare my_prepared_statement as SELECT 1");
      // Verify that we can now use the prepared statement.
      try (java.sql.Statement statement = connection.createStatement()) {
        assertTrue(statement.execute("execute my_prepared_statement"));
        try (ResultSet resultSet = statement.getResultSet()) {
          assertTrue(resultSet.next());
          assertEquals(1L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }
      }
    }
  }

  @Test
  public void testRoundParamValueForPreparedStatement() throws SQLException {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of("select * from my_table where id=$1"),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createAllTypesResultSetMetadata("")
                        .toBuilder()
                        .setUndeclaredParameters(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p1")
                                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                        .build())
                                .build()))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder("select * from my_table where id=$1").bind("p1").to(2L).build(),
            createAllTypesResultSet("")));

    try (Connection connection = DriverManager.getConnection(createUrl("my-db"))) {
      connection
          .createStatement()
          .execute("prepare my_prepared_statement as select * from my_table where id=$1");
      // 1.5 is automatically rounded to 2 because the parameter type has been inferred as bigint.
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("execute my_prepared_statement (1.5)")) {
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
      }
    }
    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest executeRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1);
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
    assertEquals(1, executeRequest.getParamTypesCount());
    assertEquals(
        Type.newBuilder().setCode(TypeCode.INT64).build(),
        executeRequest.getParamTypesMap().get("p1"));
    assertEquals("2", executeRequest.getParams().getFieldsMap().get("p1").getStringValue());
  }

  @Test
  public void testTimezone() throws SQLException {
    String sql = "select ts from foo";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(createMetadata(ImmutableList.of(TypeCode.TIMESTAMP)))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(
                            Value.newBuilder()
                                .setStringValue("2023-01-06T11:49:15.123456789Z")
                                .build())
                        .build())
                .build()));

    try (Connection connection = DriverManager.getConnection(createUrl("my-db"))) {
      connection.createStatement().execute("set time zone cet");
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        while (resultSet.next()) {
          assertEquals("2023-01-06 12:49:15.123456+01", resultSet.getString(1));
        }
      }
      connection.createStatement().execute("set time zone ist");
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        while (resultSet.next()) {
          assertEquals("2023-01-06 17:19:15.123456+05:30", resultSet.getString(1));
        }
      }
      connection.createStatement().execute("set time zone 'America/Los_Angeles'");
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        while (resultSet.next()) {
          assertEquals("2023-01-06 03:49:15.123456-08", resultSet.getString(1));
        }
      }
      connection.createStatement().execute("set time zone -12");
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        while (resultSet.next()) {
          assertEquals("2023-01-05 23:49:15.123456-12", resultSet.getString(1));
        }
      }
    }
  }

  @Test
  public void testDateForTimestamptzParameter() throws SQLException {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of("select * from my_table where ts=$1"),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createAllTypesResultSetMetadata("")
                        .toBuilder()
                        .setUndeclaredParameters(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p1")
                                        .setType(
                                            Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                                        .build())
                                .build()))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder("select * from my_table where ts=$1")
                .bind("p1")
                .to(Timestamp.parseTimestamp("2022-12-27T23:00:00Z"))
                .build(),
            createAllTypesResultSet("")));

    try (Connection connection = DriverManager.getConnection(createUrl("my-db"))) {
      connection.createStatement().execute("set time zone 'Europe/Amsterdam'");
      connection
          .createStatement()
          .execute("prepare my_prepared_statement as select * from my_table where ts=$1");
      // '2022-12-28' is interpreted in timezone 'Europe/Amsterdam', which means
      // '2022-12-28T23:00:00Z'.
      try (ResultSet resultSet =
          connection
              .createStatement()
              .executeQuery("execute my_prepared_statement ('2022-12-28')")) {
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
      }
    }
    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest executeRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1);
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
    assertEquals(1, executeRequest.getParamTypesCount());
    assertEquals(
        Type.newBuilder().setCode(TypeCode.TIMESTAMP).build(),
        executeRequest.getParamTypesMap().get("p1"));
    assertEquals(
        "2022-12-27T23:00:00Z",
        executeRequest.getParams().getFieldsMap().get("p1").getStringValue());
  }

  static StatusRuntimeException newStatusResourceNotFoundException(
      String shortName, String resourceType, String resourceName) {
    ResourceInfo resourceInfo =
        ResourceInfo.newBuilder()
            .setResourceType(resourceType)
            .setResourceName(resourceName)
            .build();
    Metadata.Key<ResourceInfo> key =
        Metadata.Key.of(
            resourceInfo.getDescriptorForType().getFullName() + Metadata.BINARY_HEADER_SUFFIX,
            ProtoLiteUtils.metadataMarshaller(resourceInfo));
    Metadata trailers = new Metadata();
    trailers.put(key, resourceInfo);
    String message =
        String.format("%s not found: %s with id %s not found", shortName, shortName, resourceName);
    return Status.NOT_FOUND.withDescription(message).asRuntimeException(trailers);
  }
}
