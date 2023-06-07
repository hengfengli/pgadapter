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

package com.google.cloud.spanner.pgadapter.statements;

import com.google.cloud.spanner.AbstractLazyInitializer;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.ConnectionOptionsHelper;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

/**
 * {@link DdlExecutor} inspects DDL statements before executing these to support commonly used DDL
 * features that are not (yet) supported by the backend.
 */
class DdlExecutor {
  /**
   * This class checks whether the backend supports 'if [not] exists' style statements. This is used
   * to determine whether PGAdapter should do the existence check itself, or whether it should send
   * the DDL statement unmodified to the backend. The check is only executed once for each
   * connection, and only if at least one 'if [not] exists' statement is executed.
   */
  static class BackendSupportsIfExists extends AbstractLazyInitializer<Boolean> {
    private final DatabaseId databaseId;
    private final Connection connection;

    BackendSupportsIfExists(DatabaseId databaseId, Connection connection) {
      this.databaseId = databaseId;
      this.connection = connection;
    }

    @Override
    protected Boolean initialize() {
      Spanner spanner = ConnectionOptionsHelper.getSpanner(this.connection);
      DatabaseAdminClient adminClient = spanner.getDatabaseAdminClient();
      try {
        // NOTE: This update will *NEVER* succeed. Either, the first statement fails to parse
        // because the backend does not support 'if [not] exists'-style statements, or the second
        // statement fails because it misses a data type for the `id` column. The error that is
        // returned indicates whether the backend supports `if [not] exists`, as Spanner will parse
        // the statements in order, and return an error for the first statement that fails.
        adminClient
            .updateDatabaseDdl(
                this.databaseId.getInstanceId().getInstance(),
                this.databaseId.getDatabase(),
                ImmutableList.of(
                    "create table if not exists test (id bigint primary key)",
                    "create table invalid (id primary key)"),
                null)
            .get();
        // This statement should not be reachable, as at least one of the statements should fail.
        return Boolean.TRUE;
      } catch (ExecutionException exception) {
        SpannerException spannerException =
            SpannerExceptionFactory.asSpannerException(exception.getCause());
        if (spannerException.getErrorCode() == ErrorCode.INVALID_ARGUMENT
            && spannerException
                .getMessage()
                .contains("<IF NOT EXISTS> clause is not supported in <CREATE TABLE> statement")) {
          return Boolean.FALSE;
        }
        return Boolean.TRUE;
      } catch (InterruptedException interruptedException) {
        throw PGExceptionFactory.newQueryCancelledException();
      }
    }
  }

  /**
   * {@link NotExecuted} is used to indicate that a statement was skipped because the 'if [not]
   * exists' check indicated that the statement should be skipped.
   */
  static final class NotExecuted implements StatementResult {
    @Override
    public ResultType getResultType() {
      return ResultType.NO_RESULT;
    }

    @Override
    public ClientSideStatementType getClientSideStatementType() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getResultSet() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Long getUpdateCount() {
      throw new UnsupportedOperationException();
    }
  }

  private static final NotExecuted NOT_EXECUTED = new NotExecuted();
  private final AbstractLazyInitializer<Boolean> backendSupportsIfExists;
  private final BackendConnection backendConnection;
  private final Connection connection;

  /**
   * Constructor only intended for testing. This will create an executor that always assumes that
   * the backend does not support 'if [not] exists'.
   */
  @VisibleForTesting
  DdlExecutor(BackendConnection backendConnection) {
    this(
        new AbstractLazyInitializer<Boolean>() {
          @Override
          protected Boolean initialize() {
            return Boolean.FALSE;
          }
        },
        backendConnection);
  }

  /** Constructor for a {@link DdlExecutor} for the given database and connection. */
  DdlExecutor(DatabaseId databaseId, BackendConnection backendConnection) {
    this(
        new BackendSupportsIfExists(databaseId, backendConnection.getSpannerConnection()),
        backendConnection);
  }

  private DdlExecutor(
      AbstractLazyInitializer<Boolean> backendSupportsIfExists,
      BackendConnection backendConnection) {
    this.backendConnection = backendConnection;
    this.connection = backendConnection.getSpannerConnection();
    this.backendSupportsIfExists = backendSupportsIfExists;
  }

  /**
   * Removes any quotes around the given identifier, and folds the identifier to lower case if it
   * was not quoted.
   */
  static String unquoteIdentifier(String identifier) {
    if (identifier.length() < 2) {
      return identifier;
    }
    if (identifier.startsWith("\"")) {
      return identifier.substring(1, identifier.length() - 1);
    }
    return identifier.toLowerCase();
  }

  /**
   * Executes the given DDL statement. This can include translating the statement into a check
   * whether the to create/drop object exists, and skipping the actual execution of the DDL
   * statement depending on the result of the `if [not] exists` check.
   */
  StatementResult execute(ParsedStatement parsedStatement, Statement statement) {
    Statement translated = translate(parsedStatement, statement);
    if (translated != null) {
      if (!backendConnection.getSessionState().isSupportDropCascade()) {
        return connection.execute(translated);
      } else {
        ImmutableList<Statement> allStatements = getDependentStatements(translated);
        if (allStatements.size() == 1) {
          return connection.execute(allStatements.get(0));
        } else {
          boolean startedBatch = false;
          try {
            if (!connection.isDdlBatchActive()) {
              connection.execute(Statement.of("START BATCH DDL"));
              startedBatch = true;
            }
            StatementResult result = null;
            for (Statement dropDependency : allStatements) {
              result = connection.execute(dropDependency);
            }
            if (startedBatch) {
              result = connection.execute(Statement.of("RUN BATCH"));
            }
            return result;
          } catch (Throwable t) {
            if (startedBatch && connection.isDdlBatchActive()) {
              connection.abortBatch();
            }
            throw t;
          }
        }
      }
    }
    return NOT_EXECUTED;
  }

  /**
   * Translates a DDL statement that can contain an `if [not] exists` statement into one that is
   * supported by the backend.
   */
  Statement translate(ParsedStatement parsedStatement, Statement statement) {
    SimpleParser parser = new SimpleParser(parsedStatement.getSqlWithoutComments());
    if (parser.eatKeyword("create")) {
      statement = translateCreate(parser, statement);
    } else if (parser.eatKeyword("drop")) {
      statement = translateDrop(parser, statement);
    }

    return statement;
  }

  Statement translateCreate(SimpleParser parser, Statement statement) {
    if (parser.eatKeyword("table")) {
      Statement createTableStatement = translateCreateTable(parser, statement);
      if (createTableStatement == null) {
        return null;
      }
      return maybeRemovePrimaryKeyConstraintName(createTableStatement);
    }
    boolean unique = parser.eatKeyword("unique");
    if (parser.eatKeyword("index")) {
      return translateCreateIndex(parser, statement, unique);
    }
    return statement;
  }

  Statement translateDrop(SimpleParser parser, Statement statement) {
    if (parser.eatKeyword("table")) {
      return translateDropTable(parser, statement);
    }
    if (parser.eatKeyword("index")) {
      return translateDropIndex(parser, statement);
    }
    return statement;
  }

  Statement translateCreateTable(SimpleParser parser, Statement statement) {
    return translateCreateTableOrIndex(parser, statement, "table", this::tableExists);
  }

  Statement translateCreateIndex(SimpleParser parser, Statement statement, boolean unique) {
    return translateCreateTableOrIndex(
        parser, statement, unique ? "unique index" : "index", this::indexExists);
  }

  Statement translateDropTable(SimpleParser parser, Statement statement) {
    return translateDropTableOrIndex(parser, statement, "table", this::tableExists);
  }

  Statement translateDropIndex(SimpleParser parser, Statement statement) {
    return translateDropTableOrIndex(parser, statement, "index", this::indexExists);
  }

  private Statement translateCreateTableOrIndex(
      SimpleParser parser,
      Statement statement,
      String type,
      Function<TableOrIndexName, Boolean> existsFunction) {
    if (!parser.eatKeyword("if", "not", "exists")) {
      return statement;
    }
    return maybeExecuteStatement(
        parser, statement, "create", type, name -> !existsFunction.apply(name));
  }

  private Statement translateDropTableOrIndex(
      SimpleParser parser,
      Statement statement,
      String type,
      Function<TableOrIndexName, Boolean> existsFunction) {
    if (!parser.eatKeyword("if", "exists")) {
      return statement;
    }
    return maybeExecuteStatement(parser, statement, "drop", type, existsFunction);
  }

  /** Executes the given DDL statement if the shouldExecuteFunction indicates so. */
  Statement maybeExecuteStatement(
      SimpleParser parser,
      Statement statement,
      String command,
      String type,
      Function<TableOrIndexName, Boolean> shouldExecuteFunction) {
    if (backendSupportsIfExists()) {
      return statement;
    }
    int identifierPosition = parser.getPos();
    if (identifierPosition >= parser.getSql().length()) {
      return statement;
    }
    TableOrIndexName name = parser.readTableOrIndexName();
    if (name == null) {
      return statement;
    }
    // Check if the table exists or not.
    if (!shouldExecuteFunction.apply(name)) {
      // Return null to indicate that the statement should be skipped.
      return null;
    }

    // Return the DDL statement without the 'if [not] exists' clause.
    return Statement.of(command + " " + type + parser.getSql().substring(identifierPosition));
  }

  boolean tableExists(TableOrIndexName tableName) {
    DatabaseClient client = connection.getDatabaseClient();
    try (ResultSet resultSet =
        client
            .singleUse()
            .executeQuery(
                Statement.newBuilder(
                        "select 1 from information_schema.tables where table_schema=$1 and table_name=$2")
                    .bind("p1")
                    .to(
                        unquoteIdentifier(
                            tableName.schema == null
                                ? backendConnection.getCurrentSchema()
                                : tableName.schema))
                    .bind("p2")
                    .to(unquoteIdentifier(tableName.name))
                    .build())) {
      return resultSet.next();
    }
  }

  boolean indexExists(TableOrIndexName indexName) {
    DatabaseClient client = connection.getDatabaseClient();
    try (ResultSet resultSet =
        client
            .singleUse()
            .executeQuery(
                Statement.newBuilder(
                        "select 1 from information_schema.indexes where table_schema=$1 and index_name=$2")
                    .bind("p1")
                    .to(
                        unquoteIdentifier(
                            indexName.schema == null
                                ? backendConnection.getCurrentSchema()
                                : indexName.schema))
                    .bind("p2")
                    .to(unquoteIdentifier(indexName.name))
                    .build())) {
      return resultSet.next();
    }
  }

  /**
   * Returns true if the backend supports `if [not] exists`-style statements. This will cause the
   * {@link DdlExecutor} to send these statements unmodified to the backend.
   */
  boolean backendSupportsIfExists() {
    try {
      return backendSupportsIfExists.get();
    } catch (Exception exception) {
      throw SpannerExceptionFactory.asSpannerException(exception);
    }
  }

  Statement maybeRemovePrimaryKeyConstraintName(Statement createTableStatement) {
    ParsedStatement parsedStatement =
        AbstractStatementParser.getInstance(Dialect.POSTGRESQL).parse(createTableStatement);
    SimpleParser parser = new SimpleParser(parsedStatement.getSqlWithoutComments());
    parser.eatKeyword("create", "table", "if", "not", "exists");
    TableOrIndexName tableName = parser.readTableOrIndexName();
    if (tableName == null) {
      return createTableStatement;
    }
    if (!parser.eatToken("(")) {
      return createTableStatement;
    }
    while (true) {
      if (parser.eatToken(")")) {
        break;
      } else if (parser.eatToken(",")) {
        continue;
      } else if (parser.peekKeyword("constraint")) {
        int positionBeforeConstraintDefinition = parser.getPos();
        parser.eatKeyword("constraint");
        String constraintName = unquoteIdentifier(parser.readIdentifierPart());
        int positionAfterConstraintName = parser.getPos();
        if (parser.eatKeyword("primary", "key")) {
          if (!constraintName.equalsIgnoreCase("pk_" + unquoteIdentifier(tableName.name))) {
            return createTableStatement;
          }
          return Statement.of(
              parser.getSql().substring(0, positionBeforeConstraintDefinition)
                  + parser.getSql().substring(positionAfterConstraintName));
        } else {
          parser.parseExpression();
        }
      } else {
        parser.parseExpression();
      }
    }
    return createTableStatement;
  }

  ImmutableList<Statement> getDependentStatements(Statement statement) {
    ImmutableList<Statement> defaultResult = ImmutableList.of(statement);
    SimpleParser parser = new SimpleParser(statement.getSql());
    if (!parser.eatKeyword("drop")) {
      return defaultResult;
    }
    parser.eatKeyword("if", "exists");
    if (parser.eatKeyword("table")) {
      TableOrIndexName tableName = parser.readTableOrIndexName();
      if (tableName == null) {
        return defaultResult;
      }
      boolean cascade = false;
      String sqlWithoutCascade = parser.getSql().substring(0, parser.getPos());
      if (parser.eatKeyword("cascade")) {
        cascade = true;
      } else {
        // This is the default, so no need to register it specifically.
        parser.eatKeyword("restrict");
      }
      if (parser.hasMoreTokens()) {
        return defaultResult;
      }
      ImmutableList.Builder<Statement> builder = ImmutableList.builder();
      builder.addAll(getDropDependentIndexesStatements(tableName));
      if (cascade) {
        // We don't have any way to get the views that depend on this table.
        builder.addAll(getDropDependentForeignKeyConstraintsStatements(tableName));
      }
      builder.add(cascade ? Statement.of(sqlWithoutCascade) : statement);
      return builder.build();
    } else if (parser.eatKeyword("schema")) {
      TableOrIndexName schemaName = parser.readTableOrIndexName();
      if (schemaName == null || schemaName.schema != null) {
        return defaultResult;
      }
      if (!parser.eatKeyword("cascade")) {
        return defaultResult;
      }
      ImmutableList.Builder<Statement> builder = ImmutableList.builder();
      builder.addAll(getDropSchemaIndexesStatements(schemaName));
      builder.addAll(getDropSchemaForeignKeysStatements(schemaName));
      builder.addAll(getDropSchemaViewsStatements(schemaName));
      builder.addAll(getDropSchemaTablesStatements(schemaName));
      return builder.build();
    }
    return defaultResult;
  }

  ImmutableList<Statement> getDropDependentIndexesStatements(TableOrIndexName tableName) {
    DatabaseClient client = connection.getDatabaseClient();
    try (ResultSet resultSet =
        client
            .singleUse()
            .executeQuery(
                Statement.newBuilder(
                        "select table_schema, index_name "
                            + "from information_schema.indexes "
                            + "where table_schema=$1 and table_name=$2 "
                            + "and not index_type = 'PRIMARY_KEY' "
                            + "and spanner_is_managed = 'NO'")
                    .bind("p1")
                    .to(
                        unquoteIdentifier(
                            tableName.schema == null
                                ? backendConnection.getCurrentSchema()
                                : tableName.schema))
                    .bind("p2")
                    .to(unquoteIdentifier(tableName.name))
                    .build())) {
      ImmutableList.Builder<Statement> dropStatements = ImmutableList.builder();
      while (resultSet.next()) {
        dropStatements.add(
            Statement.of(
                String.format(
                    "drop index \"%s\".\"%s\"",
                    resultSet.getString("table_schema"), resultSet.getString("index_name"))));
      }
      return dropStatements.build();
    }
  }

  ImmutableList<Statement> getDropDependentForeignKeyConstraintsStatements(
      TableOrIndexName tableName) {
    DatabaseClient client = connection.getDatabaseClient();
    try (ResultSet resultSet =
        client
            .singleUse()
            .executeQuery(
                Statement.newBuilder(
                        "select child.table_schema, child.table_name, rc.constraint_name\n"
                            + "from information_schema.referential_constraints rc\n"
                            + "inner join information_schema.table_constraints parent\n"
                            + "  on  rc.unique_constraint_catalog=parent.constraint_catalog\n"
                            + "  and rc.unique_constraint_schema=parent.constraint_schema\n"
                            + "  and rc.unique_constraint_name=parent.constraint_name\n"
                            + "inner join information_schema.table_constraints child\n"
                            + "  on  rc.constraint_catalog=child.constraint_catalog\n"
                            + "  and rc.constraint_schema=child.constraint_schema\n"
                            + "  and rc.constraint_name=child.constraint_name\n"
                            + "where parent.table_schema=$1\n"
                            + "and parent.table_name=$2")
                    .bind("p1")
                    .to(
                        unquoteIdentifier(
                            tableName.schema == null
                                ? backendConnection.getCurrentSchema()
                                : tableName.schema))
                    .bind("p2")
                    .to(unquoteIdentifier(tableName.name))
                    .build())) {
      ImmutableList.Builder<Statement> dropStatements = ImmutableList.builder();
      while (resultSet.next()) {
        dropStatements.add(
            Statement.of(
                String.format(
                    "alter table \"%s\".\"%s\" drop constraint \"%s\"",
                    resultSet.getString("table_schema"),
                    resultSet.getString("table_name"),
                    resultSet.getString("constraint_name"))));
      }
      return dropStatements.build();
    }
  }

  ImmutableList<Statement> getDropSchemaForeignKeysStatements(TableOrIndexName schemaName) {
    DatabaseClient client = connection.getDatabaseClient();
    try (ResultSet resultSet =
        client
            .singleUse()
            .executeQuery(
                Statement.newBuilder(
                        "select child.table_schema, child.table_name, rc.constraint_name\n"
                            + "from information_schema.referential_constraints rc\n"
                            + "inner join information_schema.table_constraints parent\n"
                            + "  on  rc.unique_constraint_catalog=parent.constraint_catalog\n"
                            + "  and rc.unique_constraint_schema=parent.constraint_schema\n"
                            + "  and rc.unique_constraint_name=parent.constraint_name\n"
                            + "inner join information_schema.table_constraints child\n"
                            + "  on  rc.constraint_catalog=child.constraint_catalog\n"
                            + "  and rc.constraint_schema=child.constraint_schema\n"
                            + "  and rc.constraint_name=child.constraint_name\n"
                            + "where parent.table_schema=$1")
                    .bind("p1")
                    .to(unquoteIdentifier(schemaName.name))
                    .build())) {
      ImmutableList.Builder<Statement> dropStatements = ImmutableList.builder();
      while (resultSet.next()) {
        dropStatements.add(
            Statement.of(
                String.format(
                    "alter table \"%s\".\"%s\" drop constraint \"%s\"",
                    resultSet.getString("table_schema"),
                    resultSet.getString("table_name"),
                    resultSet.getString("constraint_name"))));
      }
      return dropStatements.build();
    }
  }

  ImmutableList<Statement> getDropSchemaIndexesStatements(TableOrIndexName schemaName) {
    DatabaseClient client = connection.getDatabaseClient();
    try (ResultSet resultSet =
        client
            .singleUse()
            .executeQuery(
                Statement.newBuilder(
                        "select table_schema, index_name "
                            + "from information_schema.indexes "
                            + "where table_schema=$1 "
                            + "and not index_type = 'PRIMARY_KEY' "
                            + "and spanner_is_managed = 'NO'")
                    .bind("p1")
                    .to(unquoteIdentifier(schemaName.name))
                    .build())) {
      ImmutableList.Builder<Statement> dropStatements = ImmutableList.builder();
      while (resultSet.next()) {
        dropStatements.add(
            Statement.of(
                String.format(
                    "drop index \"%s\".\"%s\"",
                    resultSet.getString("table_schema"), resultSet.getString("index_name"))));
      }
      return dropStatements.build();
    }
  }

  private static final class Table implements Comparable<Table> {
    private final String schema;
    private final String name;
    private final String parent;

    Table(String schema, String name, String parent) {
      this.schema = Preconditions.checkNotNull(schema);
      this.name = Preconditions.checkNotNull(name);
      this.parent = parent;
    }

    @Override
    public int hashCode() {
      return Objects.hash(this.schema, this.name);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Table)) {
        return false;
      }
      Table other = (Table) o;
      return Objects.equals(this.schema, other.schema) && Objects.equals(this.name, other.name);
    }

    @Override
    public int compareTo(Table o) {
      if (o.parent != null && Objects.equals(this.name, o.parent)) {
        return 1;
      }
      if (this.parent != null && Objects.equals(this.parent, o.name)) {
        return -1;
      }
      if (o.parent != null && this.parent == null) {
        return 1;
      }
      if (this.parent != null && o.parent == null) {
        return -1;
      }
      if (!Objects.equals(this.schema, o.schema)) {
        return this.schema.compareTo(o.schema);
      }
      return this.name.compareTo(o.name);
    }
  }

  ImmutableList<Statement> getDropSchemaViewsStatements(TableOrIndexName schemaName) {
    DatabaseClient client = connection.getDatabaseClient();
    try (ResultSet resultSet =
        client
            .singleUse()
            .executeQuery(
                Statement.newBuilder(
                        "select table_schema, table_name "
                            + "from information_schema.tables "
                            + "where table_schema=$1 "
                            + "and table_type='VIEW'")
                    .bind("p1")
                    .to(unquoteIdentifier(schemaName.name))
                    .build())) {
      ImmutableList.Builder<Statement> dropStatements = ImmutableList.builder();
      while (resultSet.next()) {
        dropStatements.add(
            Statement.of(
                String.format(
                    "drop view \"%s\".\"%s\"",
                    resultSet.getString("table_schema"), resultSet.getString("table_name"))));
      }
      return dropStatements.build();
    }
  }

  ImmutableList<Statement> getDropSchemaTablesStatements(TableOrIndexName schemaName) {
    DatabaseClient client = connection.getDatabaseClient();
    try (ResultSet resultSet =
        client
            .singleUse()
            .executeQuery(
                Statement.newBuilder(
                        "select table_schema, table_name, parent_table_name "
                            + "from information_schema.tables "
                            + "where table_schema=$1 "
                            + "and table_type='BASE TABLE'")
                    .bind("p1")
                    .to(unquoteIdentifier(schemaName.name))
                    .build())) {
      List<Table> tables = new ArrayList<>();
      while (resultSet.next()) {
        tables.add(
            new Table(
                resultSet.getString("table_schema"),
                resultSet.getString("table_name"),
                resultSet.isNull("parent_table_name")
                    ? null
                    : resultSet.getString("parent_table_name")));
      }
      Collections.sort(tables);
      ImmutableList.Builder<Statement> dropStatements = ImmutableList.builder();
      for (Table table : tables) {
        dropStatements.add(
            Statement.of(String.format("drop table \"%s\".\"%s\"", table.schema, table.name)));
      }
      return dropStatements.build();
    }
  }
}
