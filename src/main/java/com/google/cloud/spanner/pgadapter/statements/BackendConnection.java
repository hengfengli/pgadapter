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

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerBatchUpdateException;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.connection.StatementResult.ClientSideStatementType;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata.DdlTransactionMode;
import com.google.cloud.spanner.pgadapter.wireoutput.ReadyResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.ReadyResponse.Status;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.SettableFuture;
import java.util.LinkedList;
import java.util.Objects;
import java.util.concurrent.Future;

/**
 * This class emulates a backend PostgreSQL connection. Statements are buffered in memory until a
 * flush/sync is received. This makes it possible to batch multiple statements together before
 * sending these to Cloud Spanner. This class also keeps track of the transaction status of the
 * connection.
 */
@InternalApi
public class BackendConnection {
  public static final String TRANSACTION_ABORTED_ERROR =
      "current transaction is aborted, commands ignored until end of transaction block";

  /**
   * Connection state indicates whether the backend connection is idle, in a transaction or in an
   * aborted transaction.
   */
  public enum ConnectionState {
    /** Connection is idle, no transaction is active. */
    IDLE(Status.IDLE),
    /** An implicit or explicit transaction is active. */
    TRANSACTION(Status.TRANSACTION),
    /** The current transaction is aborted. All statements are refused until rollback. */
    ABORTED(Status.FAILED);

    private final ReadyResponse.Status readyResponseStatus;

    ConnectionState(ReadyResponse.Status readyResponseStatus) {
      this.readyResponseStatus = readyResponseStatus;
    }

    public ReadyResponse.Status getReadyResponseStatus() {
      return this.readyResponseStatus;
    }
  }

  /**
   * {@link TransactionMode} indicates whether the current transaction on a backend connection is
   * implicit or explicit. Implicit transactions are automatically committed/rolled back when a
   * batch of statements finishes execution (i.e. when we receive a Sync message).
   */
  enum TransactionMode {
    IMPLICIT,
    EXPLICIT,
  }

  /**
   * Buffered statements are kept in memory until a flush or sync message is received. This makes it
   * possible to batch multiple statements together when sending them to Cloud Spanner.
   */
  abstract class BufferedStatement<T> {
    final ParsedStatement parsedStatement;
    final Statement statement;
    final SettableFuture<T> result;

    BufferedStatement(ParsedStatement parsedStatement, Statement statement) {
      this.parsedStatement = parsedStatement;
      this.statement = statement;
      this.result = SettableFuture.create();
    }

    abstract void execute();

    void checkConnectionState() {
      // Only COMMIT or ROLLBACK is allowed if we are in an ABORTED transaction.
      if (connectionState == ConnectionState.ABORTED
          && !(isCommit(parsedStatement) || isRollback(parsedStatement))) {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT, TRANSACTION_ABORTED_ERROR);
      }
    }
  }

  private final class Execute extends BufferedStatement<StatementResult> {
    Execute(ParsedStatement parsedStatement, Statement statement) {
      super(parsedStatement, statement);
    }

    @Override
    void execute() {
      try {
        checkConnectionState();
        // TODO(b/235719478): If the statement is a BEGIN statement and there is a COMMIT statement
        //  at a later point in the batch, and all the statements in the transaction block are
        //  SELECT statements, then we should create a read-only transaction. Also, if a transaction
        //  block always ends with a ROLLBACK, PGAdapter should skip the entire execution of that
        //  block.
        if (connectionState == ConnectionState.ABORTED
            && !spannerConnection.isInTransaction()
            && (isRollback(parsedStatement) || isCommit(parsedStatement))) {
          result.set(ROLLBACK_RESULT);
        } else if (isBegin(parsedStatement) && spannerConnection.isInTransaction()) {
          // Ignore the statement as it is a no-op to execute BEGIN when we are already in a
          // transaction. TODO: Return a warning.
          result.set(NO_RESULT);
        } else if ((isCommit(parsedStatement) || isRollback(parsedStatement))
            && !spannerConnection.isInTransaction()) {
          // Ignore the statement as it is a no-op to execute COMMIT/ROLLBACK when we are not in a
          // transaction. TODO: Return a warning.
          result.set(NO_RESULT);
        } else if (statement.getSql().isEmpty()) {
          result.set(NO_RESULT);
        } else {
          result.set(spannerConnection.execute(statement));
        }
      } catch (Exception exception) {
        result.setException(exception);
        throw exception;
      }
    }
  }

  private static final StatementResult NO_RESULT = new NoResult();
  private static final StatementResult ROLLBACK_RESULT = new NoResult("ROLLBACK");
  private static final Statement ROLLBACK = Statement.of("ROLLBACK");

  private ConnectionState connectionState = ConnectionState.IDLE;
  private TransactionMode transactionMode = TransactionMode.IMPLICIT;
  private final LinkedList<BufferedStatement<?>> bufferedStatements = new LinkedList<>();
  private final Connection spannerConnection;
  private final DdlTransactionMode ddlTransactionMode;

  /**
   * Creates a PG backend connection that uses the given Spanner {@link Connection} and {@link
   * DdlTransactionMode}.
   */
  BackendConnection(Connection spannerConnection, DdlTransactionMode ddlTransactionMode) {
    this.spannerConnection = spannerConnection;
    this.ddlTransactionMode = ddlTransactionMode;
  }

  /** Returns the current connection state. */
  public ConnectionState getConnectionState() {
    return this.connectionState;
  }

  /**
   * Buffers the given statement for execution on the backend connection when the next flush/sync
   * message is received. The returned future will contain the result of the statement when
   * execution has finished.
   */
  public Future<StatementResult> execute(ParsedStatement parsedStatement, Statement statement) {
    Execute execute = new Execute(parsedStatement, statement);
    bufferedStatements.add(execute);
    return execute.result;
  }

  /** Flushes the buffered statements to Spanner. */
  void flush() {
    flush(false);
  }

  /**
   * Flushes the buffered statements to Spanner and commits/rollbacks the implicit transaction (if
   * any).
   */
  void sync() {
    try {
      flush(true);
    } finally {
      endImplicitTransaction();
    }
  }

  /**
   * Flushes all buffered statements to Cloud Spanner and commits/rolls back the transaction at the
   * end if isSync=true.
   */
  private void flush(boolean isSync) {
    int index = 0;
    try {
      while (index < bufferedStatements.size()) {
        BufferedStatement<?> bufferedStatement = bufferedStatements.get(index);
        maybeBeginImplicitTransaction(index, isSync);
        // Prepare the connection for executing a DDL statement. This could include committing the
        // current transaction, depending on the settings for execute DDL in transactions.
        if (bufferedStatement.parsedStatement.isDdl()) {
          prepareExecuteDdl(bufferedStatement);
        }
        boolean canUseBatch = false;
        if (index < (getStatementCount() - 1)) {
          StatementType statementType = getStatementType(index);
          StatementType nextStatementType = getStatementType(index + 1);
          canUseBatch = canBeBatchedTogether(statementType, nextStatementType);
        }

        if (canUseBatch) {
          index += executeStatementsInBatch(index);
        } else {
          bufferedStatement.execute();

          if (isBegin(index)) {
            transactionMode = TransactionMode.EXPLICIT;
            connectionState = ConnectionState.TRANSACTION;
          } else if (isCommit(index) || isRollback(index)) {
            transactionMode = TransactionMode.IMPLICIT;
            connectionState = ConnectionState.IDLE;
          }
          index++;
        }
      }
    } catch (Exception exception) {
      connectionState = ConnectionState.ABORTED;
      if (spannerConnection.isInTransaction()) {
        spannerConnection.setStatementTag(null);
        spannerConnection.execute(ROLLBACK);
      }
    } finally {
      bufferedStatements.clear();
    }
  }

  /** Starts an implicit transaction if that is necessary. */
  private void maybeBeginImplicitTransaction(int index, boolean isSync) {
    if (connectionState != ConnectionState.IDLE) {
      return;
    }

    // Only start an implicit transaction if we have more than one statement left. Otherwise, just
    // let the Spanner connection execute the statement in auto-commit mode.
    if (isSync && index == bufferedStatements.size() - 1) {
      return;
    }
    // Don't start an implicit transaction if this is already a transaction statement.
    if (isTransactionStatement(index)) {
      return;
    }
    // No need to start a transaction for DDL or client side statements.
    if (bufferedStatements.get(index).parsedStatement.getType() == StatementType.DDL
        || bufferedStatements.get(index).parsedStatement.getType() == StatementType.CLIENT_SIDE) {
      return;
    }
    // If there are only DML statements left, those can be executed as an auto-commit dml batch.
    if (isSync && hasOnlyDmlStatementsAfter(index)) {
      return;
    }

    // We need to start an implicit transaction.
    // Check if a read-only transaction suffices.
    spannerConnection.beginTransaction();
    if (isSync && !hasDmlStatementsAfter(index)) {
      spannerConnection.setTransactionMode(
          com.google.cloud.spanner.connection.TransactionMode.READ_ONLY_TRANSACTION);
    }
    transactionMode = TransactionMode.IMPLICIT;
    connectionState = ConnectionState.TRANSACTION;
  }

  /** Ends the current implicit transaction (if any). */
  private void endImplicitTransaction() {
    // Only touch the transaction if it is an implicit transaction.
    if (transactionMode != TransactionMode.IMPLICIT) {
      return;
    }

    try {
      if (spannerConnection.isInTransaction()) {
        spannerConnection.setStatementTag(null);
        if (connectionState == ConnectionState.ABORTED) {
          spannerConnection.rollback();
        } else {
          spannerConnection.commit();
        }
      }
    } finally {
      connectionState = ConnectionState.IDLE;
    }
  }

  /**
   * Prepares the connection for executing a DDL statement. This can include committing the current
   * transaction.
   *
   * <p>Executing the DDL statement may or may not be allowed depending on the state of the
   * transaction and the selected {@link
   * com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata.DdlTransactionMode}. The method
   * will throw a {@link SpannerException} if executing a DDL statement at this point is not
   * allowed.
   */
  private void prepareExecuteDdl(BufferedStatement<?> bufferedStatement) {
    try {
      // Single statements are simpler to check, so we do that in a separate check.
      if (bufferedStatements.size() == 1) {
        switch (ddlTransactionMode) {
          case Single:
          case Batch:
          case AutocommitImplicitTransaction:
            // Single DDL statements outside explicit transactions are always allowed. For a single
            // statement, there can also not be an implicit transaction that needs to be committed.
            if (transactionMode == TransactionMode.EXPLICIT) {
              throw SpannerExceptionFactory.newSpannerException(
                  ErrorCode.FAILED_PRECONDITION,
                  "DDL statements are only allowed outside explicit transactions.");
            }
            // Fall-through to commit the transaction if necessary.
          case AutocommitExplicitTransaction:
            // DDL statements are allowed even in explicit transactions. Commit any transaction that
            // might be active.
            if (spannerConnection.isInTransaction()) {
              spannerConnection.commit();
              transactionMode = TransactionMode.IMPLICIT;
            }
        }
        return;
      }

      // We are in a batch of statements.
      switch (ddlTransactionMode) {
        case Single:
          throw SpannerExceptionFactory.newSpannerException(
              ErrorCode.FAILED_PRECONDITION,
              "DDL statements are only allowed outside batches and transactions.");
        case Batch:
          if (spannerConnection.isInTransaction()
              || bufferedStatements.stream()
                  .anyMatch(statement -> !statement.parsedStatement.isDdl())) {
            throw SpannerExceptionFactory.newSpannerException(
                ErrorCode.FAILED_PRECONDITION,
                "DDL statements are not allowed in mixed batches or transactions.");
          }
          break;
        case AutocommitImplicitTransaction:
          if (spannerConnection.isInTransaction() && transactionMode != TransactionMode.IMPLICIT) {
            throw SpannerExceptionFactory.newSpannerException(
                ErrorCode.FAILED_PRECONDITION,
                "DDL statements are only allowed outside explicit transactions.");
          }
          // Fallthrough to commit the transaction if necessary.
        case AutocommitExplicitTransaction:
          // Commit any transaction that might be active and allow executing the statement.
          // Switch the execution state to implicit transaction.
          if (spannerConnection.isInTransaction()) {
            spannerConnection.commit();
            transactionMode = TransactionMode.IMPLICIT;
          }
      }
    } catch (SpannerException exception) {
      bufferedStatement.result.setException(exception);
      throw exception;
    }
  }

  private boolean isBegin(int index) {
    return isBegin(bufferedStatements.get(index).parsedStatement);
  }

  private boolean isBegin(ParsedStatement parsedStatement) {
    return parsedStatement.getType() == StatementType.CLIENT_SIDE
        && parsedStatement.getClientSideStatementType() == ClientSideStatementType.BEGIN;
  }

  private boolean isCommit(int index) {
    return isCommit(bufferedStatements.get(index).parsedStatement);
  }

  private boolean isCommit(ParsedStatement parsedStatement) {
    return parsedStatement.getType() == StatementType.CLIENT_SIDE
        && parsedStatement.getClientSideStatementType() == ClientSideStatementType.COMMIT;
  }

  private boolean isRollback(int index) {
    return isRollback(bufferedStatements.get(index).parsedStatement);
  }

  private boolean isRollback(ParsedStatement parsedStatement) {
    return parsedStatement.getType() == StatementType.CLIENT_SIDE
        && parsedStatement.getClientSideStatementType() == ClientSideStatementType.ROLLBACK;
  }

  private boolean isTransactionStatement(int index) {
    return isBegin(index) || isCommit(index) || isRollback(index);
  }

  private boolean hasDmlStatementsAfter(int index) {
    return bufferedStatements.subList(index, bufferedStatements.size()).stream()
        .anyMatch(statement -> statement.parsedStatement.getType() == StatementType.UPDATE);
  }

  private boolean hasOnlyDmlStatementsAfter(int index) {
    return bufferedStatements.subList(index, bufferedStatements.size()).stream()
        .allMatch(statement -> statement.parsedStatement.getType() == StatementType.UPDATE);
  }

  private int getStatementCount() {
    return bufferedStatements.size();
  }

  private StatementType getStatementType(int index) {
    return bufferedStatements.get(index).parsedStatement.getType();
  }

  private boolean canBeBatchedTogether(StatementType statementType1, StatementType statementType2) {
    if (Objects.equals(statementType1, StatementType.QUERY)
        || Objects.equals(statementType1, StatementType.CLIENT_SIDE)) {
      return false;
    }
    return Objects.equals(statementType1, statementType2);
  }

  /**
   * Executes the statements from fromIndex in a DML/DDL batch. The batch will consist of all
   * statements from fromIndex till the first statement that is of a different type than the
   * statement at fromIndex. That is; If the first statement is a DML statement, the batch will
   * contain all statements that follow until it encounters a statement that is not a DML statement.
   * The same also applies to DDL statements. Query statements and other statements can not be
   * batched.
   *
   * @param fromIndex The index of the statements array where the batch should start
   * @return The number of statements included in the batch.
   */
  private int executeStatementsInBatch(int fromIndex) {
    Preconditions.checkArgument(fromIndex < getStatementCount() - 1);
    Preconditions.checkArgument(
        canBeBatchedTogether(getStatementType(fromIndex), getStatementType(fromIndex + 1)));
    StatementType batchType = getStatementType(fromIndex);
    switch (batchType) {
      case UPDATE:
        spannerConnection.startBatchDml();
        break;
      case DDL:
        spannerConnection.startBatchDdl();
        break;
      default:
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT, "Statement type is not supported for batching");
    }
    int index = fromIndex;
    while (index < getStatementCount()) {
      StatementType statementType = getStatementType(index);
      if (canBeBatchedTogether(batchType, statementType)) {
        spannerConnection.execute(bufferedStatements.get(index).statement);
        index++;
      } else {
        // End the batch here, as the statement type on this index can not be batched together with
        // the other statements in the batch.
        break;
      }
    }
    try {
      long[] counts = spannerConnection.runBatch();
      updateBatchResultCount(fromIndex, counts);
    } catch (SpannerBatchUpdateException batchUpdateException) {
      long[] counts = batchUpdateException.getUpdateCounts();
      updateBatchResultCount(fromIndex, counts);
      Execute failedExecute = (Execute) bufferedStatements.get(fromIndex + counts.length);
      failedExecute.result.setException(batchUpdateException);
      throw batchUpdateException;
    }
    return index - fromIndex;
  }

  /** Updates the results of the buffered statements after finishing executing a batch. */
  private void updateBatchResultCount(int fromIndex, long[] updateCounts) {
    for (int index = fromIndex; index < fromIndex + updateCounts.length; index++) {
      Execute execute = (Execute) bufferedStatements.get(index);
      if (execute.parsedStatement.getType() == StatementType.DDL) {
        execute.result.set(new NoResult());
      } else {
        execute.result.set(new UpdateCount(updateCounts[index - fromIndex]));
      }
    }
  }

  /**
   * {@link StatementResult} implementation for statements that do not return anything (e.g. DDL).
   */
  @InternalApi
  public static final class NoResult implements StatementResult {
    private final String commandTag;

    private NoResult() {
      this.commandTag = null;
    }

    private NoResult(String commandTag) {
      this.commandTag = commandTag;
    }

    @Override
    public ResultType getResultType() {
      return ResultType.NO_RESULT;
    }

    public boolean hasCommandTag() {
      return this.commandTag != null;
    }

    public String getCommandTag() {
      return this.commandTag;
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

  /**
   * Implementation of {@link StatementResult} for statements that return an update count (e.g.
   * DML).
   */
  @InternalApi
  public static final class UpdateCount implements StatementResult {
    private final Long updateCount;

    public UpdateCount(Long updateCount) {
      this.updateCount = updateCount;
    }

    @Override
    public ResultType getResultType() {
      return ResultType.UPDATE_COUNT;
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
      return updateCount;
    }
  }
}
