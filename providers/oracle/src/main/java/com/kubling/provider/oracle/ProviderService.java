package com.kubling.provider.oracle;

import com.kubling.provider.grpc.BeginTransactionRequest;
import com.kubling.provider.grpc.BeginTransactionResponse;
import com.kubling.provider.grpc.BeginTransactionBranchRequest;
import com.kubling.provider.grpc.BeginTransactionBranchResponse;
import com.kubling.provider.grpc.CloseConnectionRequest;
import com.kubling.provider.grpc.CloseConnectionResponse;
import com.kubling.provider.grpc.CommitTransactionRequest;
import com.kubling.provider.grpc.CommitTransactionResponse;
import com.kubling.provider.grpc.CommitTransactionBranchRequest;
import com.kubling.provider.grpc.CommitTransactionBranchResponse;
import com.kubling.provider.grpc.DeleteRequest;
import com.kubling.provider.grpc.DeleteResponse;
import com.kubling.provider.grpc.GetCapabilitiesRequest;
import com.kubling.provider.grpc.GetCapabilitiesResponse;
import com.kubling.provider.grpc.GetSchemaRequest;
import com.kubling.provider.grpc.GetSchemaResponse;
import com.kubling.provider.grpc.GetTransactionBranchStatusRequest;
import com.kubling.provider.grpc.GetTransactionBranchStatusResponse;
import com.kubling.provider.grpc.HealthRequest;
import com.kubling.provider.grpc.HealthResponse;
import com.kubling.provider.grpc.InsertRequest;
import com.kubling.provider.grpc.InsertResponse;
import com.kubling.provider.grpc.IsInTransactionRequest;
import com.kubling.provider.grpc.IsInTransactionResponse;
import com.kubling.provider.grpc.OpenConnectionRequest;
import com.kubling.provider.grpc.OpenConnectionResponse;
import com.kubling.provider.grpc.ProviderServiceGrpc;
import com.kubling.provider.grpc.QueryRequest;
import com.kubling.provider.grpc.QueryResponse;
import com.kubling.provider.grpc.RollbackTransactionRequest;
import com.kubling.provider.grpc.RollbackTransactionResponse;
import com.kubling.provider.grpc.RollbackTransactionBranchRequest;
import com.kubling.provider.grpc.RollbackTransactionBranchResponse;
import com.kubling.provider.grpc.PrepareTransactionBranchRequest;
import com.kubling.provider.grpc.PrepareTransactionBranchResponse;
import com.kubling.provider.grpc.RecoverTransactionBranchesRequest;
import com.kubling.provider.grpc.RecoverTransactionBranchesResponse;
import com.kubling.provider.grpc.ForgetTransactionBranchRequest;
import com.kubling.provider.grpc.ForgetTransactionBranchResponse;
import com.kubling.provider.grpc.UpdateRequest;
import com.kubling.provider.grpc.UpdateResponse;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.sql.SQLRecoverableException;
import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.util.OptionalLong;
import javax.transaction.xa.XAException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ProviderService extends ProviderServiceGrpc.ProviderServiceImplBase
        implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProviderService.class);
    private static final int RECOVERY_BATCH_SIZE = 256;

    private final ConnectionProbe connectionProbe;
    private final SchemaDiscovery schemaDiscovery;
    private final ConnectionRegistry connections;
    private final QueryTranslator queryTranslator;
    private final JdbcQueryExecutor queryExecutor;
    private final MutationTranslator mutationTranslator;
    private final JdbcMutationExecutor mutationExecutor;
    private final TransactionRecoveryManager transactionRecovery;

    ProviderService(
            ConnectionProbe connectionProbe,
            SchemaDiscovery schemaDiscovery,
            ConnectionFactory connectionFactory,
            SchemaPolicy schemaPolicy) {
        this(
                connectionProbe,
                schemaDiscovery,
                new ConnectionRegistry(connectionFactory),
                schemaPolicy,
                null);
    }

    ProviderService(
            ConnectionProbe connectionProbe,
            SchemaDiscovery schemaDiscovery,
            LogicalConnectionFactory connectionFactory,
            SchemaPolicy schemaPolicy,
            TransactionRecoveryManager transactionRecovery) {
        this(
                connectionProbe,
                schemaDiscovery,
                new ConnectionRegistry(connectionFactory),
                schemaPolicy,
                transactionRecovery);
    }

    private ProviderService(
            ConnectionProbe connectionProbe,
            SchemaDiscovery schemaDiscovery,
            ConnectionRegistry connections,
            SchemaPolicy schemaPolicy,
            TransactionRecoveryManager transactionRecovery) {
        this.connectionProbe = connectionProbe;
        this.schemaDiscovery = schemaDiscovery;
        this.connections = connections;
        this.queryTranslator = new QueryTranslator(schemaPolicy);
        this.queryExecutor = new JdbcQueryExecutor();
        this.mutationTranslator = new MutationTranslator(schemaPolicy);
        this.mutationExecutor = new JdbcMutationExecutor();
        this.transactionRecovery = transactionRecovery;
    }

    @Override
    public void getSchema(
            GetSchemaRequest request,
            StreamObserver<GetSchemaResponse> responseObserver) {
        try {
            responseObserver.onNext(GetSchemaResponse.newBuilder()
                    .setMetadata(schemaDiscovery.discover())
                    .build());
            responseObserver.onCompleted();
        } catch (SQLException exception) {
            LOGGER.warn(
                    "Oracle schema discovery failed ({})",
                    exception.getClass().getSimpleName());
            LOGGER.debug("Oracle schema discovery failure", exception);
            responseObserver.onError(Status.UNAVAILABLE
                    .withDescription("Oracle schema discovery failed")
                    .asRuntimeException());
        } catch (RuntimeException exception) {
            internalFailure(
                    "schema discovery",
                    "Oracle schema discovery failed",
                    exception,
                    responseObserver);
        }
    }

    @Override
    public void getCapabilities(
            GetCapabilitiesRequest request,
            StreamObserver<GetCapabilitiesResponse> responseObserver) {
        responseObserver.onNext(Capabilities.response(
                transactionRecovery == null ? null : transactionRecovery.resourceManagerId()));
        responseObserver.onCompleted();
    }

    @Override
    public void openConnection(
            OpenConnectionRequest request,
            StreamObserver<OpenConnectionResponse> responseObserver) {
        try {
            responseObserver.onNext(OpenConnectionResponse.newBuilder()
                    .setConnectionId(connections.open())
                    .build());
            responseObserver.onCompleted();
        } catch (SQLException exception) {
            sqlFailure(
                    "open logical connection",
                    "Oracle connection could not be opened",
                    exception,
                    responseObserver);
        } catch (RuntimeException exception) {
            internalFailure(
                    "open logical connection",
                    "Oracle connection could not be opened",
                    exception,
                    responseObserver);
        }
    }

    @Override
    public void closeConnection(
            CloseConnectionRequest request,
            StreamObserver<CloseConnectionResponse> responseObserver) {
        if (request.getConnectionId().isBlank()) {
            invalidConnectionId(responseObserver);
            return;
        }
        try {
            if (!connections.close(request.getConnectionId())) {
                missingConnection(responseObserver);
                return;
            }
            responseObserver.onNext(CloseConnectionResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (SQLException exception) {
            sqlFailure(
                    "close logical connection",
                    "Oracle connection could not be closed",
                    exception,
                    responseObserver);
        }
    }

    @Override
    public void beginTransaction(
            BeginTransactionRequest request,
            StreamObserver<BeginTransactionResponse> responseObserver) {
        LogicalConnection connection = connection(
                request.getConnectionId(), responseObserver);
        if (connection == null) {
            return;
        }
        try {
            connection.beginTransaction();
            responseObserver.onNext(BeginTransactionResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (TransactionStateException exception) {
            transactionStateFailure(exception, responseObserver);
        } catch (SQLException exception) {
            sqlFailure(
                    "begin transaction",
                    "Oracle transaction could not be started",
                    exception,
                    responseObserver);
        }
    }

    @Override
    public void commitTransaction(
            CommitTransactionRequest request,
            StreamObserver<CommitTransactionResponse> responseObserver) {
        LogicalConnection connection = connection(
                request.getConnectionId(), responseObserver);
        if (connection == null) {
            return;
        }
        try {
            connection.commitTransaction();
            responseObserver.onNext(CommitTransactionResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (TransactionStateException exception) {
            transactionStateFailure(exception, responseObserver);
        } catch (SQLException exception) {
            sqlFailure(
                    "commit transaction",
                    "Oracle transaction could not be committed",
                    exception,
                    responseObserver);
        }
    }

    @Override
    public void rollbackTransaction(
            RollbackTransactionRequest request,
            StreamObserver<RollbackTransactionResponse> responseObserver) {
        LogicalConnection connection = connection(
                request.getConnectionId(), responseObserver);
        if (connection == null) {
            return;
        }
        try {
            connection.rollbackTransaction();
            responseObserver.onNext(RollbackTransactionResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (TransactionStateException exception) {
            transactionStateFailure(exception, responseObserver);
        } catch (SQLException exception) {
            sqlFailure(
                    "rollback transaction",
                    "Oracle transaction could not be rolled back",
                    exception,
                    responseObserver);
        }
    }

    @Override
    public void isInTransaction(
            IsInTransactionRequest request,
            StreamObserver<IsInTransactionResponse> responseObserver) {
        LogicalConnection connection = connection(
                request.getConnectionId(), responseObserver);
        if (connection == null) {
            return;
        }
        try {
            responseObserver.onNext(IsInTransactionResponse.newBuilder()
                    .setActive(connection.isInTransaction())
                    .build());
            responseObserver.onCompleted();
        } catch (SQLException exception) {
            sqlFailure(
                    "read transaction state",
                    "Oracle transaction state could not be read",
                    exception,
                    responseObserver);
        }
    }

    @Override
    public void beginTransactionBranch(
            BeginTransactionBranchRequest request,
            StreamObserver<BeginTransactionBranchResponse> responseObserver) {
        TransactionRecoveryManager manager = recovery(responseObserver);
        if (manager == null) {
            return;
        }
        final BranchIdentity identity;
        try {
            identity = BranchIdentity.from(
                    request.hasBranch() ? request.getBranch() : null,
                    manager.resourceManagerId());
        } catch (RequestValidationException exception) {
            invalidRequest(exception, responseObserver);
            return;
        }
        LogicalConnection connection = connection(
                request.getConnectionId(), responseObserver);
        if (connection == null) {
            return;
        }
        try {
            connection.beginBranch(identity.toProto());
            responseObserver.onNext(BeginTransactionBranchResponse.newBuilder()
                    .setState(com.kubling.provider.grpc.TransactionBranchState
                            .TRANSACTION_BRANCH_STATE_ACTIVE)
                    .build());
            responseObserver.onCompleted();
        } catch (RequestValidationException exception) {
            invalidRequest(exception, responseObserver);
        } catch (TransactionStateException exception) {
            transactionStateFailure(exception, responseObserver);
        } catch (SQLException exception) {
            sqlFailure(
                    "begin transaction branch",
                    "Oracle transaction branch could not be started",
                    exception,
                    responseObserver);
        } catch (XAException exception) {
            xaFailure("begin transaction branch", exception, responseObserver);
        }
    }

    @Override
    public void prepareTransactionBranch(
            PrepareTransactionBranchRequest request,
            StreamObserver<PrepareTransactionBranchResponse> responseObserver) {
        TransactionRecoveryManager manager = recovery(responseObserver);
        if (manager == null) {
            return;
        }
        final BranchIdentity identity;
        try {
            identity = BranchIdentity.from(
                    request.hasBranch() ? request.getBranch() : null,
                    manager.resourceManagerId());
        } catch (RequestValidationException exception) {
            invalidRequest(exception, responseObserver);
            return;
        }
        LogicalConnection connection = connection(
                request.getConnectionId(), responseObserver);
        if (connection == null) {
            return;
        }
        try {
            responseObserver.onNext(PrepareTransactionBranchResponse.newBuilder()
                    .setOutcome(connection.prepareBranch(identity.toProto()))
                    .build());
            responseObserver.onCompleted();
        } catch (RequestValidationException exception) {
            invalidRequest(exception, responseObserver);
        } catch (TransactionStateException exception) {
            transactionStateFailure(exception, responseObserver);
        } catch (SQLException exception) {
            sqlFailure(
                    "prepare transaction branch",
                    "Oracle transaction branch could not be prepared",
                    exception,
                    responseObserver);
        } catch (XAException exception) {
            xaFailure("prepare transaction branch", exception, responseObserver);
        }
    }

    @Override
    public void commitTransactionBranch(
            CommitTransactionBranchRequest request,
            StreamObserver<CommitTransactionBranchResponse> responseObserver) {
        TransactionRecoveryManager manager = recovery(responseObserver);
        if (manager == null) {
            return;
        }
        try {
            BranchIdentity identity = BranchIdentity.from(
                    request.hasBranch() ? request.getBranch() : null,
                    manager.resourceManagerId());
            com.kubling.provider.grpc.TransactionBranchState state;
            if (request.getOnePhase()) {
                if (!request.hasConnectionId() || request.getConnectionId().isBlank()) {
                    invalidArgument(
                            "connection_id is required for a one-phase commit",
                            responseObserver);
                    return;
                }
                LogicalConnection connection = connection(
                        request.getConnectionId(), responseObserver);
                if (connection == null) {
                    return;
                }
                state = connection.commitBranch(identity.toProto());
            } else {
                if (request.hasConnectionId()) {
                    invalidArgument(
                            "connection_id must be absent when committing a prepared branch",
                            responseObserver);
                    return;
                }
                state = manager.commit(identity);
            }
            responseObserver.onNext(CommitTransactionBranchResponse.newBuilder()
                    .setState(state)
                    .build());
            responseObserver.onCompleted();
        } catch (RequestValidationException exception) {
            invalidRequest(exception, responseObserver);
        } catch (TransactionStateException exception) {
            transactionStateFailure(exception, responseObserver);
        } catch (SQLException exception) {
            sqlFailure(
                    "commit transaction branch",
                    "Oracle transaction branch could not be committed",
                    exception,
                    responseObserver);
        } catch (XAException exception) {
            xaFailure("commit transaction branch", exception, responseObserver);
        }
    }

    @Override
    public void rollbackTransactionBranch(
            RollbackTransactionBranchRequest request,
            StreamObserver<RollbackTransactionBranchResponse> responseObserver) {
        TransactionRecoveryManager manager = recovery(responseObserver);
        if (manager == null) {
            return;
        }
        try {
            BranchIdentity identity = BranchIdentity.from(
                    request.hasBranch() ? request.getBranch() : null,
                    manager.resourceManagerId());
            com.kubling.provider.grpc.TransactionBranchState state;
            if (request.hasConnectionId()) {
                if (request.getConnectionId().isBlank()) {
                    invalidArgument(
                            "connection_id must not be empty when rolling back an active branch",
                            responseObserver);
                    return;
                }
                LogicalConnection connection = connection(
                        request.getConnectionId(), responseObserver);
                if (connection == null) {
                    return;
                }
                state = connection.rollbackBranch(identity.toProto());
            } else {
                state = manager.rollback(identity);
            }
            responseObserver.onNext(RollbackTransactionBranchResponse.newBuilder()
                    .setState(state)
                    .build());
            responseObserver.onCompleted();
        } catch (RequestValidationException exception) {
            invalidRequest(exception, responseObserver);
        } catch (TransactionStateException exception) {
            transactionStateFailure(exception, responseObserver);
        } catch (SQLException exception) {
            sqlFailure(
                    "roll back transaction branch",
                    "Oracle transaction branch could not be rolled back",
                    exception,
                    responseObserver);
        } catch (XAException exception) {
            xaFailure("roll back transaction branch", exception, responseObserver);
        }
    }

    @Override
    public void recoverTransactionBranches(
            RecoverTransactionBranchesRequest request,
            StreamObserver<RecoverTransactionBranchesResponse> responseObserver) {
        TransactionRecoveryManager manager = recovery(responseObserver);
        if (manager == null) {
            return;
        }
        try {
            BranchIdentity.validateResourceManagerId(request.getResourceManagerId());
        } catch (RequestValidationException exception) {
            invalidRequest(exception, responseObserver);
            return;
        }
        if (!request.getResourceManagerId().equals(manager.resourceManagerId())) {
            responseObserver.onError(Status.FAILED_PRECONDITION
                    .withDescription("resource_manager_id does not match this Oracle provider")
                    .asRuntimeException());
            return;
        }
        try {
            var branches = manager.recover();
            for (int start = 0; start < branches.size(); start += RECOVERY_BATCH_SIZE) {
                int end = Math.min(start + RECOVERY_BATCH_SIZE, branches.size());
                responseObserver.onNext(RecoverTransactionBranchesResponse.newBuilder()
                        .addAllBranches(branches.subList(start, end))
                        .build());
            }
            responseObserver.onCompleted();
        } catch (SQLException exception) {
            sqlFailure(
                    "recover transaction branches",
                    "Oracle transaction branches could not be recovered",
                    exception,
                    responseObserver);
        }
    }

    @Override
    public void getTransactionBranchStatus(
            GetTransactionBranchStatusRequest request,
            StreamObserver<GetTransactionBranchStatusResponse> responseObserver) {
        TransactionRecoveryManager manager = recovery(responseObserver);
        if (manager == null) {
            return;
        }
        try {
            BranchIdentity identity = BranchIdentity.from(
                    request.hasBranch() ? request.getBranch() : null,
                    manager.resourceManagerId());
            responseObserver.onNext(GetTransactionBranchStatusResponse.newBuilder()
                    .setState(manager.status(identity))
                    .build());
            responseObserver.onCompleted();
        } catch (RequestValidationException exception) {
            invalidRequest(exception, responseObserver);
        } catch (SQLException exception) {
            sqlFailure(
                    "read transaction branch status",
                    "Oracle transaction branch status could not be read",
                    exception,
                    responseObserver);
        }
    }

    @Override
    public void forgetTransactionBranch(
            ForgetTransactionBranchRequest request,
            StreamObserver<ForgetTransactionBranchResponse> responseObserver) {
        TransactionRecoveryManager manager = recovery(responseObserver);
        if (manager == null) {
            return;
        }
        try {
            BranchIdentity identity = BranchIdentity.from(
                    request.hasBranch() ? request.getBranch() : null,
                    manager.resourceManagerId());
            manager.forget(identity);
            responseObserver.onNext(ForgetTransactionBranchResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (RequestValidationException exception) {
            invalidRequest(exception, responseObserver);
        } catch (TransactionStateException exception) {
            transactionStateFailure(exception, responseObserver);
        } catch (SQLException exception) {
            sqlFailure(
                    "forget transaction branch",
                    "Oracle transaction branch could not be forgotten",
                    exception,
                    responseObserver);
        } catch (XAException exception) {
            xaFailure("forget transaction branch", exception, responseObserver);
        }
    }

    @Override
    public void query(QueryRequest request, StreamObserver<QueryResponse> responseObserver) {
        LogicalConnection connection = connection(
                request.getConnectionId(), responseObserver);
        if (connection == null) {
            return;
        }
        try {
            QueryPlan plan = queryTranslator.translate(request);
            connection.query(queryExecutor, request, plan, responseObserver);
            responseObserver.onCompleted();
        } catch (RequestValidationException exception) {
            invalidRequest(exception, responseObserver);
        } catch (StatusRuntimeException exception) {
            responseObserver.onError(exception);
        } catch (SQLException exception) {
            sqlFailure(
                    "execute query",
                    "Oracle query execution failed",
                    exception,
                    responseObserver);
        } catch (RuntimeException exception) {
            internalFailure(
                    "query",
                    "Oracle query execution failed",
                    exception,
                    responseObserver);
        }
    }

    @Override
    public void insert(InsertRequest request, StreamObserver<InsertResponse> responseObserver) {
        LogicalConnection connection = connection(
                request.getConnectionId(), responseObserver);
        if (connection == null) {
            return;
        }
        try {
            OptionalLong affectedRows = connection.insert(
                    mutationExecutor, mutationTranslator.translate(request));
            InsertResponse.Builder response = InsertResponse.newBuilder();
            affectedRows.ifPresent(response::setAffectedRows);
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        } catch (RequestValidationException exception) {
            invalidRequest(exception, responseObserver);
        } catch (StatusRuntimeException exception) {
            responseObserver.onError(exception);
        } catch (SQLException exception) {
            sqlFailure(
                    "execute insert",
                    "Oracle insert execution failed",
                    exception,
                    responseObserver);
        } catch (RuntimeException exception) {
            internalFailure(
                    "insert",
                    "Oracle insert execution failed",
                    exception,
                    responseObserver);
        }
    }

    @Override
    public void update(UpdateRequest request, StreamObserver<UpdateResponse> responseObserver) {
        LogicalConnection connection = connection(
                request.getConnectionId(), responseObserver);
        if (connection == null) {
            return;
        }
        try {
            long affectedRows = connection.mutate(
                    mutationExecutor, mutationTranslator.translate(request));
            responseObserver.onNext(UpdateResponse.newBuilder()
                    .setAffectedRows(affectedRows)
                    .build());
            responseObserver.onCompleted();
        } catch (RequestValidationException exception) {
            invalidRequest(exception, responseObserver);
        } catch (StatusRuntimeException exception) {
            responseObserver.onError(exception);
        } catch (SQLException exception) {
            sqlFailure(
                    "execute update",
                    "Oracle update execution failed",
                    exception,
                    responseObserver);
        } catch (RuntimeException exception) {
            internalFailure(
                    "update",
                    "Oracle update execution failed",
                    exception,
                    responseObserver);
        }
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        LogicalConnection connection = connection(
                request.getConnectionId(), responseObserver);
        if (connection == null) {
            return;
        }
        try {
            long affectedRows = connection.mutate(
                    mutationExecutor, mutationTranslator.translate(request));
            responseObserver.onNext(DeleteResponse.newBuilder()
                    .setAffectedRows(affectedRows)
                    .build());
            responseObserver.onCompleted();
        } catch (RequestValidationException exception) {
            invalidRequest(exception, responseObserver);
        } catch (StatusRuntimeException exception) {
            responseObserver.onError(exception);
        } catch (SQLException exception) {
            sqlFailure(
                    "execute delete",
                    "Oracle delete execution failed",
                    exception,
                    responseObserver);
        } catch (RuntimeException exception) {
            internalFailure(
                    "delete",
                    "Oracle delete execution failed",
                    exception,
                    responseObserver);
        }
    }

    @Override
    public void health(HealthRequest request, StreamObserver<HealthResponse> responseObserver) {
        boolean healthy = false;
        try {
            healthy = connectionProbe.isHealthy();
        } catch (Exception exception) {
            LOGGER.warn(
                    "Oracle connectivity health check failed ({})",
                    exception.getClass().getSimpleName());
        }

        String message = healthy
                ? "Oracle database is reachable"
                : "Oracle database is unavailable";
        responseObserver.onNext(HealthResponse.newBuilder()
                .setHealthy(healthy)
                .setMessage(message)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void close() {
        try {
            connections.close();
        } catch (SQLException exception) {
            reportSqlFailure("close provider connections", exception);
        }
        if (transactionRecovery != null) {
            try {
                transactionRecovery.close();
            } catch (Exception exception) {
                LOGGER.warn(
                        "Oracle resource manager lease could not be released cleanly ({})",
                        exception.getClass().getSimpleName());
            }
        }
    }

    private LogicalConnection connection(
            String connectionId, StreamObserver<?> responseObserver) {
        if (connectionId.isBlank()) {
            invalidConnectionId(responseObserver);
            return null;
        }
        LogicalConnection connection = connections.get(connectionId);
        if (connection == null) {
            missingConnection(responseObserver);
            return null;
        }
        return connection;
    }

    private TransactionRecoveryManager recovery(StreamObserver<?> responseObserver) {
        if (transactionRecovery == null) {
            responseObserver.onError(Status.UNIMPLEMENTED
                    .withDescription("durable Oracle transaction recovery is not configured")
                    .asRuntimeException());
            return null;
        }
        return transactionRecovery;
    }

    private static void invalidConnectionId(StreamObserver<?> responseObserver) {
        invalidArgument("connection_id is required", responseObserver);
    }

    private static void invalidArgument(String description, StreamObserver<?> responseObserver) {
        responseObserver.onError(
                Status.INVALID_ARGUMENT.withDescription(description).asRuntimeException());
    }

    private static void missingConnection(StreamObserver<?> responseObserver) {
        responseObserver.onError(Status.NOT_FOUND
                .withDescription("Oracle logical connection was not found")
                .asRuntimeException());
    }

    private static void invalidRequest(
            RequestValidationException exception,
            StreamObserver<?> responseObserver) {
        responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription(exception.getMessage())
                .asRuntimeException());
    }

    private static void transactionStateFailure(
            TransactionStateException exception,
            StreamObserver<?> responseObserver) {
        responseObserver.onError(Status.FAILED_PRECONDITION
                .withDescription(exception.getMessage())
                .asRuntimeException());
    }

    private static void sqlFailure(
            String operation,
            String description,
            SQLException exception,
            StreamObserver<?> responseObserver) {
        reportSqlFailure(operation, exception);
        Status status = isConnectivityFailure(exception)
                ? Status.UNAVAILABLE
                : Status.FAILED_PRECONDITION;
        responseObserver.onError(status
                .withDescription(description)
                .asRuntimeException());
    }

    private static void internalFailure(
            String operation,
            String description,
            RuntimeException exception,
            StreamObserver<?> responseObserver) {
        LOGGER.error(
                "Oracle {} encountered an internal failure ({})",
                operation,
                exception.getClass().getSimpleName());
        responseObserver.onError(Status.INTERNAL
                .withDescription(description)
                .asRuntimeException());
    }

    private static void xaFailure(
            String operation, XAException exception, StreamObserver<?> responseObserver) {
        LOGGER.warn("Oracle {} failed (XA code {})", operation, exception.errorCode);
        Status status = switch (exception.errorCode) {
            case XAException.XAER_RMERR,
                    XAException.XAER_RMFAIL,
                    XAException.XA_RETRY -> Status.UNAVAILABLE;
            case XAException.XAER_INVAL,
                    XAException.XAER_PROTO,
                    XAException.XAER_NOTA,
                    XAException.XA_RBROLLBACK,
                    XAException.XA_RBCOMMFAIL,
                    XAException.XA_RBDEADLOCK,
                    XAException.XA_RBINTEGRITY,
                    XAException.XA_RBOTHER,
                    XAException.XA_RBPROTO,
                    XAException.XA_RBTIMEOUT,
                    XAException.XA_RBTRANSIENT -> Status.FAILED_PRECONDITION;
            default -> Status.INTERNAL;
        };
        responseObserver.onError(status
                .withDescription("Oracle XA operation failed")
                .asRuntimeException());
    }

    private static boolean isConnectivityFailure(SQLException exception) {
        return exception instanceof SQLRecoverableException
                || exception instanceof SQLTransientException
                || exception.getSQLState() != null && exception.getSQLState().startsWith("08");
    }

    private static void reportSqlFailure(String operation, SQLException exception) {
        LOGGER.warn(
                "Oracle {} failed (SQL state {}, vendor code {}, {})",
                operation,
                exception.getSQLState(),
                exception.getErrorCode(),
                exception.getClass().getSimpleName());
    }
}
