package org.ptm.kvservice.transpost;

import org.ptm.kvservice.db.FastDB;
import org.ptm.kvservice.proto.ReplicationServiceGrpc;
import org.ptm.kvservice.proto.Updates;
import com.google.common.util.concurrent.AbstractService;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.ptm.kvservice.proto.SequenceNumber;
import org.rocksdb.RocksDBException;
import org.rocksdb.TransactionLogIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

@SuppressWarnings("ALL")
public class MasterService extends AbstractService {

    private static final Logger LOGGER = LoggerFactory.getLogger(MasterService.class);

    private static class ReplicationService extends ReplicationServiceGrpc.ReplicationServiceImplBase {

        private final FastDB fastDB;

        ReplicationService(FastDB fastDB) {
            this.fastDB = fastDB;
        }

        @Override
        public void sync(SequenceNumber request,
                         io.grpc.stub.StreamObserver<Updates> responseObserver) {
            long latestSequenceNumberFromReplica = request.getSequenceNumber();
            LOGGER.warn("getLatestUpdates, sqNumber {}", latestSequenceNumberFromReplica);
            updateHandle(latestSequenceNumberFromReplica, responseObserver);
        }

        public void getUpdatesSince(SequenceNumber request,
                                    io.grpc.stub.StreamObserver<Updates> responseObserver) {
            long latestSequenceNumber = request.getSequenceNumber();
            LOGGER.warn("getUpdatesSince, sqNumber {}", latestSequenceNumber);
            updateHandle(latestSequenceNumber, responseObserver);
        }

        /**
         */
        public void getLatestUpdates(SequenceNumber request,
                                     io.grpc.stub.StreamObserver<Updates> responseObserver) {
            long latestSequenceNumber = this.fastDB.getLatestSequenceNumber();
            LOGGER.warn("getLatestUpdates, sqNumber {}", latestSequenceNumber);
            updateHandle(latestSequenceNumber, responseObserver);
        }

        private void updateHandle(long sequenceNumber,
                                  io.grpc.stub.StreamObserver<Updates> responseObserver) {
            try {
                Iterator<TransactionLogIterator.BatchResult> batchResultIterator =
                        fastDB.getUpdatesSince(sequenceNumber);
                int iteratorCount = 0;
                while (batchResultIterator.hasNext()) {
                    iteratorCount++;
                    TransactionLogIterator.BatchResult batchResult = batchResultIterator.next();
                    Updates.Builder updateBuilder = Updates.newBuilder();
                    updateBuilder.setSequenceNumber(batchResult.sequenceNumber());
                    updateBuilder.setUpdates(ByteString.copyFrom(batchResult.writeBatch().data()));
                    responseObserver.onNext(updateBuilder.build());
                }
                LOGGER.warn("Number of iterator count {}", iteratorCount);
                responseObserver.onCompleted();
            } catch (RocksDBException e) {
                LOGGER.error("Error when hander update ", e);
                LOGGER.error("Current master seq number {}", fastDB.getLatestSequenceNumber());
                LOGGER.error("Sequence number of reqquest {}", sequenceNumber);
                responseObserver.onError(e.getCause());
            }
        }

    }

    private final ReplicationService replicationService;
    private final int port;
    private final Server server;
    private final AtomicBoolean isRunning;

    public MasterService(FastDB fastDB, int port) {
        this.replicationService = new ReplicationService(fastDB);
        this.port = port;
        ServerBuilder<?> serverBuilder = ServerBuilder.forPort(this.port);
        serverBuilder.addService(this.replicationService);
        this.server = serverBuilder.build();
        this.isRunning = new AtomicBoolean(false);
    }

    @Override
    protected void doStart() {
        try {
            LOGGER.info("Start master, at port {}", port);
            this.server.start();
            LOGGER.info("Start grpc server at port {} ", port);
            this.isRunning.set(true);
            notifyStarted();
        } catch (IOException e) {
            LOGGER.error("Error when start Grpc server at port {} ", port, e);
            notifyFailed(e);
        }
    }

    @Override
    protected void doStop() {
        this.server.shutdownNow();
        this.isRunning.set(false);
        LOGGER.info("Grpc server is stoped");
    }

}

