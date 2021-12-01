package org.ptm.kvservice.transpost;

import org.ptm.kvservice.db.FastDB;
import org.ptm.kvservice.proto.ReplicationServiceGrpc;
import org.ptm.kvservice.proto.SequenceNumber;
import org.ptm.kvservice.proto.Updates;
import com.google.common.util.concurrent.AbstractService;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings("ALL")
public class SlaveService extends AbstractService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SlaveService.class);
    private static SlaveService INSTANCE;

    private ExecutorService executorService;
    private final ManagedChannel channel;
    private final FastDB fastDB;
    private final AtomicBoolean isRunning;
    private final AtomicLong currentSequenceNumber;
    private final MappedByteBuffer mappedByteBuffer;

    private SlaveService(FastDB fastDB, String name, int grpcPort, RandomAccessFile
                         sequenceNumberRAFile) throws IOException {
        this.fastDB = fastDB;
        this.channel = ManagedChannelBuilder.forAddress(name, grpcPort).usePlaintext().build();
        this.isRunning = new AtomicBoolean(false);
        final int BUFFER_SIZE = 1024;
        this.mappedByteBuffer = sequenceNumberRAFile.getChannel()
                .map(FileChannel.MapMode.READ_WRITE, 0, BUFFER_SIZE);
        this.currentSequenceNumber = new AtomicLong(this.mappedByteBuffer.getLong(0));
        System.out.println();
    }

    private void updateCurrentSequenceNumber(long sequenceNumber) {
        this.mappedByteBuffer.putLong(0, sequenceNumber);
        mappedByteBuffer.force();
    }

    private void runSync() {
        ReplicationServiceGrpc.ReplicationServiceBlockingStub stub = ReplicationServiceGrpc.newBlockingStub(channel);

//        StreamObserver<Updates> updatesStreamObserver = new StreamObserver<Updates>() {
//            @Override
//            public void onNext(Updates updates) {
//                try {
//                    long sequenceNumber = updates.getSequenceNumber();
//                    currentSequenceNumber.set(sequenceNumber);
//                    updateCurrentSequenceNumber(sequenceNumber);
//                    listDB.writeUpdate(updates.getUpdates().toByteArray());
//                } catch (Exception e) {
//                    e.printStackTrace();
//                    onError(e);
//                }
//            }
//
//            @Override
//            public void onError(Throwable throwable) {
//                throwable.printStackTrace();
//                stopAsync();
//            }
//
//            @Override
//            public void onCompleted() {
//                LOGGER.info("Compete syn");
//            }
//        };
        try {
            while (isRunning.get()) {
                Iterator<Updates> updatesIterator =
                        stub.sync(SequenceNumber.newBuilder().setSequenceNumber(currentSequenceNumber.get()).build());
                while (updatesIterator.hasNext()) {
                    Updates updates = updatesIterator.next();
                    long sequenceNumber = updates.getSequenceNumber();
                    updateCurrentSequenceNumber(sequenceNumber);
                    fastDB.writeUpdate(updates.getUpdates().toByteArray());
                }
                TimeUnit.MILLISECONDS.sleep(10);
            }
        } catch (Exception e) {
            LOGGER.error("Error wuen sync with master", e);
            //stopAsync();
        }
    }

    public static void createInstance(FastDB fastDB, String name, int grpcPort,
                                      RandomAccessFile randomAccessFile) throws IOException {
        if (INSTANCE == null) {
            synchronized (SlaveService.class) {
                INSTANCE = new SlaveService(fastDB, name, grpcPort, randomAccessFile);
            }
            return;
        }
        throw new IllegalArgumentException("Slave service is created");
    }

    public static SlaveService getInstance() {
        if (INSTANCE == null) {
            throw new RuntimeException("Slave service is not created yet");
        }
        return INSTANCE;
    }

    @Override
    protected void doStart() {
        LOGGER.info("Start slave node");
        this.executorService = Executors.newSingleThreadExecutor();
        this.isRunning.set(true);
        this.executorService.submit(this::runSync);
        notifyStarted();
    }

    @Override
    protected void doStop() {
        this.isRunning.set(false);
        this.executorService.shutdownNow();
        notifyStopped();
    }
}
