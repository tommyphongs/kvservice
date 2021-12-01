package org.ptm.kvservice.db;

import org.ptm.kvservice.db.models.KeyValuesObj;
import org.ptm.kvservice.proto.FastDbModel;
import org.ptm.kvservice.utils.Utils;
import com.google.common.util.concurrent.Striped;
import com.google.gson.JsonPrimitive;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shaded.org.apache.commons.lang3.ArrayUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

@SuppressWarnings("ALL")
public class FastDBIml implements FastDB {

    private final Logger LOGGER = LoggerFactory.getLogger(FastDBIml.class);

    private OptimisticTransactionDB opRocksDB;

    public static final byte[] PREFIX_HEAD_KEY_BYTES = "_list".getBytes();

    private static final int MAX_SLICE_SIZE_DEFAULT = 100;
    private static final int MAX_ELEMENTS_DEFAULT = 2000;
    private static final int GUAVA_STRIPES_DEFAULT = 100_000;
    private static final int GUAVA_CACHE_MAX_ENTRY = 2500000;
    private static final int GUAVA_CACHE_MAX_LENGTH = 500;

    private final int maxSliceSize;
    private final int maxElements;
    private final Striped<Lock> striped;
    private final Options options;
    private final org.rocksdb.Options rocksdbOptions;
    private final Statistics statistics;
    private Map<String, FastDbModel.SliceMetadata> metadataMap;

    public static class Options {
        private String dirName;
        private Integer maxSliceSize;
        private Integer maxElements;
        private Integer guavaStripes;
        private boolean allowMMapReads = false;
        private boolean isAllowMMapWrites = false;

        public Options setDirName(String dirName) {
            this.dirName = dirName;
            return this;
        }

        public Options setMaxSliceSize(Integer maxSliceSize) {
            this.maxSliceSize = maxSliceSize;
            return this;
        }

        public Options setMaxElements(Integer maxElements) {
            this.maxElements = maxElements;
            return this;
        }

        public Options setGuavaStripes(Integer guavaStripes) {
            this.guavaStripes = guavaStripes;
            return this;
        }

        public Options setMMapReads(boolean allowMMapReads) {
            this.allowMMapReads = allowMMapReads;
            return this;
        }

        public Options setMMapWrites(boolean allowMMapWrites) {
            this.isAllowMMapWrites = allowMMapWrites;
            return this;
        }

        public String getDirName() {
            return dirName;
        }

    }

    FastDBIml(Options options) throws RocksDBException, InvalidProtocolBufferException {
        this.options = options;
        Objects.requireNonNull(options.dirName, "Data dir is not be null");
        RocksDB.loadLibrary();
        this.statistics = new Statistics();
        this.rocksdbOptions = new org.rocksdb.Options()
                .setAllowMmapReads(options.allowMMapReads)
                .setAllowMmapWrites(options.isAllowMMapWrites);
        org.rocksdb.Logger logger = new org.rocksdb.Logger(this.rocksdbOptions) {
            @Override
            protected void log(InfoLogLevel infoLogLevel, String s) {
                switch (infoLogLevel) {
                    case INFO_LEVEL:
                        LOGGER.info(s);
                        break;
                    case WARN_LEVEL:
                        LOGGER.info(s);
                    case FATAL_LEVEL:
                    case ERROR_LEVEL:
                        LOGGER.error(s);
                        break;
                    case DEBUG_LEVEL:
                        LOGGER.debug(s);
                        break;
                }
            }
        };
        this.rocksdbOptions.setLogger(logger).setStatistics(statistics);
        String dirName = options.dirName;
        this.maxSliceSize = (options.maxSliceSize != null && options.maxSliceSize > 0) ?
                options.maxSliceSize : MAX_SLICE_SIZE_DEFAULT;
        this.maxElements = (options.maxElements != null && options.maxElements > 0) ?
                options.maxElements : MAX_ELEMENTS_DEFAULT;
        RocksDB.open(dirName).close();
        this.opRocksDB = OptimisticTransactionDB.open(rocksdbOptions, dirName);
        this.striped = (options.guavaStripes != null && options.guavaStripes > 0) ?
                Striped.lock(options.guavaStripes) : Striped.lock(GUAVA_STRIPES_DEFAULT);
        long s = System.currentTimeMillis();
        this.metadataMap = getAllMetadata();
        LOGGER.info("Time to get all metadata: {}", System.currentTimeMillis() - s);

        LOGGER.info("Data dir: {}", dirName);
        LOGGER.info("Max chunk size: {}", this.maxSliceSize);
        LOGGER.info("Max elements: {}", this.maxElements);
    }

    public void reopen() throws RocksDBException {
        RocksDB.loadLibrary();
        RocksDB.open(options.dirName).close();
        this.opRocksDB = OptimisticTransactionDB.open(this.rocksdbOptions, options.dirName);
    }

    public Statistics getStatistics() {
        return statistics;
    }

    public OptimisticTransactionDB getOpRocksDB() {
        return opRocksDB;
    }

    public FastDbModel.SliceMetadata getSliceMetadataFromRocksDb(String key) throws RocksDBException, InvalidProtocolBufferException {
        byte[] keyByteArray = key.getBytes(StandardCharsets.UTF_8);
        byte[] headKeyBytes = ArrayUtils.addAll(PREFIX_HEAD_KEY_BYTES, keyByteArray);
        byte[] headBytes = opRocksDB.get(headKeyBytes);
        if (headBytes == null) {
            return null;
        }
        FastDbModel.SliceMetadata metadata = FastDbModel.SliceMetadata.parseFrom(headBytes);
        return metadata;
    }

    public Map<String, FastDbModel.SliceMetadata> getAllMetadata() throws InvalidProtocolBufferException {
        Map<String, FastDbModel.SliceMetadata> metadataMap = new ConcurrentHashMap<>();
        RocksIterator iterator = opRocksDB.newIterator();
        ReadOptions readOptions = new ReadOptions();
        readOptions.setPrefixSameAsStart(true);
        opRocksDB.newIterator(readOptions);
        for (iterator.seek(FastDBIml.PREFIX_HEAD_KEY_BYTES); iterator.isValid()
                && new String(iterator.key()).startsWith(new String(FastDBIml.PREFIX_HEAD_KEY_BYTES)); iterator.next()) {
            metadataMap.put(new String(ArrayUtils.subarray(iterator.key(), PREFIX_HEAD_KEY_BYTES.length, iterator.key().length)),
                    FastDbModel.SliceMetadata.parseFrom(iterator.value()));
        }
        return metadataMap;
    }

    public Map<String, List<JsonPrimitive>> listHeadPeek(List<String> keys, int size) throws RocksDBException {
        if (size < 0) {
            throw new IllegalArgumentException("Size must larger or equal zero");
        }
        if (size == 0) {
            return Map.of();
        }
        Iterable<Lock> locks = striped.bulkGet(keys);
        locks.forEach(lock -> lock.lock());
        try {
            List<byte[]> bytes = new ArrayList<>();
            List<String> availableKeys = new ArrayList<>();
            keys.forEach(k -> {
                if (metadataMap.containsKey(k)) {
                    availableKeys.add(k);
                }
            });
            Collections.sort(availableKeys);
            List<byte[]> sliceKeys = new ArrayList<>();
            LOGGER.info("A key size {}", availableKeys.size());
            for (String key : availableKeys) {
                byte[] keyByteArray = key.getBytes(StandardCharsets.UTF_8);
                FastDbModel.SliceMetadata metadata = this.metadataMap.get(key);
                int startIndex = 0;
                if (metadata.getSize() > this.maxElements) {
                    startIndex = metadata.getSize() - maxElements;
                }
                int endIndex = metadata.getSize() - startIndex > size ? startIndex + size : metadata.getSize();

                List<JsonPrimitive> values = new ArrayList<>();
                for (int i = startIndex / maxSliceSize; i < metadata.getSliceIndiesCount(); i++) {
                    if (values.size() == size) {
                        break;
                    }

                    long index = metadata.getSliceIndies(i);
                    byte[] sliceKey = ArrayUtils.addAll(keyByteArray, ByteBuffer.allocate(8).putLong(index).array());
                    sliceKeys.add(sliceKey);
                }
            }
            long s = System.currentTimeMillis();
            RocksIterator iterator = this.opRocksDB.newIterator();
            for (byte[] by : sliceKeys) {
                iterator.seek(by);
            }
            LOGGER.info("Time to multiget {}", System.currentTimeMillis() - s);
        } finally {
            locks.forEach(lock -> lock.unlock());
        }
        return null;
    }

    @Override
    public List<JsonPrimitive> listHeadPeek(String key, int size) throws Exception {
        if (size < 0) {
            throw new IllegalArgumentException("Size must larger or equal zero");
        }
        if (size == 0) {
            return Collections.emptyList();
        }
        try {
            striped.get(key).lock();
            byte[] keyByteArray = key.getBytes(StandardCharsets.UTF_8);

            if (!metadataMap.containsKey(key)) {
                return null;
            }
            FastDbModel.SliceMetadata metadata = metadataMap.get(key);
            int startIndex = 0;
            if (metadata.getSize() > this.maxElements) {
                startIndex = metadata.getSize() - maxElements;
            }
            int endIndex = metadata.getSize() - startIndex > size ? startIndex + size : metadata.getSize();

            List<JsonPrimitive> values = new ArrayList<>();
            for (int i = startIndex / maxSliceSize; i < metadata.getSliceIndiesCount(); i++) {
                if (values.size() == size) {
                    break;
                }
                long index = metadata.getSliceIndies(i);
                byte[] sliceKey = ArrayUtils.addAll(keyByteArray, ByteBuffer.allocate(8).putLong(index).array());
                byte[] nodeBytes = opRocksDB.get(sliceKey);
                FastDbModel.Slice slice = FastDbModel.Slice.parseFrom(nodeBytes);
                List<JsonPrimitive> primitives = DataTypes.toListV(slice.getData(), metadata.getType());
                int startIndexAtSlice = i == startIndex / maxSliceSize ? startIndex % maxSliceSize :
                        0;
                int endIndexAtSlice = size <= primitives.size() - startIndexAtSlice ?
                        startIndexAtSlice + size : primitives.size();
                values.addAll(primitives.subList(startIndexAtSlice, endIndexAtSlice));
            }
            return values;

        } finally {
            striped.get(key).unlock();
        }
    }

    @Override
    public List<JsonPrimitive> listTailPeek(String key, int size) throws Exception {
        if (size < 0) {
            throw new IllegalArgumentException("Size must larger or equal zero");
        }
        if (size == 0) {
            return Collections.emptyList();
        }
        try {
            striped.get(key).lock();
            byte[] keyByteArray = key.getBytes(StandardCharsets.UTF_8);
            if (!metadataMap.containsKey(key)) {
                return null;
            }
            FastDbModel.SliceMetadata metadata = metadataMap.get(key);
            int startIndex = 0;
            if (metadata.getSize() > this.maxElements) {
                startIndex = metadata.getSize() - maxElements;
            }
            int endIndex = metadata.getSize() - startIndex > size ? startIndex + size : metadata.getSize();

            List<JsonPrimitive> values = new ArrayList<>();
            for (int i = metadata.getSliceIndiesCount() - 1; i >= startIndex / maxSliceSize; i--) {
                if (values.size() == size) {
                    break;
                }
                long index = metadata.getSliceIndies(i);
                byte[] sliceKey = ArrayUtils.addAll(keyByteArray, ByteBuffer.allocate(8).putLong(index).array());
                byte[] nodeBytes = opRocksDB.get(sliceKey);
                FastDbModel.Slice slice = FastDbModel.Slice.parseFrom(nodeBytes);
                List<JsonPrimitive> primitives = DataTypes.toListV(slice.getData(), metadata.getType());
                int remainSize = size - values.size();
                int startIndexAvaibleAtSlice = i == startIndex / maxSliceSize ? startIndex % maxSliceSize :
                        0;
                int startIndexAtSlice = remainSize >= primitives.size() - startIndexAvaibleAtSlice ?
                        startIndexAvaibleAtSlice : primitives.size() - remainSize;
                int endIndexAtSlice = primitives.size();

                for (int k = endIndexAtSlice - 1; k >= startIndexAtSlice; k--) {
                    values.add(primitives.get(k));
                }
            }
            Collections.reverse(values);
            return values;

        } finally {
            striped.get(key).unlock();
        }
    }

    @Override
    public void listInsert(KeyValuesObj obj) throws Exception {
        if (obj.getValues().size() == 0) {
            return;
        }
        try {
            striped.get(obj.getKey()).lock();
            byte[] keyByteArray = obj.getKey().getBytes(StandardCharsets.UTF_8);
            byte[] headKeyBytes = ArrayUtils.addAll(PREFIX_HEAD_KEY_BYTES, keyByteArray);
            FastDbModel.SliceMetadata newSliceMetadata = null;
            try (Transaction transaction = opRocksDB.beginTransaction(new WriteOptions())) {
                List<JsonPrimitive> valuesToPush = obj.getValues().size() > maxElements ? obj.getValues().subList(0, maxElements) :
                        obj.getValues();
                if (!metadataMap.containsKey(obj.getKey())) {
                    if (obj.getType() == null) {
                        obj.setType(DataTypes.STRING_TYPE_STR);
                    }
                    else if (!DataTypes.isSupport(obj.getType()))
                    {
                        throw new UnsupportedOperationException(String.format("Type %s is not supported", obj.getType()));
                    }
                    LinkedHashMap<Long, FastDbModel.Slice> dataNodes = generateChunksDataNode(0, valuesToPush, obj.getType());
                    FastDbModel.SliceMetadata.Builder newMetadataBuilder =  FastDbModel.SliceMetadata.newBuilder()
                            .setKey(ByteString.copyFrom(keyByteArray))
                            .setType(obj.getType())
                            .setSize(valuesToPush.size());
                    for (Map.Entry<Long, FastDbModel.Slice> entry : dataNodes.entrySet()) {
                        newMetadataBuilder.addSliceIndies(entry.getKey());
                        transaction.put(fromIndexToDataNodeKey(keyByteArray, entry.getKey()),
                                entry.getValue().toByteArray());
                    }
                    newSliceMetadata = newMetadataBuilder.build();
                    transaction.put(headKeyBytes, newSliceMetadata.toByteArray());

                } else {
                    FastDbModel.SliceMetadata sliceMetadata = metadataMap.get(obj.getKey());
                    if (obj.getType() != null && !sliceMetadata.getType().equals(obj.getType())) {
                        throw new IllegalArgumentException(String.format("Key %s have %s data type, don't allow append with " +
                                "different data type %s",obj.getKey(), sliceMetadata.getType(), obj.getType()));
                    }
                    Map<Long, FastDbModel.Slice> dataNodes = new HashMap<>();
                    int nAdded = 0;
                    List<Long> dataNodeKeysToRemove;
                    int nSliceToRemove = estimateTotalSliceToRemove(valuesToPush.size(), sliceMetadata.getSize());
                    dataNodeKeysToRemove = sliceMetadata.getSliceIndiesList().subList(0, nSliceToRemove);
                    List<Long> remainDataNodeIndies = sliceMetadata.getSliceIndiesList().subList(nSliceToRemove,
                            sliceMetadata.getSliceIndiesCount());
                    int remainSize = sliceMetadata.getSize() - nSliceToRemove * maxSliceSize;
                    if (sliceMetadata.getSize() % maxSliceSize != 0) {
                        long firstNodeIndex =
                                remainDataNodeIndies.get(remainDataNodeIndies.size() - 1);
                        int nPad = maxSliceSize - remainSize % maxSliceSize;
                        FastDbModel.Slice firstSlice = FastDbModel
                                .Slice.parseFrom(transaction.get(new ReadOptions(),
                                        fromIndexToDataNodeKey(keyByteArray, firstNodeIndex)));
                        List<JsonPrimitive> listValuesFromFirstSlice = DataTypes.toListV(firstSlice.getData(),
                                sliceMetadata.getType());
                        if (nPad <= valuesToPush.size()) {
                            for (JsonPrimitive primitive : valuesToPush.subList(0, nPad)) {
                                listValuesFromFirstSlice.add(primitive);
                            }
                            valuesToPush = valuesToPush.subList(nPad, valuesToPush.size());
                            nAdded += nPad;
                        } else {
                            for (JsonPrimitive primitive : valuesToPush) {
                                listValuesFromFirstSlice.add(primitive);
                            }
                            nAdded += valuesToPush.size();
                            valuesToPush = Collections.emptyList();
                        }
                        FastDbModel.Slice newFirstSlice = FastDbModel.Slice.newBuilder()
                                .setData(DataTypes.toByteArray(listValuesFromFirstSlice, sliceMetadata.getType()))
                                .build();
                        dataNodes.put(firstNodeIndex, newFirstSlice);
                    }
                    FastDbModel.SliceMetadata.Builder newSliceMetadataBuilder = FastDbModel.SliceMetadata.newBuilder()
                            .setKey(ByteString.copyFrom(keyByteArray))
                            .addAllSliceIndies(remainDataNodeIndies).setType(sliceMetadata.getType());
                    if (valuesToPush.size() != 0) {
                        LinkedHashMap<Long, FastDbModel.Slice> slices =
                                sliceMetadata.getSliceIndiesCount() > 0 ?
                                        generateChunksDataNode(
                                                sliceMetadata.getSliceIndies(
                                                        sliceMetadata.getSliceIndiesCount() - 1)
                                                        + 1, valuesToPush, obj.getType()) :
                                        generateChunksDataNode(0, valuesToPush, sliceMetadata.getType());
                        for (Map.Entry<Long, FastDbModel.Slice> entry : slices.entrySet()) {
                            newSliceMetadataBuilder.addSliceIndies(entry.getKey());
                            dataNodes.put(entry.getKey(), entry.getValue());
                        }
                        nAdded += valuesToPush.size();
                    }
                    newSliceMetadataBuilder.setSize(remainSize + nAdded);
                    for (long index : dataNodes.keySet()) {
                        transaction.put(fromIndexToDataNodeKey(keyByteArray, index), dataNodes.get(index).toByteArray());
                    }
                    for (long index : dataNodeKeysToRemove) {
                        transaction.delete(fromIndexToDataNodeKey(keyByteArray, index));
                    }
                    newSliceMetadata = newSliceMetadataBuilder.build();
                    transaction.put(headKeyBytes, newSliceMetadata.toByteArray());
                }
                transaction.commit();
                metadataMap.put(obj.getKey(), newSliceMetadata);
            }
        } finally {
            striped.get(obj.getKey()).unlock();
        }
    }

    @Override
    public Iterator<TransactionLogIterator.BatchResult> getUpdatesSince(long sequenceNumber) throws RocksDBException {
        TransactionLogIterator logIterator = this.opRocksDB.getUpdatesSince(sequenceNumber);
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return logIterator.isValid();
            }

            @Override
            public TransactionLogIterator.BatchResult next() {
                TransactionLogIterator.BatchResult batchResult = logIterator.getBatch();
                logIterator.next();
                return batchResult;
            }
        };
    }

    public void writeUpdates(byte[] data) throws Exception {
        this.opRocksDB.write(new WriteOptions(), new WriteBatch(data));
    }

    @Override
    public long getLatestSequenceNumber() {
        return this.opRocksDB.getLatestSequenceNumber();
    }

    @Override
    public void writeUpdate(byte[] data) throws Exception {
        this.opRocksDB.write(new WriteOptions(), new WriteBatch(data));
    }

    int estimateTotalSliceToRemove(int pushSize, int currentSize) {
        if (pushSize >= maxElements) {
            if (currentSize % maxSliceSize == 0) {
                return currentSize / maxSliceSize;
            }
            else {
                return currentSize / maxSliceSize + 1;
            }
        }
        int nElementsToRemove = pushSize + currentSize - maxElements;
        if (nElementsToRemove >= maxSliceSize) {
            return nElementsToRemove / maxSliceSize;
        }
        return 0;
    }

    @Override
    public void listDelete(String key) throws RocksDBException, InvalidProtocolBufferException {
        Objects.requireNonNull(key, "key not be null");
        if (!metadataMap.containsKey(key)) {
            throw new IllegalArgumentException(String.format("Key %s is don't exists", key));
        }
        striped.get(key).lock();
        try {
            byte[] keyByteArray = key.getBytes(StandardCharsets.UTF_8);
            byte[] headKey = ArrayUtils.addAll(PREFIX_HEAD_KEY_BYTES, keyByteArray);
            try (Transaction transaction = opRocksDB.beginTransaction(new WriteOptions())) {
                byte[] headBytes = opRocksDB.get(headKey);
                if (headBytes == null) {
                    return;
                }
                FastDbModel.SliceMetadata metadata =  metadataMap.get(key);;
                for (long index : metadata.getSliceIndiesList()) {
                    transaction.delete(fromIndexToDataNodeKey(keyByteArray, index));
                }
                transaction.delete(headKey);
                transaction.commit();
                metadataMap.remove(key);
            }
        } finally {
            striped.get(key).unlock();
        }
    }

    public int numberOfKeys(String wildcard) {
        int count = 0;
        for (String key : metadataMap.keySet()) {
            if (key.matches(Utils.wildcaldToRegexp(wildcard))) {
                count++;
            }
        }
        return count;
    }

    public Collection<String> getKeys(String wildCard) {
        TreeSet<String> keys = new TreeSet<>();
        for (String key : metadataMap.keySet()) {
            if (key.matches(Utils.wildcaldToRegexp(wildCard))) {
                keys.add(key);
            }
        }
        return keys;
    }

    public int totalKey() {
        return metadataMap.size();
    }

    private <V> LinkedHashMap<Long, FastDbModel.Slice> generateChunksDataNode(long startOffset,
                                                                                   List<JsonPrimitive> jsonPrimitives,
                                                                                       String type) throws Exception {
        LinkedHashMap<Long, FastDbModel.Slice> slices = new LinkedHashMap<>();
        long keyIncr = startOffset;
        List<JsonPrimitive> current = new ArrayList<>();
        for (int i = 0; i < jsonPrimitives.size() ; i++) {
            current.add(jsonPrimitives.get(i));
            if (current.size() == this.maxSliceSize || (current.size() > 0 && i == jsonPrimitives.size() - 1)) {
                FastDbModel.Slice slice = FastDbModel.Slice.newBuilder()
                        .setData(DataTypes.toByteArray(current, type))
                        .build();
                slices.put(keyIncr, slice);
                current.clear();
                keyIncr++;
            }
        }
        return slices;
    }

    @Override
    public void close() throws Exception {
        opRocksDB.close();
    }

}
