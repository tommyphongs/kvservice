package org.ptm.kvservice.db;

import org.ptm.kvservice.db.models.KeyValuesObj;
import com.google.gson.JsonPrimitive;
import org.rocksdb.RocksDBException;
import org.rocksdb.TransactionLogIterator;
import shaded.org.apache.commons.lang3.ArrayUtils;

import java.nio.ByteBuffer;
import java.util.*;

public interface FastDB extends AutoCloseable {

    List<JsonPrimitive> listHeadPeek(String key, int size) throws Exception;

    void listInsert(KeyValuesObj keyValuesObj) throws Exception;

    List<JsonPrimitive> listTailPeek(String key, int size) throws Exception;

    void listDelete(String key) throws Exception;

    Iterator<TransactionLogIterator.BatchResult> getUpdatesSince(long sequenceNumber) throws RocksDBException;

    long getLatestSequenceNumber();

    void writeUpdate(byte[] data) throws Exception;

    static FastDBIml createFastDB(FastDBIml.Options options) throws Exception {
        return new FastDBIml(options);
    }

    default byte[] fromIndexToDataNodeKey(byte[] baseKey, long index) {
        return ArrayUtils.addAll(baseKey, ByteBuffer.allocate(8).putLong(index).array());
    }

}
