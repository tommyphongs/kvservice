package org.ptm.kvservice.db;


import org.ptm.kvservice.db.models.KeyValuesObj;
import com.google.gson.JsonPrimitive;
import org.apache.log4j.PropertyConfigurator;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

class FastDBImlTest {

    static {
        PropertyConfigurator.configure("src/main/resources/lo4j.properties");
    }

    private static final  Random random = new Random();
    private static final Logger LOGGER = LoggerFactory.getLogger(FastDBIml.class);

    private static final String KEY_TEST = "key1";

    @Test
    public void test1EstimateRemoveChunkSize() throws Exception {
        PropertyConfigurator.configure("src/main/resources/lo4j.properties");
        try (FastDB fastDB = FastDB.createFastDB(new FastDBIml.Options()
                .setDirName(System.getenv("FASTDB_DIR")))) {
            List<JsonPrimitive> data = new ArrayList<>();
            random.longs(1999).boxed().forEach(l -> data.add(new JsonPrimitive(l)));
            KeyValuesObj obj = new KeyValuesObj(KEY_TEST, data, DataTypes.LONG_TYPE_STR);
            fastDB.listInsert(obj);

        }

    }

    @Test
    public void test2PushFront() throws Exception {
        try (FastDB fastDB = FastDB.createFastDB(new FastDBIml.Options()
                .setDirName(System.getenv("LISTDB_DIR")))) {
            fastDB.listDelete(KEY_TEST);
            List<JsonPrimitive> data = new ArrayList<>();
            random.longs(1999).boxed().forEach(l -> data.add(new JsonPrimitive(l)));
            KeyValuesObj obj = new KeyValuesObj(KEY_TEST, data, DataTypes.LONG_TYPE_STR);
            fastDB.listInsert(obj);
        }
    }

//    @Test
//    public void test3DeleteKey() throws Exception {
//        try (FastDB fastDB = FastDB.createFastDB(new FastDBIml.Options()
//                .setDirName(System.getenv("LISTDB_DIR")))) {
//            fastDB.listAppend(KEY_TEST.getBytes(), Collections.emptyList());
//            fastDB.listDelete(KEY_TEST.getBytes());
////            Random random = new Random();
////            rocksdbWrapper.pushFront(KEY_TEST, random.longs(1999).boxed().collect(Collectors.toList()));
//            System.out.println(fastDB.listGet(KEY_TEST.getBytes(), 10000).size());
////            try (Transaction transaction = rocksdbWrapper.opRocksDB.beginTransaction(new WriteOptions())) {
////                transaction.put("phong".getBytes(), "khoang".getBytes());
////                transaction.delete("phong".getBytes());
////                System.out.println(rocksdbWrapper.opRocksDB.get("phong".getBytes()));
////            }
//        }
//
//    }

    @Test
    public void test4PushFrontEmptyList() throws Exception {
        try (FastDB fastDB = FastDB.createFastDB(new FastDBIml.Options()
                .setDirName(System.getenv("LISTDB_DIR")))) {
//            listDB.delete(KEY_TEST);
//            listDB.addAll(KEY_TEST, new Random().longs(100).boxed().collect(Collectors.toList()));
//            System.out.println(listDB.get(KEY_TEST, 1000000).size());
//            Assertions.assertEquals(0, listDB.get(KEY_TEST, 10000).size());

        }
    }

//    @Test
//    public void test5PushMultiFront() throws Exception {
//        try (FastDB fastDB = FastDB.createFastDB(new FastDBIml.Options()
//                .setDirName(System.getenv("LISTDB_DIR")))) {
//            Random random = new Random();
//            List<Long> longsFirst = new ArrayList<>();
//            for (long i = 0; i < 99; i++) {
//                longsFirst.add(i);
//            }
//            List<Long> longsSecond = new ArrayList<>();
//            for (long i = 0; i < 9; i++) {
//                longsSecond.add(2000 + i);
//            }
//            List<Long> longsThird = new ArrayList<>();
//            for (long i = 0; i < 1980; i++) {
//                longsThird.add(10000 + i);
//            }
//
//            List<Long> longsFour = new ArrayList<>();
//            for (long i = 0; i < 20; i++) {
//                longsFour.add(20000 + i);
//            }
//
//
//            fastDB.listAppend(KEY_TEST.getBytes(), longsFirst);
//            fastDB.listAppend(KEY_TEST.getBytes(), longsSecond);
////            listDB.pushFront(KEY_TEST, longsThird);
////            listDB.pushFront(KEY_TEST, longsFour);
////            Assertions.assertEquals(2009, rocksdbWrapper.poll(KEY_TEST, 10000).size());
//            System.out.println(longsFirst.size() + longsSecond.size() + longsThird.size() + longsFour.size());
//            System.out.println(fastDB.listGet(KEY_TEST.getBytes(), 10000).size());
//            System.out.println(fastDB.listGet(KEY_TEST.getBytes(), 100000));
//        }
//    }

    @Test
    public void testGetAllKey() throws Exception {
        try (FastDB fastDB = FastDB.createFastDB(new FastDBIml.Options()
                .setDirName(System.getenv("LISTDB_DIR")))) {
        }
    }


}