package org.ptm.kvservice.db;

import org.apache.log4j.BasicConfigurator;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class SeekTest {

    static {
        BasicConfigurator.configure();
    }
    private static final Logger LOGGER = LoggerFactory.getLogger(SeekTest.class);

    @Test
    public void test1EstimateRemoveChunkSize() throws Exception {
        long startTime = System.currentTimeMillis();
        try (FastDBIml fastDB
                     = FastDB.createFastDB(new FastDBIml.Options().setDirName(System.getenv("DATA_DIR")));) {
            AtomicLong count = new AtomicLong(0);
            fastDB.getAllMetadata().forEach((k, v) -> {
                count.incrementAndGet();
                if (count.get() % 1000 == 0) {
                    System.out.println(count.get());
                }
            });
            System.out.println(System.currentTimeMillis() - startTime);
        }
    }

    @Test
    public void test2EstimateRemoveChunkSize() throws Exception {
        final String PREFIX = "kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk";
        final String KEY_FORMAT = "%09d";

        final int totalKeys = 1_000_000;
        final int maxNumberOfValues = 1_000_000;
        List<AtomicLong> values = new Random().longs(maxNumberOfValues).boxed().map(AtomicLong::new).collect(Collectors.toList());

        LOGGER.info("Total memory: {} at", Runtime.getRuntime().totalMemory());

        int[][] kvalues = new int[totalKeys][2000];
        Random random = new Random();
        for (int i = 0; i < totalKeys; i++) {
            for (int j = 0; j < 2000; j++) {
                kvalues[i][j] = random.nextInt(maxNumberOfValues);
            }
            if (i % 1000 == 0) {
                LOGGER.info("Total memory: {} at {}", Runtime.getRuntime().totalMemory(), i);
            }
        }


    }


}
