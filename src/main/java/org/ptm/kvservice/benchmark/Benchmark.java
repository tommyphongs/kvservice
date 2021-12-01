package org.ptm.kvservice.benchmark;

import org.ptm.kvservice.db.DataTypes;
import org.ptm.kvservice.db.FastDBIml;
import org.ptm.kvservice.db.FastDB;
import org.ptm.kvservice.utils.Monitor;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.commons.cli.*;
import org.apache.http.*;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class Benchmark {

    private final static String PREFIX = "kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk";
    private final static String KEY_FORMAT = "%09d";
    private final static Random RANDOM = new Random();
    private static final Logger LOGGER = LoggerFactory.getLogger(Benchmark.class);

    public Benchmark() {
    }

    public static void main(String[] args) throws Exception {
        try {
            Options options = new Options();
            options.addOption(Option.builder().longOpt("type").hasArg().build())
                    .addOption(Option.builder().longOpt("numberOfKey").hasArg().build())
                    .addOption(Option.builder().longOpt("nThread").hasArg().build())
                    .addOption(Option.builder().longOpt("loadStartIndex").hasArg().build())
                    .addOption(Option.builder().longOpt("arrLoadSize").hasArg().build())
                    .addOption(Option.builder().longOpt("arrReadSize").hasArg().build())
                    .addOption(Option.builder().longOpt("arrWriteSize").hasArg().build())
                    .addOption(Option.builder().longOpt("rwRatio").hasArg().build())
                    .addOption(Option.builder().longOpt("numberOfOps").hasArg().build())
                    .addOption(Option.builder().longOpt("batchSize").hasArg().build())
                    .addOption(Option.builder().longOpt("dbDir").hasArg().build())
                    .addOption(Option.builder().longOpt("serverUrl").required(false).hasArg().build())
                    .addOption(Option.builder().longOpt("out").hasArg().build())
            ;
            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);
            OutputStream outputStream;
            if (!cmd.hasOption("out")) {
                outputStream = System.out;
            } else {
                outputStream = new FileOutputStream(cmd.getOptionValue("out"));
            }
            if (!cmd.hasOption("type")) {
                throw new IllegalArgumentException("type arg must be set");
            }
            try (BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream))) {
                switch (cmd.getOptionValue("type")) {
                    case "load":
                        long startIndex = 0;
                        if (cmd.hasOption("loadStartIndex")) {
                            startIndex = Long.parseLong(cmd.getOptionValue("loadStartIndex"));
                        }
                        loadBenchmark( startIndex, Integer.parseInt(cmd.getOptionValue("nThread")),
                                Integer.parseInt(cmd.getOptionValue("numberOfKey")),
                                Integer.parseInt(cmd.getOptionValue("arrLoadSize")),
                                cmd.getOptionValue("serverUrl"),
                                bufferedWriter);
                        break;
                    case "read":
                        readBenchmark(Integer.parseInt(cmd.getOptionValue("nThread")),
                                Integer.parseInt(cmd.getOptionValue("numberOfKey")),
                                Integer.parseInt(cmd.getOptionValue("numberOfOps")),
                                Integer.parseInt(cmd.getOptionValue("batchSize")),
                                Integer.parseInt(cmd.getOptionValue("arrReadSize")),
                                cmd.getOptionValue("serverUrl"),
                                bufferedWriter);
                        break;
                    case "read_directly":
                        readDirectlyBenchmark(Integer.parseInt(cmd.getOptionValue("nThread")),
                                Integer.parseInt(cmd.getOptionValue("numberOfKey")),
                                Integer.parseInt(cmd.getOptionValue("numberOfOps")),
                                Integer.parseInt(cmd.getOptionValue("arrReadSize")),
                                cmd.getOptionValue("dbDir"),
                                Integer.parseInt(cmd.getOptionValue("batchSize")),
                                bufferedWriter);
                        break;
                    case "load_directly":
                        loadDirectlyBenchmark(Integer.parseInt(cmd.getOptionValue("nThread")),
                                Integer.parseInt(cmd.getOptionValue("numberOfKey")),
                                Integer.parseInt(cmd.getOptionValue("arrReadSize")),
                                cmd.getOptionValue("dbDir"),
                                bufferedWriter);
                    default:
                        throw new IllegalArgumentException(cmd.getOptionValue("type") + " type not support");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static HttpPost buildInsertRequest(long index, int arrSize, String serverUri)
            throws URISyntaxException, UnsupportedEncodingException {
        String key = PREFIX + String.format(KEY_FORMAT, index);
        URIBuilder uriBuilderAppend = new URIBuilder(serverUri);
        HttpPost httpPost = new HttpPost(uriBuilderAppend.build());
        httpPost.setHeader("Accept", "application/json");
        httpPost.setHeader("Content-type", "application/json");
        JsonObject jsonObject = new JsonObject();
        jsonObject.add("key", new JsonPrimitive(key));
        JsonArray array = new JsonArray();
        RANDOM.longs(arrSize).boxed().forEach(array::add);
        jsonObject.add("values", array);
        jsonObject.add("type", new JsonPrimitive(DataTypes.LONG_TYPE_STR));
        httpPost.setEntity(new StringEntity(jsonObject.toString()));
        return httpPost;
    }

    private static HttpPost buildGetRequest(List<Integer> indexes, int arrSize, String serverUri)
            throws URISyntaxException, UnsupportedEncodingException {

        JsonArray keys = new JsonArray();
        indexes.forEach(index -> keys.add(PREFIX + String.format(KEY_FORMAT, index)));
        URIBuilder uriBuilderAppend = new URIBuilder(serverUri);
        HttpPost httpPost = new HttpPost(uriBuilderAppend.build());
        httpPost.setHeader("Accept", "application/json");
        httpPost.setHeader("Content-type", "application/json");
        httpPost.setHeader("Accept-Encoding", "gzip, deflate");
        JsonObject jsonObject = new JsonObject();
        jsonObject.add("keys", keys);
        jsonObject.add("size", new JsonPrimitive(arrSize));
        httpPost.setEntity(new StringEntity(jsonObject.toString()));
        return httpPost;
    }

    public static CloseableHttpClient createHttpClient() {
        ConnectionKeepAliveStrategy myStrategy = (response, context) -> {
            HeaderElementIterator it = new BasicHeaderElementIterator
                    (response.headerIterator(HTTP.CONN_KEEP_ALIVE));
            while (it.hasNext()) {
                HeaderElement he = it.nextElement();
                String param = he.getName();
                String value = he.getValue();
                if (value != null && param.equalsIgnoreCase
                        ("timeout")) {
                    return Long.parseLong(value) * 1000;
                }
            }
            return 5000 * 1000;
        };
        PoolingHttpClientConnectionManager connManager
                = new PoolingHttpClientConnectionManager();
        return HttpClients.custom()
                .setKeepAliveStrategy(myStrategy)
                .setConnectionManager(connManager)
                .build();
    }

    public static Thread createMonitorThread(Monitor monitor, BufferedWriter outputWriter, Map<String, AtomicLong> metrics) {
        return new Thread( () -> {
                try {
                    while (monitor.isRunning()) {
                        JsonObject obj = monitor.getThroughput();
                        metrics.forEach((k, v) -> obj.add(k, new JsonPrimitive(v.get())));
                        outputWriter.write(obj.toString());
                        outputWriter.newLine();
                        outputWriter.flush();
                        TimeUnit.SECONDS.sleep(1);
                    }
                    outputWriter.write(monitor.getSnapshots().toString());
                    outputWriter.newLine();
                    outputWriter.flush();
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(0);
                }
        });
    }

    public static void readBenchmark(int nThread, int numberOfKey, long numberOfOps, int batchSize,
                                          int readArrSize, String serverUrl,
                                          BufferedWriter outWriter) throws IOException, ExecutionException, InterruptedException {
            final String metricName = "get";
            final String uri = serverUrl + "/api/list/tailpeek";
            AtomicLong opsCurrent = new AtomicLong(0);
            AtomicInteger currentIndex = new AtomicInteger(new Random().nextInt(numberOfKey));
            ExecutorService executorService = Executors.newFixedThreadPool(nThread + 1);
            Set<String> metrics = new HashSet<>();
            metrics.add(metricName);

            Monitor monitor = new Monitor(metrics, batchSize);
            monitor.start();
            List<Future> futures = new ArrayList<>();
            JsonObject jsonObject = new JsonObject();
            jsonObject.add("nThread", new JsonPrimitive(nThread));
            jsonObject.add("numberOfKey", new JsonPrimitive(numberOfKey));
            jsonObject.add("number_of_ops", new JsonPrimitive(numberOfOps));
            jsonObject.add("readArrSize", new JsonPrimitive(readArrSize));
            outWriter.write(jsonObject.toString());
            outWriter.newLine();
            Future monitorFuture = executorService.submit(createMonitorThread(monitor, outWriter, Map.of()));
            Gson gson = new Gson();
            for (int i = 0; i < nThread; i++) {
                futures.add(executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        try(CloseableHttpClient client = createHttpClient()) {
                            Random random = new Random();
                            long startTime;
                            List<Integer> indexes = new ArrayList<>();
                            while ((opsCurrent.incrementAndGet()) * 1.0 < numberOfOps) {
                                indexes.add(random.nextInt(numberOfKey));
                                if (indexes.size() == batchSize) {
                                    startTime = System.currentTimeMillis();
                                    CloseableHttpResponse res = client.execute(buildGetRequest(indexes, readArrSize, uri));
                                    byte[] bytes = res.getEntity().getContent().readAllBytes();
                                    System.out.println(System.currentTimeMillis() - startTime);
                                    monitor.recordValue(metricName, System.currentTimeMillis() - startTime);
                                    indexes.clear();
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }));
            }
            for (Future future : futures) {
                future.get();
            }
            monitor.stop();
            monitorFuture.get();
            executorService.shutdown();
    }

    public static void readDirectlyBenchmark(int nThread, int numberOfKey, long numberOfOps,
                                             int readArrSize, String dbDir, int batchSize,
                                             BufferedWriter outWriter) throws Exception {
        try (FastDBIml fastDB
                = FastDB.createFastDB(new FastDBIml.Options().setDirName(dbDir))) {
            AtomicLong opsCurrent = new AtomicLong(0);
            AtomicLong currentIndex = new AtomicLong(new Random().nextInt(numberOfKey));
            ExecutorService executorService = Executors.newFixedThreadPool(nThread + 1);
            Set<String> metrics = new HashSet<>();
            metrics.add("read");
            Monitor monitor = new Monitor(metrics, 1);
            monitor.start();
            List<Future> futures = new ArrayList<>();
            JsonObject jsonObject = new JsonObject();
            jsonObject.add("nThread", new JsonPrimitive(nThread));
            jsonObject.add("numberOfKey", new JsonPrimitive(numberOfKey));
            jsonObject.add("number_of_ops", new JsonPrimitive(numberOfOps));
            jsonObject.add("readArrSize", new JsonPrimitive(readArrSize));
            outWriter.write(jsonObject.toString());
            outWriter.newLine();
            Future monitorFuture = executorService.submit(createMonitorThread(monitor, outWriter, Map.of()));
            for (int i = 0; i < nThread; i++) {
                futures.add(executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            Random random = new Random();
                            List<String> keys = new ArrayList<>();
                            while ((opsCurrent.incrementAndGet()) * 1.0 < numberOfOps) {
                               String key =  PREFIX + String.format(KEY_FORMAT, random.nextInt(numberOfKey));
                               keys.add(key);
                               long startTime = System.currentTimeMillis();
                               if (keys.size() == batchSize )
                               {
                                   fastDB.listHeadPeek(keys, readArrSize);
                                   keys.clear();
                                   monitor.recordValue("read", System.currentTimeMillis() - startTime);
                               }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }));
            }
            for (Future future : futures) {
                future.get();
            }
            monitor.stop();
            monitorFuture.get();
            executorService.shutdown();
        }
    }

    public static void loadDirectlyBenchmark(int nThread, int numberOfKey,
                                             int readArrSize, String dbDir,
                                             BufferedWriter outWriter) throws Exception {
        try (FastDB fastDB
                     = FastDB.createFastDB(new FastDBIml.Options().setDirName(dbDir))) {
            AtomicLong opsCurrent = new AtomicLong(0);
            AtomicLong currentIndex = new AtomicLong(new Random().nextInt(numberOfKey));
            ExecutorService executorService = Executors.newFixedThreadPool(nThread + 1);
            Set<String> metrics = new HashSet<>();
            metrics.add("read");
            Monitor monitor = new Monitor(metrics, 1);
            monitor.start();
            List<Future> futures = new ArrayList<>();
            JsonObject jsonObject = new JsonObject();
            jsonObject.add("nThread", new JsonPrimitive(nThread));
            jsonObject.add("numberOfKey", new JsonPrimitive(numberOfKey));
            jsonObject.add("readArrSize", new JsonPrimitive(readArrSize));
            outWriter.write(jsonObject.toString());
            outWriter.newLine();
            Future monitorFuture = executorService.submit(createMonitorThread(monitor, outWriter, Map.of()));
            Random random = new Random();
            for (int i = 0; i < nThread; i++) {
                futures.add(executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            while ((opsCurrent.incrementAndGet()) * 1.0 < numberOfKey) {
                                String key =  PREFIX + String.format(KEY_FORMAT, currentIndex.incrementAndGet() % numberOfKey);
                                long startTime = System.currentTimeMillis();
                                List<Long> longs = random.longs(readArrSize).boxed().collect(Collectors.toList());
//                                fastDB.listAppend(key.getBytes(StandardCharsets.UTF_8), longs);
                                monitor.recordValue("read", System.currentTimeMillis() - startTime);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }));
            }
            for (Future future : futures) {
                future.get();
            }
            monitor.stop();
            monitorFuture.get();
            executorService.shutdown();
        }
    }


    @SuppressWarnings("rawtypes")
    public static void loadBenchmark(long startIndex, int nThread, int numberOfKey, int arrSize,
                                     String serverUrl, BufferedWriter outWriter) throws IOException, InterruptedException, ExecutionException {
        try(CloseableHttpClient client = createHttpClient()) {
            final String metricName = "load";
            final String uri = serverUrl + "/api/list/insert";
            AtomicLong current = new AtomicLong(startIndex);
            ExecutorService executorService = Executors.newFixedThreadPool(nThread + 1);
            Set<String> metrics = new HashSet<>();
            metrics.add(metricName);
            Monitor monitor = new Monitor(metrics, 1);
            monitor.start();
            List<Future> futures = new ArrayList<>();
            Future monitorFuture =
                    executorService.submit(createMonitorThread(monitor, outWriter, Map.of("Current index", current)));
            for (int i = 0; i < nThread; i++) {
                futures.add(executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        long index;
                        try {
                            long startTime;
                            while ((index = current.incrementAndGet()) < numberOfKey) {
                                startTime = System.currentTimeMillis();
                                CloseableHttpResponse res = client.execute(buildInsertRequest(index, arrSize, uri));
                                res.getEntity().getContent().readAllBytes();
                                monitor.recordValue(metricName, System.currentTimeMillis() - startTime);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }));
            }
            while (current.get() < numberOfKey) {
                TimeUnit.SECONDS.sleep(1);
            }
            for (Future future : futures) {
                future.get();
            }
            monitor.stop();
            monitorFuture.get();
            executorService.shutdown();
        }
    }

}
