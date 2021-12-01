package org.ptm.kvservice;

import org.ptm.kvservice.db.FastDB;
import org.ptm.kvservice.db.FastDBIml;
import org.ptm.kvservice.transpost.HttpService;
import org.ptm.kvservice.transpost.MasterService;
import org.ptm.kvservice.transpost.SlaveService;
import org.ptm.kvservice.utils.Stats;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("ALL")
public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
    static {
        if (System.getenv().containsKey("LOG4J_CONF_FILE")) {
            PropertyConfigurator.configure(System.getenv("LOG4J_CONF_FILE"));
        }
        else {
            BasicConfigurator.configure();
        }
    }

    public static void main(String[] args) {
        String dir = System.getenv("DATA_DIR");
        int httpPort = Integer.parseInt(System.getenv("HTTP_PORT"));
        int grpcPort = Integer.parseInt(System.getenv("GRPC_PORT"));
        String currentSequenceFile = dir + "/" + "CURRENT_SEQUENCE_NUMBER";
        Boolean isMaster = System.getenv("MODE").toUpperCase().equals("MASTER");
        if (isMaster == null) {
            throw new IllegalArgumentException("MODE ENV must be set");
        }

        LOGGER.info("Data dir {}", dir);
        LOGGER.info("Run at http port {}", httpPort);
        LOGGER.info("Run at grpc port {}", grpcPort);
        LOGGER.info("Mode {} ", isMaster ? "MASTER" : "SLAVE");
        FastDBIml.Options options = new FastDBIml.Options().setDirName(dir)
                .setMMapReads(true).setMMapWrites(true);
        try (FastDBIml fastDB
                     = FastDB.createFastDB(options);
             RandomAccessFile randomAccessFile = new RandomAccessFile(currentSequenceFile, "rw")) {
            List<Service> services = new ArrayList<>();
            HttpService httpService = new HttpService(fastDB, httpPort, isMaster,options );
            Stats.init(dir,httpService.getRequestName(), fastDB);
            services.add(httpService);
            if (isMaster) {
                MasterService masterService = new MasterService(fastDB, grpcPort);
                services.add(masterService);
            }
            else {
                SlaveService.createInstance(fastDB, System.getenv("MASTER_ADDR"), grpcPort, randomAccessFile);
                SlaveService slaveService = SlaveService.getInstance();
                services.add(slaveService);
            }
            ServiceManager serviceManager = new ServiceManager(services);
            serviceManager.addListener(new ServiceManager.Listener() {
                @Override
                public void healthy() {
                    super.healthy();
                }
            });
            serviceManager.startAsync();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info("Shutdown.......");
                serviceManager.stopAsync();
            }));
            while (true) {
                TimeUnit.SECONDS.sleep(1);
                for (Service.State state : serviceManager.servicesByState().keys()) {
                    if (!state.equals(Service.State.RUNNING)) {
                        LOGGER.info("Stop all service");
                        serviceManager.stopAsync();
                        return;
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error when run server", e);
        }
    }

}
