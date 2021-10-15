package fr.ans.psc.pscextract.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.management.RuntimeErrorException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * The type Aggregation service.
 */
@Service
public class AggregationService {

    /**
     * logger.
     */
    private static final Logger log = LoggerFactory.getLogger(AggregationService.class);

    @Value("${mongodb.name}")
    private String mongodbName;

    @Value("${mongodb.host}")
    private String mongoHost;

    @Value("${mongodb.port}")
    private String mongoPort;

    @Value("${mongodb.username}")
    private String mongoUserName;

    @Value("${mongodb.password}")
    private String mongoPassword;

    @Value("${mongodb.admin.database}")
    private String mongoAdminDatabase;

    /**
     * Instantiates a new Extraction service.
     */
    private AggregationService() {
    }

    /**
     * Aggregate.
     */
    public void aggregate() throws IOException {
        log.info("aggregating ...");
        String cmdSafe = "mongosh --host=" + mongoHost + " --port=" + mongoPort + " --username=" + mongoUserName + " --password=***"
                + " --authenticationDatabase=" + mongoAdminDatabase + " " + mongodbName + " < /app/resources/aggregate.mongo";
        log.info("running command: {}", cmdSafe);

        // transform Dos/Windows end of lines (CRLF) to Unix end of lines (LF).
        Process dos2Unix = Runtime.getRuntime().exec("dos2unix /app/resources/aggregate.mongo");
        if (dos2Unix.exitValue() != 0) {
            log.error("Dos2Unix failed : code retour = {}", dos2Unix.exitValue());
            throw new RuntimeException("Dos2Unix failed");
        }

        String cmd = "mongosh --host=" + mongoHost + " --port=" + mongoPort + " --username=" + mongoUserName + " --password=" + mongoPassword
                + " --authenticationDatabase=" + mongoAdminDatabase + " " + mongodbName + " < /app/resources/aggregate.mongo";

        String[] cmdArr = {
                "/bin/sh",
                "-c",
                cmd
        };

        Process p = Runtime.getRuntime().exec(cmdArr);
        if (p.exitValue() == 0) {
            log.info("finished aggregation");
        } else {
            StringBuilder infoBuilder = new StringBuilder();
            StringBuilder errorBuilder = new StringBuilder();
            try (Reader infoReader = new BufferedReader(new InputStreamReader
                    (p.getInputStream(), Charset.forName(StandardCharsets.UTF_8.name())));
                 Reader errorReader = new BufferedReader(new InputStreamReader
                         (p.getErrorStream(), Charset.forName(StandardCharsets.UTF_8.name())))) {
                int c;
                while ((c = infoReader.read()) != -1) {
                    infoBuilder.append((char) c);
                }
                while ((c = errorReader.read()) != -1) {
                    errorBuilder.append((char) c);
                }
            }
            log.error("inputstream : {}", infoBuilder);
            log.error("errorstream : {}", errorBuilder);
            log.error("exit value : {}", p.exitValue());

            throw new RuntimeException("mongosh command failed : " + errorBuilder);
        }
    }

}
