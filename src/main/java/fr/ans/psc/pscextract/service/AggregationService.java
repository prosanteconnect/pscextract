package fr.ans.psc.pscextract.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

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

    @Value("${mongodb.host")
    private String mongoHost;

    @Value("${mongodb.port}")
    private String mongoPort;

    @Value("${mongodb.username")
    private String mongoUserName;

    @Value("${mongodb.password")
    private String mongoPassword;

    @Value("${mongodb.admin.database")
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

        // transform Dos/Windows end of lines (CRLF) to Unix end of lines (LF).
        Runtime.getRuntime().exec("dos2unix /app/resources/aggregate.mongo");

        String cmd = "mongosh --host=" + mongoHost + " --port=" + mongoPort + " --username=" + mongoUserName + " --password=" + mongoPassword
                + " --authenticationDatabase=" + mongoAdminDatabase + " " + mongodbName + " < /app/resources/aggregate.mongo";

        String[] cmdArr = {
                "/bin/sh",
                "-c",
                cmd
        };

        log.info("running command: {}", cmd);

        Process p = Runtime.getRuntime().exec(cmdArr);
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
        log.debug("inputstream : {}", infoBuilder);
        log.debug("errorstream : {}", errorBuilder);
        log.debug("exit value : {}", p.exitValue());
        log.info("finished aggregation");
    }

}
