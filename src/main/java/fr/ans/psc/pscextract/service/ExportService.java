package fr.ans.psc.pscextract.service;

import fr.ans.psc.pscextract.model.PsLine;
import fr.ans.psc.pscextract.service.utils.FileNamesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The type Extraction service.
 */
@Service
public class ExportService {

    /**
     * logger.
     */
    private static final Logger log = LoggerFactory.getLogger(ExportService.class);

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

    @Value("${files.directory}")
    private String filesDirectory;

    @Value("${extract.name}")
    private String extractName;

    /**
     * Instantiates a new Extraction service.
     */
    private ExportService() {
    }

    /**
     * Extract.
     *
     * @throws IOException the io exception
     */
    public void export() throws IOException {
        List<String> fieldsList = Arrays.stream(PsLine.class.getDeclaredFields()).map(Field::getName).collect(Collectors.toList());

        String fields = String.join(",", fieldsList);

        String cmd = "mongoexport " +
                "--db=" + mongodbName + " " +
                "--username=" + mongoUserName + " " +
                "--password=" + mongoPassword + " " +
                "--authenticationDatabase=" + mongoAdminDatabase + " " +
                "--collection=extractRass " +
                "--host=" + mongoHost + " " +
                "--port=" + mongoPort + " " +
                "--fields=" + fields + " " +
                "--out=" + FileNamesUtil.getFilePath(filesDirectory, extractName) + " " +
                "--type=csv " +
                "--forceTableScan";

        log.info("running command : {}", cmd);
        log.info("exporting schema {}", "extractRass");

        Runtime.getRuntime().exec(cmd);

        log.info("export done");
    }

}
