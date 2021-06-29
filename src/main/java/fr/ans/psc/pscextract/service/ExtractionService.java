package fr.ans.psc.pscextract.service;

import fr.ans.psc.pscextract.model.PsLine;
import fr.ans.psc.pscextract.service.utils.FileNamesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
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
public class ExtractionService {

    /**
     * logger.
     */
    private static final Logger log = LoggerFactory.getLogger(ExtractionService.class);

    @Autowired
    private MongoTemplate mongoTemplate;

    @Value("${mongodb.outCollection}")
    private String outCollection;

    @Value("${mongodb.addr}")
    private String mongoAddr;

    @Value("${files.directory}")
    private String filesDirectory;

    @Value("${extract.name}")
    private String extractName;

    /**
     * Instantiates a new Extraction service.
     */
    private ExtractionService() {
    }

    /**
     * Extract.
     *
     * @throws IOException the io exception
     */
    public void extract() throws IOException {
        List<String> fieldsList = Arrays.stream(PsLine.class.getDeclaredFields()).map(Field::getName).collect(Collectors.toList());

        String fields = String.join(",", fieldsList);

        String cmd = "mongoexport " +
                "--db=" + mongoTemplate.getDb().getName() + " " +
                "--collection=" + outCollection + " " +
                "--host=" + mongoAddr + " " +
                "--fields=" + fields + " " +
                "--out=" + FileNamesUtil.getFilePath(filesDirectory, extractName) + " " +
                "--type=csv " +
                "--forceTableScan";

        log.info("running command : {}", cmd);
        log.info("exporting schema {}", outCollection);

        Runtime.getRuntime().exec(cmd);

        log.info("export done");
    }

}
