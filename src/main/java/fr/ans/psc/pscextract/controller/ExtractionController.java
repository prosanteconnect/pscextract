package fr.ans.psc.pscextract.controller;

import fr.ans.psc.pscextract.service.AggregationService;
import fr.ans.psc.pscextract.service.ExportService;
import fr.ans.psc.pscextract.service.TransformationService;
import fr.ans.psc.pscextract.service.utils.FileNamesUtil;
import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RestController
public class ExtractionController {

    @Autowired
    ExportService exportService;

    @Autowired
    AggregationService aggregationService;

    @Autowired
    TransformationService transformationService;

    @Value("${files.directory}")
    private String filesDirectory;

    @Value("${extract.test.name}")
    public String extractTestName;

    @Value("${extract.name}")
    private String extractName;

    /**
     * logger.
     */
    private static final Logger log = LoggerFactory.getLogger(ExtractionController.class);

    @GetMapping(value = "/check", produces = MediaType.APPLICATION_JSON_VALUE)
    public String index() {
        return "alive";
    }

    @GetMapping(value = "/files", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String listFiles() {
        return Stream.of(Objects.requireNonNull(new File(filesDirectory).listFiles()))
                .filter(file -> !file.isDirectory())
                .map(file -> file.getName() + ":" + file.length())
                .collect(Collectors.toSet()).toString();
    }

    @PostMapping(value = "/aggregate")
    public String aggregate() {
        ForkJoinPool.commonPool().submit(() -> {
            try {
                aggregationService.aggregate();
            } catch (Exception e) {
                log.error("Error during aggregation", e);
            }
            log.info("Aggregation done.");
        });
        return "Aggregating...";
    }

    @PostMapping(value = "/export")
    public String export() {
        ForkJoinPool.commonPool().submit(() -> {
            try {
                exportService.export();
            } catch (IOException | InterruptedException e) {
                log.error("Error during export", e);
            }
        });
        return "Exporting...";
    }

    @PostMapping(value = "/transform")
    public DeferredResult<ResponseEntity<String>> transform() {
        DeferredResult<ResponseEntity<String>> output = new DeferredResult<>();
        ForkJoinPool.commonPool().submit(() -> {
            try {
                transformationService.transformCsv();
                FileNamesUtil.cleanup(filesDirectory, extractTestName);
            } catch (IOException e) {
                log.error("Error during transformation", e);
                log.error(e.getMessage());
            }
            log.info("Transformation done.");
            output.setResult(ResponseEntity.ok("Transformation done."));
        });
        return output;
    }

    @PostMapping(value = "/generate-extract")
    public void generateExtract() {
        ForkJoinPool.commonPool().submit(() -> {
            try {
                aggregationService.aggregate();
                exportService.export();
                transformationService.transformCsv();
                FileNamesUtil.cleanup(filesDirectory, extractTestName);
            } catch (IOException | InterruptedException e) {
                log.error("Exception raised :", e);
            }
        });
    }

    @GetMapping(value = "/download")
    @ResponseBody
    public ResponseEntity getFile() {
        File extractFile = FileNamesUtil.getLatestExtract(filesDirectory, extractName);

        if (extractFile.exists()) {
                FileSystemResource resource = new FileSystemResource(extractFile);

                HttpHeaders responseHeaders = new HttpHeaders();
                responseHeaders.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + extractFile.getName());
                responseHeaders.add(HttpHeaders.CONTENT_TYPE, "application/zip");
                responseHeaders.add(HttpHeaders.CONTENT_LENGTH, String.valueOf(extractFile.length()));

                log.info("download done");
                return new ResponseEntity<>(resource, responseHeaders, HttpStatus.OK);
        }
        else {
            log.error("download failed");
            return new ResponseEntity<>("File not found", HttpStatus.NOT_FOUND);
        }
    }

    @GetMapping(value="/download/test")
    @ResponseBody
    public ResponseEntity getDemoExtractFile() {
        File extractTestFile = new File(FileNamesUtil.getFilePath(filesDirectory, extractTestName));

        if (extractTestFile.exists()) {
            FileSystemResource resource = new FileSystemResource(extractTestFile);

            HttpHeaders responseHeaders = new HttpHeaders();
            responseHeaders.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + extractTestFile.getName());
            responseHeaders.add(HttpHeaders.CONTENT_TYPE, "application/zip");
            responseHeaders.add(HttpHeaders.CONTENT_LENGTH, String.valueOf(extractTestFile.length()));

            log.info("download done");
            return new ResponseEntity<>(resource, responseHeaders, HttpStatus.OK);
        }
        else {
            log.error("download failed");
            return new ResponseEntity<>("File not found", HttpStatus.NOT_FOUND);
        }

    }

    @PostMapping(value = "/clean-all", produces = MediaType.APPLICATION_JSON_VALUE)
    public String cleanAll()  {
        try {
            FileUtils.cleanDirectory(new File(filesDirectory));
            log.info("all files in {} were deleted!", filesDirectory);
            return "all files in storage were deleted";
        } catch (IOException e) {
            log.error("cleaning directory failed", e);
            return "cleaning directory failed";
        }

    }

    @PostMapping(value = "/gc")
    public void forceGC() {
        log.info("calling GC...");
        System.gc();
        log.info("gc has been called");
    }
}
