package fr.ans.psc.pscextract.controller;

import fr.ans.psc.pscextract.service.AggregationService;
import fr.ans.psc.pscextract.service.DownloadExtractService;
import fr.ans.psc.pscextract.service.ExportService;
import fr.ans.psc.pscextract.service.TransformationService;
import fr.ans.psc.pscextract.service.utils.FileNamesUtil;
import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.apache.tomcat.util.http.fileupload.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StreamUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;

import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

@RestController
public class ExtractionController {

    @Autowired
    ExportService exportService;

    @Autowired
    AggregationService aggregationService;

    @Autowired
    TransformationService transformationService;

    @Autowired
    DownloadExtractService downloadExtractService;

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

//    @GetMapping(value = "/download")
//    public void getFile(HttpServletResponse response) {

//        try {
//            response.setContentType("application/zip");
//            response.setHeader("Content-Disposition", "attachment; filename=" + downloadExtractService.zipName());
//            downloadExtractService.zipFile(response.getOutputStream(), true);
//            log.info("Download done");
//            FileNamesUtil.cleanup(filesDirectory, extractTestName);
//        } catch (IOException e) {
//            log.error("download failed", e);
//            response.setStatus(500);
//        }
//    }

    @GetMapping(value = "/download")
    @ResponseBody
    public ResponseEntity getFile() {
        File extractFile = FileNamesUtil.getLatestExtract(filesDirectory, extractName);

        if (extractFile.exists()) {
            try {
                ByteArrayResource resource = new ByteArrayResource(Files.readAllBytes(Paths.get(extractFile.getAbsolutePath())));

                HttpHeaders responseHeaders = new HttpHeaders();
                responseHeaders.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + extractFile.getName());
                responseHeaders.add(HttpHeaders.CONTENT_TYPE, "application/zip");
                responseHeaders.add(HttpHeaders.CONTENT_LENGTH, String.valueOf(extractFile.length()));
                return new ResponseEntity<>(resource, responseHeaders, HttpStatus.OK);

            } catch (IOException e) {
                log.error("could not attach zip file", e);
                return new ResponseEntity<>("could not attach zip file", HttpStatus.INTERNAL_SERVER_ERROR);
            }
        }
        else {
            return new ResponseEntity<>("File not found", HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping(value="/download/test")
    public void getDemoExtractFile(HttpServletResponse response) {
        response.setContentType("application/zip");
        response.setHeader("Content-Disposition", "attachment; filename=" + extractTestName + ".zip");
        try {
            downloadExtractService.zipFile(response.getOutputStream(), false);
            log.info("Download demo file done");
            FileNamesUtil.cleanup(filesDirectory, extractTestName);
        } catch (IOException e) {
            log.error("download failed", e);
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

}
