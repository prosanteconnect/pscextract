package fr.ans.psc.pscextract.controller;

import fr.ans.psc.pscextract.service.ExtractionService;
import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RestController
public class ExtractionController {

    @Autowired
    ExtractionService extractionService;

    @Value("${files.directory}")
    private String filesDirectory;

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
        ForkJoinPool.commonPool().submit(() -> extractionService.aggregate());
        return "aggregating...";
    }

    @PostMapping(value = "/extract")
    public String extract() {
        ForkJoinPool.commonPool().submit(() -> {
            try {
                extractionService.extract();
            } catch (IOException e) {
                log.error(e.getMessage());
            }
        });
        return "extracting...";
    }

    @PostMapping(value = "/transform")
    public String transform() {
        ForkJoinPool.commonPool().submit(() -> {
            try {
                extractionService.transformCsv();
            } catch (IOException e) {
                log.error(e.getMessage());
            }
        });
        return "transforming...";
    }

    @GetMapping(value = "/download")
    public void getFile(HttpServletResponse response) throws IOException {
        response.setContentType("application/zip");
        response.setHeader("Content-Disposition", "attachment; filename=" + extractionService.zipName());
        extractionService.zipFile(response.getOutputStream());
    }

    @PostMapping(value = "/clean-all", produces = MediaType.APPLICATION_JSON_VALUE)
    public String cleanAll() throws IOException {
        FileUtils.cleanDirectory(new File(filesDirectory));
        log.info("all files in {} were deleted!", filesDirectory);
        return "all files in storage were deleted";
    }

}
