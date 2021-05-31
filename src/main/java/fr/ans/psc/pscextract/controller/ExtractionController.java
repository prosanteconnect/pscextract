package fr.ans.psc.pscextract.controller;

import fr.ans.psc.pscextract.service.ExtractionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@RestController
public class ExtractionController {

    @Autowired
    ExtractionService extractionService;

    @Value("${files.directory}")
    private String filesDirectory;

    @Value("${compressed.name}")
    private String compressedName;

    /**
     * logger.
     */
    private static final Logger log = LoggerFactory.getLogger(ExtractionController.class);

    @GetMapping(value = "/check", produces = MediaType.APPLICATION_JSON_VALUE)
    public String index() {
        return "alive";
    }

    @GetMapping(value = "/download")
    public void getFile(HttpServletResponse response) throws IOException {
        response.setContentType("application/zip");
        response.setHeader("Content-Disposition", "attachment; filename=" + compressedName);
        extractionService.zipFile(response.getOutputStream());
    }

    @PostMapping(value = "/extract")
    public void aggregate() throws IOException {
        extractionService.aggregate();
        extractionService.extract();
    }

}
