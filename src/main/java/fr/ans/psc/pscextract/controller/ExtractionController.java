package fr.ans.psc.pscextract.controller;

import fr.ans.psc.ApiClient;
import fr.ans.psc.api.PsApi;
import fr.ans.psc.pscextract.service.EmailService;
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

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RestController
public class ExtractionController {

    @Value("${working.directory}")
  private String workingDirectory;

  PsApi psApi;

  @Value("${api.base.url}")
  private String apiBaseUrl;

  @Autowired
  TransformationService transformationService;

  @Autowired
  EmailService emailService;

  @Value("${files.directory}")
  private String filesDirectory;

  @Value("${page.size}")
  private Integer pageSize;

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

  @GetMapping(value = "/download")
  @ResponseBody
  public ResponseEntity<FileSystemResource> getFile() {
    File extractFile = FileNamesUtil.getLatestExtract(filesDirectory, extractName);

    if (extractFile != null) {
      FileSystemResource resource = new FileSystemResource(extractFile);

      HttpHeaders responseHeaders = new HttpHeaders();
      responseHeaders.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + extractFile.getName());
      responseHeaders.add(HttpHeaders.CONTENT_TYPE, "application/zip");
      responseHeaders.add(HttpHeaders.CONTENT_LENGTH, String.valueOf(extractFile.length()));

      log.info("download done");
      return new ResponseEntity<>(resource, responseHeaders, HttpStatus.OK);
    } else {
      log.error("download failed");
      return new ResponseEntity<>(null, null, HttpStatus.NOT_FOUND);
    }
  }

  @GetMapping(value = "/download/test")
  @ResponseBody
  public ResponseEntity<FileSystemResource> getDemoExtractFile() {
    File extractTestFile = new File(FileNamesUtil.getFilePath(filesDirectory, extractTestName));

    if (extractTestFile.exists()) {
      FileSystemResource resource = new FileSystemResource(extractTestFile);

      HttpHeaders responseHeaders = new HttpHeaders();
      responseHeaders.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + extractTestFile.getName());
      responseHeaders.add(HttpHeaders.CONTENT_TYPE, "application/zip");
      responseHeaders.add(HttpHeaders.CONTENT_LENGTH, String.valueOf(extractTestFile.length()));

      log.info("download done");
      return new ResponseEntity<>(resource, responseHeaders, HttpStatus.OK);
    } else {
      log.error("download failed");
      return new ResponseEntity<>(null, null, HttpStatus.NOT_FOUND);
    }

  }

  @PostMapping(value = "/generate-extract")
  public void generateExtract() {

    try {
      instantiateApi();
      File latestExtract = transformationService.extractToCsv(this);
      FileNamesUtil.cleanup(filesDirectory, extractTestName);

      if(latestExtract != null)
      emailService.sendSimpleMessage("PSCEXTRACT - sécurisation effectuée", latestExtract);
      else
        emailService.sendSimpleMessage("PSCEXTRACT - sécurisation échouée", null);
    } catch (IOException e) {
      log.error("Exception raised :", e);
    }
  }

  private void instantiateApi() {
    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath(apiBaseUrl);
    this.psApi = new PsApi(apiClient);
    log.info("Api client with url " + apiBaseUrl + " created");
  }

  @PostMapping(value = "/clean-all", produces = MediaType.APPLICATION_JSON_VALUE)
  public String cleanAll() {
    try {
      FileUtils.cleanDirectory(new File(filesDirectory));
      log.info("all files in {} were deleted!", filesDirectory);
      return "all files in storage were deleted";
    } catch (IOException e) {
      log.error("cleaning directory failed", e);
      return "cleaning directory failed";
    }

  }

  public String getZIP_EXTENSION() {
    return ".zip";
  }

  public String getTXT_EXTENSION() {
    return ".txt";
  }

  public String getWorkingDirectory() {
    return workingDirectory;
  }

  public PsApi getPsApi() {
    return psApi;
  }

  public String getApiBaseUrl() {
    return apiBaseUrl;
  }

  public String getFilesDirectory() {
    return filesDirectory;
  }

  public Integer getPageSize() {
    return pageSize;
  }

}
