package fr.ans.psc.pscextract.controller;

import fr.ans.psc.ApiClient;
import fr.ans.psc.api.PsApi;
import fr.ans.psc.model.Ps;
import fr.ans.psc.pscextract.service.AggregationService;
import fr.ans.psc.pscextract.service.EmailService;
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
import org.springframework.util.StreamUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClientException;
import org.springframework.web.context.request.async.DeferredResult;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@RestController
public class ExtractionController {

    private final String ZIP_EXTENSION = ".zip";
    private final String TXT_EXTENSION = ".txt";

    private String extractTime ="197001010001";

    @Value("${working.directory}")
    private String workingDirectory;

    PsApi psApi;

    @Value("${api.base.url}")
    private String apiBaseUrl;

    @Autowired
    ExportService exportService;

    @Autowired
    AggregationService aggregationService;

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
                log.info("Transformation done.");
                output.setResult(ResponseEntity.ok("Transformation done."));
            } catch (IOException e) {
                log.error("Error during transformation", e);
                log.error(e.getMessage());
            }
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

                File latestExtract = FileNamesUtil.getLatestExtract(filesDirectory, extractName);
                emailService.sendSimpleMessage("PSCEXTRACT - sécurisation effectuée", latestExtract);
            } catch (IOException | InterruptedException e) {
                log.error("Exception raised :", e);
            }
        });
    }

    @GetMapping(value = "/download")
    @ResponseBody
    public ResponseEntity getFile() {
        File extractFile = FileNamesUtil.getLatestExtract(filesDirectory, extractName);

        if (extractFile != null) {
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

    @GetMapping(value = "/extract-download")
    @ResponseBody
    public ResponseEntity generateExtractAndGetFile() throws IOException {

        ApiClient apiClient = new ApiClient();
        apiClient.setBasePath(apiBaseUrl);
        this.psApi = new PsApi(apiClient);
        log.info("Api client with url "+apiBaseUrl+" created");

        File tempExtractFile = File.createTempFile("tempExtract", "tmp");
        BufferedWriter bw = Files.newBufferedWriter(tempExtractFile.toPath(), StandardCharsets.UTF_8);
        log.info("BufferedWriter initialized");

        String header = "Type d'identifiant PP|Identifiant PP|Identification nationale PP|Nom de famille|Prénoms|" +
        "Date de naissance|Code commune de naissance|Code pays de naissance|Lieu de naissance|Code sexe|" +
        "Téléphone (coord. correspondance)|Adresse e-mail (coord. correspondance)|Code civilité|Code profession|" +
        "Code catégorie professionnelle|Code civilité d'exercice|Nom d'exercice|Prénom d'exercice|" +
        "Code type savoir-faire|Code savoir-faire|Code mode exercice|Code secteur d'activité|" +
        "Code section tableau pharmaciens|Code rôle|Numéro SIRET site|Numéro SIREN site|Numéro FINESS site|" +
        "Numéro FINESS établissement juridique|Identifiant technique de la structure|Raison sociale site|" +
        "Enseigne commerciale site|Complément destinataire (coord. structure)|" +
        "Complément point géographique (coord. structure)|Numéro Voie (coord. structure)|" +
        "Indice répétition voie (coord. structure)|Code type de voie (coord. structure)|" +
        "Libellé Voie (coord. structure)|Mention distribution (coord. structure)|" +
        "Bureau cedex (coord. structure)|Code postal (coord. structure)|Code commune (coord. structure)|" +
        "Code pays (coord. structure)|Téléphone (coord. structure)|Téléphone 2 (coord. structure)|" +
        "Télécopie (coord. structure)|Adresse e-mail (coord. structure)|Code département (coord. structure)|" +
        "Ancien identifiant de la structure|Autorité d'enregistrement|Autres identifiants|Code genre d'activité|\n";
        bw.write(header);
        log.info("Header written");


        transformationService.setExtractionTime(this);

        Integer page = 0;
        List<Ps> responsePsList;
        List<Ps> tempPsList;

        log.info("Starting extraction at "+apiBaseUrl);

        try {
            BigDecimal size = BigDecimal.valueOf(pageSize);
            List<Ps> response = psApi.getPsByPage(BigDecimal.valueOf(page), size);
            log.info("Page "+page+" of size "+size+" received");
            Boolean outOfPages = false;

            do {
                responsePsList = response;
                tempPsList = transformationService.unwind(responsePsList);

                for (Ps ps : tempPsList) {
                    bw.write(transformationService.transformPsToLine(ps, this));
                    log.info("Ps "+ps.getId()+" transformed and written");
                }
                page++;
                try {
                    response =psApi.getPsByPage(BigDecimal.valueOf(page),size);
                    log.info("Page "+page+" of size"+size+" received");
                }catch( RestClientException e ){
                    log.warn("Out of pages: "+e.getMessage());
                    outOfPages = true;
                }
                } while ( !outOfPages );
        }catch (RestClientException e) {
            log.error("No pages found :", e);
            return new ResponseEntity<>(null, null, HttpStatus.NO_CONTENT);
        }
        finally {
            bw.close();
            log.info("BufferedWriter closed");
        }


        InputStream fileContent = new FileInputStream(tempExtractFile);
        log.info("File content read");

        ZipEntry zipEntry = new ZipEntry(transformationService.getFileNameWithExtension(TXT_EXTENSION, this));
        zipEntry.setTime(System.currentTimeMillis());
        ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(FileNamesUtil.getFilePath(workingDirectory, transformationService.getFileNameWithExtension(ZIP_EXTENSION, this))));
        zos.putNextEntry(zipEntry);
        StreamUtils.copy(fileContent, zos);

        fileContent.close();
        zos.closeEntry();
        zos.finish();
        zos.close();


        tempExtractFile.delete();
        log.info("Temp file at "+tempExtractFile.getAbsolutePath()+" deleted");

        Files.move(Path.of(FileNamesUtil.getFilePath(workingDirectory, transformationService.getFileNameWithExtension(ZIP_EXTENSION, this))),
                Path.of(FileNamesUtil.getFilePath(filesDirectory, transformationService.getFileNameWithExtension(ZIP_EXTENSION, this))));
        log.info("File at "+FileNamesUtil.getFilePath(workingDirectory, transformationService.getFileNameWithExtension(ZIP_EXTENSION, this))+" moved to "+FileNamesUtil.getFilePath(filesDirectory, transformationService.getFileNameWithExtension(ZIP_EXTENSION, this)));

        File extractFile = FileNamesUtil.getLatestExtract(filesDirectory, extractName);
        log.info("File "+extractFile.getName()+" created");

        FileSystemResource resource = new FileSystemResource(extractFile);

        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + extractFile.getName());
        responseHeaders.add(HttpHeaders.CONTENT_TYPE, "application/zip");
        responseHeaders.add(HttpHeaders.CONTENT_LENGTH, String.valueOf(extractFile.length()));


        return new ResponseEntity<>(resource, responseHeaders, HttpStatus.OK);
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
