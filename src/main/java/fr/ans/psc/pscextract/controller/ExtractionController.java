package fr.ans.psc.pscextract.controller;

import fr.ans.psc.ApiClient;
import fr.ans.psc.api.PsApi;
import fr.ans.psc.model.Expertise;
import fr.ans.psc.model.FirstName;
import fr.ans.psc.model.Profession;
import fr.ans.psc.model.Ps;
import fr.ans.psc.model.Structure;
import fr.ans.psc.model.WorkSituation;
import fr.ans.psc.pscextract.service.AggregationService;
import fr.ans.psc.pscextract.service.EmailService;
import fr.ans.psc.pscextract.service.ExportService;
import fr.ans.psc.pscextract.service.TransformationService;
import fr.ans.psc.pscextract.service.utils.CloneUtil;
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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
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
        log.info("api client created");

        File tempExtractFile = File.createTempFile("tempExtract", "tmp");
        BufferedWriter bw = Files.newBufferedWriter(tempExtractFile.toPath(), StandardCharsets.UTF_8);
        log.info("temp extract file created");

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
        "Ancien identifiant de la structure|Autorité d'enregistrement|Autres identifiants|\n";
        bw.write(header);
        log.info("header written");


        setExtractionTime();

        Integer page = 0;
        List<Ps> tempPsList;

        log.info("starting extraction on "+apiBaseUrl);

        try {
            ResponseEntity<List<Ps>> response = psApi.getPsListByPageWithHttpInfo(page,null);
            log.info("response received");
            Boolean outOfPages = false;

            do {
                tempPsList = response.getBody();
                tempPsList = unwind(tempPsList);

                for (Ps ps : tempPsList) {
                    bw.write(transformPsToLine(ps));
                }
                page++;
                try {
                    psApi.getPsListByPageWithHttpInfo(page,null);
                }catch( RestClientException e ){
                    log.warn("Out of pages: "+e.getMessage());
                    outOfPages = true;
                }
                } while ( !outOfPages );
        }catch (RestClientException e) {
            log.error("No pages :", e);
        }
        finally {
            bw.close();
        }


        InputStream fileContent = new FileInputStream(tempExtractFile);
        log.info("File content read");

        ZipEntry zipEntry = new ZipEntry(getFileNameWithExtension(TXT_EXTENSION));
        zipEntry.setTime(System.currentTimeMillis());
        ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(FileNamesUtil.getFilePath(workingDirectory, getFileNameWithExtension(ZIP_EXTENSION))));
        zos.putNextEntry(zipEntry);
        StreamUtils.copy(fileContent, zos);

        fileContent.close();
        zos.closeEntry();
        zos.finish();
        zos.close();

        tempExtractFile.delete();

        Files.move(Path.of(FileNamesUtil.getFilePath(workingDirectory, getFileNameWithExtension(ZIP_EXTENSION))),
                Path.of(FileNamesUtil.getFilePath(filesDirectory, getFileNameWithExtension(ZIP_EXTENSION))));

        File extractFile = FileNamesUtil.getLatestExtract(filesDirectory, extractName);

        FileSystemResource resource = new FileSystemResource(extractFile);

        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + tempExtractFile.getName());
        responseHeaders.add(HttpHeaders.CONTENT_TYPE, "application/zip");
        responseHeaders.add(HttpHeaders.CONTENT_LENGTH, String.valueOf(tempExtractFile.length()));

        return new ResponseEntity<>(resource, responseHeaders, HttpStatus.OK);
    }

    private ArrayList<Ps> unwind(List<Ps> psList){
        ArrayList<Ps> unwoundPsList = new ArrayList<>();
        Ps tempPs;
        for(Ps ps : psList){
            if (ps.getDeactivated()==null || ps.getActivated() > ps.getDeactivated()) {
                for (Profession profession : ps.getProfessions()) {
                    for (Expertise expertise : profession.getExpertises()) {
                        for (WorkSituation workSituation : profession.getWorkSituations()) {
                            tempPs = CloneUtil.clonePs(ps,profession,expertise,workSituation);
                            unwoundPsList.add(tempPs);
                        }
                    }
                }
            }
        }
        return unwoundPsList;
    }


    private String transformPsToLine(Ps ps) {
        Profession profession = ps.getProfessions().get(0);
        Expertise expertise = profession.getExpertises().get(0);
        WorkSituation workSituation = profession.getWorkSituations().get(0);
        Structure structure = workSituation.getStructure();

        StringBuilder sb = new StringBuilder();
        sb.append(ps.getIdType()).append("|");
        sb.append(ps.getId()).append("|");
        sb.append(ps.getNationalId()).append("|");
        sb.append(ps.getLastName()).append("|");
        sb.append(transformFirstNamesToString(ps.getFirstNames())).append("|");
        sb.append(ps.getDateOfBirth()).append("|");
        sb.append(ps.getBirthAddressCode()).append("|");
        sb.append(ps.getBirthCountryCode()).append("|");
        sb.append(ps.getBirthAddress()).append("|");
        sb.append(ps.getGenderCode()).append("|");
        sb.append(ps.getPhone()).append("|");
        sb.append(ps.getEmail()).append("|");
        sb.append(ps.getSalutationCode()).append("|");
        sb.append(profession.getCode()).append("|");
        sb.append(profession.getCategoryCode()).append("|");
        sb.append(profession.getSalutationCode()).append("|");
        sb.append(profession.getLastName()).append("|");
        sb.append(profession.getFirstName()).append("|");
        sb.append(expertise.getTypeCode()).append("|");
        sb.append(expertise.getCode()).append("|");
        sb.append(workSituation.getModeCode()).append("|");
        sb.append(workSituation.getActivitySectorCode()).append("|");
        sb.append(workSituation.getPharmacistTableSectionCode()).append("|");
        sb.append(workSituation.getRoleCode()).append("|");
        sb.append(structure.getSiteSIRET()).append("|");
        sb.append(structure.getSiteSIREN()).append("|");
        sb.append(structure.getSiteFINESS()).append("|");
        sb.append(structure.getLegalEstablishmentFINESS()).append("|");
        sb.append(structure.getStructureTechnicalId()).append("|");
        sb.append(structure.getLegalCommercialName()).append("|");
        sb.append(structure.getPublicCommercialName()).append("|");
        sb.append(structure.getRecipientAdditionalInfo()).append("|");
        sb.append(structure.getGeoLocationAdditionalInfo()).append("|");
        sb.append(structure.getStreetNumber()).append("|");
        sb.append(structure.getStreetNumberRepetitionIndex()).append("|");
        sb.append(structure.getStreetCategoryCode()).append("|");
        sb.append(structure.getStreetLabel()).append("|");
        sb.append(structure.getDistributionMention()).append("|");
        sb.append(structure.getCedexOffice()).append("|");
        sb.append(structure.getPostalCode()).append("|");
        sb.append(structure.getCommuneCode()).append("|");
        sb.append(structure.getCountryCode()).append("|");
        sb.append(structure.getPhone()).append("|");
        sb.append(structure.getPhone2()).append("|");
        sb.append(structure.getFax()).append("|");
        sb.append(structure.getEmail()).append("|");
        sb.append(structure.getDepartmentCode()).append("|");
        sb.append(structure.getOldStructureId()).append("|");
        sb.append(workSituation.getRegistrationAuthority()).append("|");
        sb.append(workSituation.getActivityKindCode()).append("|");
        sb.append(transformIdsToString(ps.getIds())).append("|");

        sb.append("\n");
        return sb.toString();
    }

    public String getFileNameWithExtension(String fileExtension) {
        return extractName + "_" + extractTime + fileExtension;
    }

    private String transformFirstNamesToString(List<FirstName> firstNames){
        firstNames.sort(Comparator.comparing(FirstName::getOrder));
        StringBuilder sb = new StringBuilder();
        for(FirstName firstName : firstNames){
            sb.append(firstName.getFirstName()).append(" ");
        }
        return sb.toString();
    }

    private String transformIdsToString(List<String> ids) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ids.size(); i++) {
            sb.append(getLinkString(ids.get(i)));
            if (i != ids.size() - 1) {
                sb.append(";");
            }
        }
        return sb.toString();
    }

    private String getLinkString(String id) {
        switch (id.charAt(0)) {
            case ('1'):
                // if (s.charAt(1) == '0') return s+','+"MSSante"+','+'1';
                return id + ',' + "ADELI" + ',' + '1';
            case ('3'):
                return id + ',' + "FINESS" + ',' + '1';
            case ('4'):
                return id + ',' + "SIREN" + ',' + '1';
            case ('5'):
                return id + ',' + "SIRET" + ',' + '1';
            case ('6'):
            case ('8'):
                return id + ',' + "RPPS" + ',' + '1';
            default:
                return id + ',' + "ADELI" + ',' + '1';
        }
    }

    private void setExtractionTime() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMddHHmm");
        LocalDateTime now = LocalDateTime.now();
        extractTime = dtf.format(now);
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
