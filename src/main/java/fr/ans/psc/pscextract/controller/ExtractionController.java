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
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
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

    @Value("${first.name.count}")
    private Integer firstNameCount;

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


        setExtractionTime();

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
                tempPsList = unwind(responsePsList);

                for (Ps ps : tempPsList) {
                    bw.write(transformPsToLine(ps));
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
            log.error("No pages :", e);
        }
        finally {
            bw.close();
            log.info("BufferedWriter closed");
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
        log.info("Temp file at "+tempExtractFile.getAbsolutePath()+" deleted");

        Files.move(Path.of(FileNamesUtil.getFilePath(workingDirectory, getFileNameWithExtension(ZIP_EXTENSION))),
                Path.of(FileNamesUtil.getFilePath(filesDirectory, getFileNameWithExtension(ZIP_EXTENSION))));
        log.info("File at "+FileNamesUtil.getFilePath(workingDirectory, getFileNameWithExtension(ZIP_EXTENSION))+" moved to "+FileNamesUtil.getFilePath(filesDirectory, getFileNameWithExtension(ZIP_EXTENSION)));

        File extractFile = FileNamesUtil.getLatestExtract(filesDirectory, extractName);
        log.info("File "+extractFile.getName()+" created");

        FileSystemResource resource = new FileSystemResource(extractFile);

        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + extractFile.getName());
        responseHeaders.add(HttpHeaders.CONTENT_TYPE, "application/zip");
        responseHeaders.add(HttpHeaders.CONTENT_LENGTH, String.valueOf(extractFile.length()));


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
        String activityCode = null;
        StringBuilder sb = new StringBuilder();
        sb.append(Optional.ofNullable(ps.getIdType()).orElse("")).append("|");
        sb.append(Optional.ofNullable(ps.getId()).orElse("")).append("|");
        sb.append(Optional.ofNullable(ps.getNationalId()).orElse("")).append("|");
        sb.append(Optional.ofNullable(ps.getLastName()).orElse("")).append("|");
        sb.append(Optional.ofNullable(transformFirstNamesToStringWithApostrophes(ps.getFirstNames())).orElse("''")).append("|");
        sb.append(Optional.ofNullable(ps.getDateOfBirth()).orElse("")).append("|");
        sb.append(Optional.ofNullable(ps.getBirthAddressCode()).orElse("")).append("|");
        sb.append(Optional.ofNullable(ps.getBirthCountryCode()).orElse("")).append("|");
        sb.append(Optional.ofNullable(ps.getBirthAddress()).orElse("")).append("|");
        sb.append(Optional.ofNullable(ps.getGenderCode()).orElse("")).append("|");
        sb.append(Optional.ofNullable(ps.getPhone()).orElse("")).append("|");
        sb.append(Optional.ofNullable(ps.getEmail()).orElse("")).append("|");
        sb.append(Optional.ofNullable(ps.getSalutationCode()).orElse("")).append("|");

        if(ps.getProfessions()!=null) {
            Profession profession = ps.getProfessions().get(0);
            sb.append(Optional.ofNullable(profession.getCode()).orElse("")).append("|");
            sb.append(Optional.ofNullable(profession.getCategoryCode()).orElse("")).append("|");
            sb.append(Optional.ofNullable(profession.getSalutationCode()).orElse("")).append("|");
            sb.append(Optional.ofNullable(profession.getLastName()).orElse("")).append("|");
            sb.append(Optional.ofNullable(profession.getFirstName()).orElse("")).append("|");

            if(profession.getExpertises()!=null) {
                Expertise expertise = profession.getExpertises().get(0);
                sb.append(Optional.ofNullable(expertise.getTypeCode()).orElse("")).append("|");
                sb.append(Optional.ofNullable(expertise.getCode()).orElse("")).append("|");
            }else{
                sb.append("|".repeat(2));
            }

            if(profession.getWorkSituations()!=null) {
                WorkSituation workSituation = profession.getWorkSituations().get(0);
                sb.append(Optional.ofNullable(workSituation.getModeCode()).orElse("")).append("|");
                sb.append(Optional.ofNullable(workSituation.getActivitySectorCode()).orElse("")).append("|");
                sb.append(Optional.ofNullable(workSituation.getPharmacistTableSectionCode()).orElse("")).append("|");
                sb.append(Optional.ofNullable(workSituation.getRoleCode()).orElse("")).append("|");

                if(workSituation.getStructure()!=null) {
                    Structure structure = workSituation.getStructure();
                    sb.append(Optional.ofNullable(structure.getSiteSIRET()).orElse("")).append("|");
                    sb.append(Optional.ofNullable(structure.getSiteSIREN()).orElse("")).append("|");
                    sb.append(Optional.ofNullable(structure.getSiteFINESS()).orElse("")).append("|");
                    sb.append(Optional.ofNullable(structure.getLegalEstablishmentFINESS()).orElse("")).append("|");
                    sb.append(Optional.ofNullable(structure.getStructureTechnicalId()).orElse("")).append("|");
                    sb.append(Optional.ofNullable(structure.getLegalCommercialName()).orElse("")).append("|");
                    sb.append(Optional.ofNullable(structure.getPublicCommercialName()).orElse("")).append("|");
                    sb.append(Optional.ofNullable(structure.getRecipientAdditionalInfo()).orElse("")).append("|");
                    sb.append(Optional.ofNullable(structure.getGeoLocationAdditionalInfo()).orElse("")).append("|");
                    sb.append(Optional.ofNullable(structure.getStreetNumber()).orElse("")).append("|");
                    sb.append(Optional.ofNullable(structure.getStreetNumberRepetitionIndex()).orElse("")).append("|");
                    sb.append(Optional.ofNullable(structure.getStreetCategoryCode()).orElse("")).append("|");
                    sb.append(Optional.ofNullable(structure.getStreetLabel()).orElse("")).append("|");
                    sb.append(Optional.ofNullable(structure.getDistributionMention()).orElse("")).append("|");
                    sb.append(Optional.ofNullable(structure.getCedexOffice()).orElse("")).append("|");
                    sb.append(Optional.ofNullable(structure.getPostalCode()).orElse("")).append("|");
                    sb.append(Optional.ofNullable(structure.getCommuneCode()).orElse("")).append("|");
                    sb.append(Optional.ofNullable(structure.getCountryCode()).orElse("")).append("|");
                    sb.append(Optional.ofNullable(structure.getPhone()).orElse("")).append("|");
                    sb.append(Optional.ofNullable(structure.getPhone2()).orElse("")).append("|");
                    sb.append(Optional.ofNullable(structure.getFax()).orElse("")).append("|");
                    sb.append(Optional.ofNullable(structure.getEmail()).orElse("")).append("|");
                    sb.append(Optional.ofNullable(structure.getDepartmentCode()).orElse("")).append("|");
                    sb.append(Optional.ofNullable(structure.getOldStructureId()).orElse("")).append("|");
                }else{
                    sb.append("|".repeat(24));
                }
                sb.append(Optional.ofNullable(workSituation.getRegistrationAuthority()).orElse("")).append("|");
                activityCode = (Optional.ofNullable(workSituation.getActivityKindCode()).orElse(null));

            }else{
                sb.append("|".repeat(29));
            }
        }else{
            sb.append("|".repeat(36));
        }
        sb.append(Optional.ofNullable(transformIdsToString(ps.getIds())).orElse("")).append("|");
        sb.append(Optional.ofNullable(activityCode).orElse("")).append("|");
        sb.append("\n");

        return sb.toString();
    }

    public String getFileNameWithExtension(String fileExtension) {
        return extractName + "_" + extractTime + fileExtension;
    }

    private String transformFirstNamesToStringWithApostrophes(List<FirstName> firstNames) {
        if (firstNames != null) {
            firstNames.sort(Comparator.comparing(FirstName::getOrder));
            StringBuilder sb = new StringBuilder();

            for (int i = 0; i < firstNameCount; i++) {
                if (i < firstNames.size()) sb.append(firstNames.get(i).getFirstName());
                sb.append("'");
            }

            sb.deleteCharAt(sb.length() - 1);
            return sb.toString();
        } else return null;
    }

    private String transformIdsToString(List<String> ids) {
        if(ids==null)
            return null;

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
