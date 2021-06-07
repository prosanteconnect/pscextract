package fr.ans.psc.pscextract.service;

import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.ObjectRowProcessor;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import fr.ans.psc.pscextract.model.PsLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.OutOperation;
import org.springframework.data.mongodb.core.aggregation.ProjectionOperation;
import org.springframework.stereotype.Service;
import org.springframework.util.StreamUtils;

import java.io.*;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

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

    @Value("${mongodb.inCollection}")
    private String inCollection;

    @Value("${mongodb.addr}")
    private String mongoAddr;

    @Value("${files.directory}")
    private String filesDirectory;

    @Value("${extract.name}")
    private String extractName;

    private String aggregationDate="197001010001";

    /**
     * Instantiates a new Extraction service.
     */
    private ExtractionService() {
    }

    /**
     * Aggregate.
     */
    public void aggregate() {
        AggregationOperation unwindProfessions = Aggregation.unwind("professions");
        AggregationOperation unwindExpertises = Aggregation.unwind("professions.expertises");
        AggregationOperation unwindWorkSituations = Aggregation.unwind("professions.workSituations");
        AggregationOperation unwindStructureId = Aggregation.unwind("professions.workSituations.structures");
        AggregationOperation unwindThisStructure = Aggregation.unwind("thisStructure");
        AggregationOperation lookup = Aggregation.lookup("structure",
                "professions.workSituations.structures.structureId",
                "structureTechnicalId",
                "thisStructure");

        ProjectionOperation projection = Aggregation.project()
                .andExclude("_id")
                .and("idType").as("idType")
                .and("id").as("id")
                .and("nationalId").as("nationalId")
                .and("lastName").as("lastName")
                .and("firstName").as("firstName")
                .and("dateOfBirth").as("dateOfBirth")
                .and("birthAddressCode").as("birthAddressCode")
                .and("birthCountryCode").as("birthCountryCode")
                .and("birthAddress").as("birthAddress")
                .and("genderCode").as("genderCode")
                .and("phone").as("phone")
                .and("email").as("email")
                .and("salutationCode").as("salutationCode")
                .and("professions.code").as("profession_code")
                .and("professions.categoryCode").as("profession_categoryCode")
                .and("professions.salutationCode").as("profession_salutationCode")
                .and("professions.lastName").as("profession_lastName")
                .and("professions.firstName").as("profession_firstName")
                .and("professions.expertises.typeCode").as("profession_expertise_typeCode")
                .and("professions.expertises.code").as("profession_expertise_code")
                .and("professions.workSituations.modeCode").as("profession_workSituation_modeCode")
                .and("professions.workSituations.activitySectorCode").as("profession_workSituation_activitySectorCode")
                .and("professions.workSituations.pharmacistTableSectionCode").as("profession_workSituation_pharmacistTableSectionCode")
                .and("professions.workSituations.roleCode").as("profession_workSituation_roleCode")
                .and("thisStructure.siteSIRET").as("structure_siteSIRET")
                .and("thisStructure.siteSIREN").as("structure_siteSIREN")
                .and("thisStructure.siteFINESS").as("structure_siteFINESS")
                .and("thisStructure.legalEstablishmentFINESS").as("structure_legalEstablishmentFINESS")
                .and("thisStructure.structureTechnicalId").as("structure_structureTechnicalId")
                .and("thisStructure.legalCommercialName").as("structure_legalCommercialName")
                .and("thisStructure.publicCommercialName").as("structure_publicCommercialName")
                .and("thisStructure.recipientAdditionalInfo").as("structure_recipientAdditionalInfo")
                .and("thisStructure.geoLocationAdditionalInfo").as("structure_geoLocationAdditionalInfo")
                .and("thisStructure.streetNumber").as("structure_streetNumber")
                .and("thisStructure.streetNumberRepetitionIndex").as("structure_streetNumberRepetitionIndex")
                .and("thisStructure.streetCategoryCode").as("structure_streetCategoryCode")
                .and("thisStructure.streetLabel").as("structure_streetLabel")
                .and("thisStructure.distributionMention").as("structure_distributionMention")
                .and("thisStructure.cedexOffice").as("structure_cedexOffice")
                .and("thisStructure.postalCode").as("structure_postalCode")
                .and("thisStructure.communeCode").as("structure_communeCode")
                .and("thisStructure.countryCode").as("structure_countryCode")
                .and("thisStructure.phone").as("structure_phone")
                .and("thisStructure.phone2").as("structure_phone2")
                .and("thisStructure.fax").as("structure_fax")
                .and("thisStructure.email").as("structure_email")
                .and("thisStructure.departmentCode").as("structure_departmentCode")
                .and("thisStructure.oldStructureId").as("structure_oldStructureId")
                .and("thisStructure.registrationAuthority").as("structure_registrationAuthority");

        OutOperation out = Aggregation.out(outCollection);

        Aggregation aggregation = Aggregation.newAggregation(unwindProfessions,
                unwindExpertises, unwindWorkSituations, unwindStructureId, lookup, unwindThisStructure, projection, out);

        mongoTemplate.aggregate(aggregation, inCollection, PsLine.class);

        setAggregationDate();
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
                "--out=" + getFilePath(extractName) + " " +
                "--type=csv " +
                "--forceTableScan";

        log.info("running command : {}", cmd);
        log.info("exporting schema {}", outCollection);

        Runtime.getRuntime().exec(cmd);

        log.info("export done");
    }

    /**
     * Transform csv.
     *
     * @throws IOException the io exception
     */
    public void transformCsv() throws IOException {
        log.info("starting file transformation");
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
                "Télécopie (coord. structure)|Adresse e-mail (coord. structure)|Code Département (structure)|" +
                "Ancien identifiant de la structure|Autorité d'enregistrement|";
        Files.write(Paths.get(getFilePath(extractRASS())), Collections.singleton(header),
                StandardCharsets.UTF_8);

        FileWriter f = new FileWriter(getFilePath(extractRASS()), true);
        BufferedWriter b = new BufferedWriter(f);
        PrintWriter p = new PrintWriter(b);

        // ObjectRowProcessor converts the parsed values and gives you the resulting row.
        ObjectRowProcessor rowProcessor = new ObjectRowProcessor() {
            @Override
            public void rowProcessed(Object[] objects, ParsingContext parsingContext) {
                String line = String.join("|", Arrays.asList(objects).toArray(new String[objects.length])) + "|";
                p.println(line);
            }

        };

        CsvParserSettings parserSettings = new CsvParserSettings();
        parserSettings.getFormat().setLineSeparator("\n");
        parserSettings.getFormat().setDelimiter(',');
        parserSettings.setProcessor(rowProcessor);
        parserSettings.setHeaderExtractionEnabled(true);
        parserSettings.setNullValue("");

        CsvParser parser = new CsvParser(parserSettings);
        parser.parse(new BufferedReader(new FileReader(getFilePath(extractName))));
        p.close();b.close();f.close();
        log.info("transformation complete!");
    }

    /**
     * Zip file.
     *
     * @param out the OutputStream
     */
    public void zipFile(OutputStream out) {
        FileSystemResource resource = new FileSystemResource(getFilePath(extractRASS()));
        try (ZipOutputStream zippedOut = new ZipOutputStream(out)) {
            ZipEntry e = new ZipEntry(Objects.requireNonNull(resource.getFilename()));
            // Configure the zip entry, the properties of the file
            e.setSize(resource.contentLength());
            e.setTime(System.currentTimeMillis());
            // etc.
            zippedOut.putNextEntry(e);
            // And the content of the resource:
            StreamUtils.copy(resource.getInputStream(), zippedOut);
            zippedOut.closeEntry();
            zippedOut.finish();
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    /**
     * Extract rass string.
     *
     * @return the string
     */
    public String extractRASS() {
        return extractName + "_" + aggregationDate + ".txt";
    }

    /**
     * Zip name string.
     *
     * @return the string
     */
    public String zipName() {
        return extractName + "_" + aggregationDate + ".zip";
    }

    private String getFilePath(String fileName) {
        if ("".equals(filesDirectory)) {
            return filesDirectory + fileName;
        } else {
            return filesDirectory + '/' + fileName;
        }
    }

    private void setAggregationDate() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMddHHmm");
        LocalDateTime now = LocalDateTime.now();
        this.aggregationDate = dtf.format(now);
    }

}
