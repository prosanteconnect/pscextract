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

    /**
     * The Mongo template.
     */
    @Autowired
    MongoTemplate mongoTemplate;

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

    @Value("${output.name}")
    private String outputName;

    /**
     * Instantiates a new Extraction service.
     *
     * @param mongoTemplate the mongo template
     */
    public ExtractionService(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    /**
     * Instantiates a new Extraction service.
     */
    public ExtractionService() {
    }

    /**
     * Aggregate.
     *
     */
    public void aggregate() {
        AggregationOperation unwindProfessions = Aggregation.unwind("professions");
        AggregationOperation unwindExpertises = Aggregation.unwind("professions.expertises");
        AggregationOperation unwindWorkSituations = Aggregation.unwind("professions.workSituations");

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
                .and("professions.expertises.categoryCode").as("profession_expertise_categoryCode")
                .and("professions.expertises.code").as("profession_expertise_code")
                .and("professions.workSituations.modeCode").as("profession_workSituation_modeCode")
                .and("professions.workSituations.activitySectorCode").as("profession_workSituation_activitySectorCode")
                .and("professions.workSituations.pharmacistTableSectionCode").as("profession_workSituation_pharmacistTableSectionCode")
                .and("professions.workSituations.roleCode").as("profession_workSituation_roleCode")
                .and("professions.workSituations.structure.siteSIRET").as("structure_siteSIRET")
                .and("professions.workSituations.structure.siteSIREN").as("structure_siteSIREN")
                .and("professions.workSituations.structure.siteFINESS").as("structure_siteFINESS")
                .and("professions.workSituations.structure.legalEstablishmentFINESS").as("structure_legalEstablishmentFINESS")
                .and("professions.workSituations.structure.structureTechnicalId").as("structure_structureTechnicalId")
                .and("professions.workSituations.structure.legalCommercialName").as("structure_legalCommercialName")
                .and("professions.workSituations.structure.publicCommercialName").as("structure_publicCommercialName")
                .and("professions.workSituations.structure.recipientAdditionalInfo").as("structure_recipientAdditionalInfo")
                .and("professions.workSituations.structure.geoLocationAdditionalInfo").as("structure_geoLocationAdditionalInfo")
                .and("professions.workSituations.structure.streetNumber").as("structure_streetNumber")
                .and("professions.workSituations.structure.streetNumberRepetitionIndex").as("structure_streetNumberRepetitionIndex")
                .and("professions.workSituations.structure.streetCategoryCode").as("structure_streetCategoryCode")
                .and("professions.workSituations.structure.streetLabel").as("structure_streetLabel")
                .and("professions.workSituations.structure.distributionMention").as("structure_distributionMention")
                .and("professions.workSituations.structure.cedexOffice").as("structure_cedexOffice")
                .and("professions.workSituations.structure.postalCode").as("structure_postalCode")
                .and("professions.workSituations.structure.communeCode").as("structure_communeCode")
                .and("professions.workSituations.structure.countryCode").as("structure_countryCode")
                .and("professions.workSituations.structure.phone").as("structure_phone")
                .and("professions.workSituations.structure.phone2").as("structure_phone2")
                .and("professions.workSituations.structure.fax").as("structure_fax")
                .and("professions.workSituations.structure.email").as("structure_email")
                .and("professions.workSituations.structure.departmentCode").as("structure_departmentCode")
                .and("professions.workSituations.structure.oldStructureId").as("structure_oldStructureId")
                .and("professions.workSituations.structure.registrationAuthority").as("structure_registrationAuthority");

        OutOperation out = Aggregation.out(outCollection);

        Aggregation aggregation = Aggregation.newAggregation(unwindProfessions, unwindExpertises, unwindWorkSituations, projection, out);

        mongoTemplate.aggregate(aggregation, inCollection, PsLine.class);
    }

    public void extract() throws IOException {
        List<String> fieldsList = Arrays.stream(PsLine.class.getDeclaredFields()).map(Field::getName).collect(Collectors.toList());

        String fields = String.join(",", fieldsList);

        String cmd = "mongoexport " +
                "--db=" + mongoTemplate.getDb().getName() + " " +
                "--collection=" + outCollection + " " +
                "--host=" + mongoAddr + " " +
                "--fields=" + fields + " " +
                "--type=csv " +
                "--out=" + getFilePath(extractName) +
                "--forceTableScan";

        log.info("running command : {}", cmd);
        log.info("exporting schema {}", outCollection);

        Runtime.getRuntime().exec(cmd);

        log.info("export done");
    }

    private String getFilePath(String fileName) {
        if ("".equals(filesDirectory)) {
            return filesDirectory + fileName;
        } else {
            return filesDirectory + '/' + fileName;
        }
    }

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
        Files.write(Paths.get(getFilePath(outputName)), Collections.singleton(header),
                StandardCharsets.UTF_8);

        FileWriter f = new FileWriter(getFilePath(outputName), true);
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

    public void zipFile(OutputStream out) {
        FileSystemResource resource = new FileSystemResource(getFilePath(outputName));
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

    public void cleanup() {
        File outputFile = new File(getFilePath(outputName));
        File extractFile = new File(getFilePath(extractName));
        outputFile.delete();
        extractFile.delete();
    }

}
