package fr.ans.psc.pscextract.service;

import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.ObjectRowProcessor;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import fr.ans.psc.pscextract.model.LinkToOtherIds;
import fr.ans.psc.pscextract.model.PsLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.*;
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
        aggregateLink();
        aggregatePsLines();
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
                "Ancien identifiant de la structure|Autorité d'enregistrement|Autres Ids|";
        Files.write(Paths.get(getFilePath(extractRASS())), Collections.singleton(header),
                StandardCharsets.UTF_8);

        FileWriter f = new FileWriter(getFilePath(extractRASS()), true);
        BufferedWriter b = new BufferedWriter(f);
        PrintWriter p = new PrintWriter(b);

        // ObjectRowProcessor converts the parsed values and gives you the resulting row.
        ObjectRowProcessor rowProcessor = new ObjectRowProcessor() {
            @Override
            public void rowProcessed(Object[] objects, ParsingContext parsingContext) {
                String line = String.join("|", getLineArray(objects)) + "|";
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

    private void aggregateLink() {
        String group = "{$group: {_id: '$nationalId', otherIdsArr: {$addToSet: '$nationalIdRef'}}}";
        String project = "{$project: {otherIds: {$reduce: {input: '$otherIdsArr', initialValue: '', in: {$concat: ['$$value', ' ', '$$this']}}}}}";
        OutOperation out = Aggregation.out("extractOtherIds");
        TypedAggregation<LinkToOtherIds> aggregation = Aggregation.newAggregation(
                LinkToOtherIds.class,
                new CustomAggregationOperation(group),
                new CustomAggregationOperation(project),
                out
        );
        mongoTemplate.aggregate(aggregation, inCollection, LinkToOtherIds.class);
    }

    private void aggregatePsLines() {
        String match = "{$match: {$expr: {$gt: ['$activated', '$deactivated']}}}";
        AggregationOperation lookupPs = Aggregation.lookup("ps", "nationalId", "nationalId", "thisPs");
        AggregationOperation unwindPs = Aggregation.unwind("thisPs");
        AggregationOperation unwindProfessions = Aggregation.unwind("thisPs.professions", true);
        AggregationOperation unwindExpertises = Aggregation.unwind("thisPs.professions.expertises", true);
        AggregationOperation unwindWorkSituations = Aggregation.unwind("thisPs.professions.workSituations", true);
        AggregationOperation unwindStructureId = Aggregation.unwind("thisPs.professions.workSituations.structures", true);
        AggregationOperation lookupStructure = Aggregation.lookup("structure",
                "thisPs.professions.workSituations.structures.structureId",
                "structureTechnicalId",
                "thisStructure");
        AggregationOperation unwindThisStructure = Aggregation.unwind("thisStructure", true);
        AggregationOperation lookupOtherIds = Aggregation.lookup("extractOtherIds", "nationalId", "_id", "thisOtherIds");
        AggregationOperation unwindOtherIds = Aggregation.unwind("thisOtherIds", true);

        ProjectionOperation projection = Aggregation.project()
                .andExclude("_id")
                .and("thisPs.idType").as("idType")
                .and("thisPs.id").as("id")
                .and("nationalIdRef").as("nationalId")
                .and("thisPs.lastName").as("lastName")
                .and("thisPs.firstName").as("firstName")
                .and("thisPs.dateOfBirth").as("dateOfBirth")
                .and("thisPs.birthAddressCode").as("birthAddressCode")
                .and("thisPs.birthCountryCode").as("birthCountryCode")
                .and("thisPs.birthAddress").as("birthAddress")
                .and("thisPs.genderCode").as("genderCode")
                .and("thisPs.phone").as("phone")
                .and("thisPs.email").as("email")
                .and("thisPs.salutationCode").as("salutationCode")
                .and("thisPs.professions.code").as("profession_code")
                .and("thisPs.professions.categoryCode").as("profession_categoryCode")
                .and("thisPs.professions.salutationCode").as("profession_salutationCode")
                .and("thisPs.professions.lastName").as("profession_lastName")
                .and("thisPs.professions.firstName").as("profession_firstName")
                .and("thisPs.professions.expertises.typeCode").as("profession_expertise_typeCode")
                .and("thisPs.professions.expertises.code").as("profession_expertise_code")
                .and("thisPs.professions.workSituations.modeCode").as("profession_workSituation_modeCode")
                .and("thisPs.professions.workSituations.activitySectorCode").as("profession_workSituation_activitySectorCode")
                .and("thisPs.professions.workSituations.pharmacistTableSectionCode").as("profession_workSituation_pharmacistTableSectionCode")
                .and("thisPs.professions.workSituations.roleCode").as("profession_workSituation_roleCode")
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
                .and("thisStructure.registrationAuthority").as("structure_registrationAuthority")
                .and("thisOtherIds.otherIds").as("otherIds");

        OutOperation out = Aggregation.out(outCollection);

        TypedAggregation<PsLine> aggregation = Aggregation.newAggregation(
                PsLine.class,
                new CustomAggregationOperation(match),
                lookupPs, unwindPs, unwindProfessions, unwindExpertises, unwindWorkSituations, unwindStructureId,
                lookupStructure, unwindThisStructure,
                lookupOtherIds, unwindOtherIds,
                projection,
                out
        );
        mongoTemplate.aggregate(aggregation, inCollection, PsLine.class);
    }

    private void setAggregationDate() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMddHHmm");
        LocalDateTime now = LocalDateTime.now();
        this.aggregationDate = dtf.format(now);
    }

    private String[] getLineArray(Object[] objects) {
        String[] lineArr = Arrays.asList(objects).toArray(new String[objects.length]);
        String[] linkElementArr = lineArr[lineArr.length - 1].trim().split(" ");
        for (int i=0; i<linkElementArr.length; i++) {
            linkElementArr[i] = getLinkString(linkElementArr[i]);
        }
        lineArr[lineArr.length-1] = String.join(";", linkElementArr);
        return lineArr;
    }

    private String getLinkString(String s) {
        switch (s.charAt(0)) {
            case ('1'):
                if (s.charAt(1) == '0') return s+','+"MSSante"+','+'1';
                return s+','+"ADELI"+','+'1';
            case ('3'):
                return s+','+"FINESS"+','+'1';
            case ('4'):
                return s+','+"SIREN"+','+'1';
            case ('5'):
                return s+','+"SIRET"+','+'1';
            case ('6'):
            case ('8'):
                return s+','+"RPPS"+','+'1';
            default:
                return s+','+"ADELI"+','+'1';
        }
    }

}
