package fr.ans.psc.pscextract.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.management.RuntimeErrorException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * The type Aggregation service.
 */
@Service
public class AggregationService {

    /**
     * logger.
     */
    private static final Logger log = LoggerFactory.getLogger(AggregationService.class);

    @Value("${mongodb.name}")
    private String mongodbName;

    @Value("${mongodb.host}")
    private String mongoHost;

    @Value("${mongodb.port}")
    private String mongoPort;

    @Value("${mongodb.username}")
    private String mongoUserName;

    @Value("${mongodb.password}")
    private String mongoPassword;

    @Value("${mongodb.admin.database}")
    private String mongoAdminDatabase;

    /**
     * Instantiates a new Extraction service.
     */
    private AggregationService() {
    }

    /**
     * Aggregate.
     */
    public void aggregate() throws IOException, InterruptedException {
        log.info("aggregating ...");

        // transform Dos/Windows end of lines (CRLF) to Unix end of lines (LF).
        Process dos2Unix = Runtime.getRuntime().exec("dos2unix /app/resources/aggregate.mongo");
        if (dos2Unix.waitFor() != 0) {
            log.error("Dos2Unix failed : code retour = {}", dos2Unix.exitValue());
            throw new RuntimeException("Dos2Unix failed");
        }
        log.info("Dos2Unix success");

        String script = "use mongodb;\n" +
                "db.ps.aggregate([\n" +
                "    {$match: {$expr: {$gt: ['$activated', '$deactivated']}}},\n" +
                "    {$unwind: {path:\"$firstNames\"}},\n" +
                "    {$sort: {\"firstNames.order\":1}},\n" +
                "    {$group: {\n" +
                "        \"_id\": \"$_id\",\n" +
                "        \"idType\":{\"$first\":\"$idType\"},\n" +
                "        \"id\":{\"$first\":\"$id\"},\n" +
                "        \"nationalId\":{\"$first\":\"$nationalId\"},\n" +
                "        \"lastName\":{\"$first\":\"$lastName\"},\n" +
                "        \"firstNames\": {$push: \"$firstNames\"},\n" +
                "        \"dateOfBirth\":{\"$first\":\"$dateOfBirth\"},\n" +
                "        \"birthAddressCode\":{\"$first\":\"$birthAddressCode\"},\n" +
                "        \"birthCountryCode\":{\"$first\":\"$birthCountryCode\"},\n" +
                "        \"birthAddress\":{\"$first\":\"$birthAddress\"},\n" +
                "        \"genderCode\":{\"$first\":\"$genderCode\"},\n" +
                "        \"phone\":{\"$first\":\"$phone\"},\n" +
                "        \"email\":{\"$first\":\"$email\"},\n" +
                "        \"salutationCode\":{\"$first\":\"$salutationCode\"},\n" +
                "        \"professions\":{\"$first\":\"$professions\"},\n" +
                "        \"otherIds\":{$first:\"$ids\"}\n" +
                "    }},\n" +
                "    {$addFields: { firstNamesString: {$reduce: {input: \"$firstNames\",\n" +
                "        initialValue: \"\",\n" +
                "        in: {$concat: [\"$$value\", \"$$this.firstName\",\"\\'\"]}\n" +
                "    }}}},\n" +
                "    {$unwind: {path: \"$professions\", preserveNullAndEmptyArrays: true}},\n" +
                "    {$unwind: {path: \"$professions.expertises\", preserveNullAndEmptyArrays: true}},\n" +
                "    {$unwind: {path: \"$professions.workSituations\", preserveNullAndEmptyArrays: true}},\n" +
                "    {$unwind: {path: \"$professions.workSituations.structure\", preserveNullAndEmptyArrays: true}},\n" +
                "    {$project: {\n" +
                "        _id: 0,\n" +
                "        idType: \"$idType\",\n" +
                "        id: \"$id\",\n" +
                "        nationalId: \"$nationalId\",\n" +
                "        lastName: \"$lastName\",\n" +
                "        firstName: {$substrCP: [\"$firstNamesString\",0,{$add:[{$strLenCP:\"$firstNamesString\"},-1]}]},\n" +
                "        dateOfBirth: \"$dateOfBirth\",\n" +
                "        birthAddressCode: \"$birthAddressCode\",\n" +
                "        birthCountryCode: \"$birthCountryCode\",\n" +
                "        birthAddress: \"$birthAddress\",\n" +
                "        genderCode: \"$genderCode\",\n" +
                "        phone: \"$phone\",\n" +
                "        email: \"$email\",\n" +
                "        salutationCode: \"$salutationCode\",\n" +
                "        profession_code: \"$professions.code\",\n" +
                "        profession_categoryCode: \"$professions.categoryCode\",\n" +
                "        profession_salutationCode: \"$professions.salutationCode\",\n" +
                "        profession_lastName: \"$professions.lastName\",\n" +
                "        profession_firstName: \"$professions.firstName\",\n" +
                "        profession_expertise_typeCode: \"$professions.expertises.typeCode\",\n" +
                "        profession_expertise_code: \"$professions.expertises.code\",\n" +
                "        profession_situation_modeCode: \"$professions.workSituations.modeCode\",\n" +
                "        profession_situation_activitySectorCode: \"$professions.workSituations.activitySectorCode\",\n" +
                "        profession_situation_pharmacistTableSectionCode: \"$professions.workSituations.pharmacistTableSectionCode\",\n" +
                "        profession_situation_roleCode: \"$professions.workSituations.roleCode\",\n" +
                "        structure_siteSIRET: \"$professions.workSituations.structure.siteSIRET\",\n" +
                "        structure_siteSIREN: \"$professions.workSituations.structure.siteSIREN\",\n" +
                "        structure_siteFINESS: \"$professions.workSituations.structure.siteFINESS\",\n" +
                "        structure_legalEstablishmentFINESS: \"$professions.workSituations.structure.legalEstablishmentFINESS\",\n" +
                "        structure_structureTechnicalId: \"$professions.workSituations.structure.structureTechnicalId\",\n" +
                "        structure_legalCommercialName: \"$professions.workSituations.structure.legalCommercialName\",\n" +
                "        structure_publicCommercialName: \"$professions.workSituations.structure.publicCommercialName\",\n" +
                "        structure_recipientAdditionalInfo: \"$professions.workSituations.structure.recipientAdditionalInfo\",\n" +
                "        structure_geoLocationAdditionalInfo: \"$professions.workSituations.structure.geoLocationAdditionalInfo\",\n" +
                "        structure_streetNumber: \"$professions.workSituations.structure.streetNumber\",\n" +
                "        structure_streetNumberRepetitionIndex: \"$professions.workSituations.structure.streetNumberRepetitionIndex\",\n" +
                "        structure_streetCategoryCode: \"$professions.workSituations.structure.streetCategoryCode\",\n" +
                "        structure_streetLabel: \"$professions.workSituations.structure.streetLabel\",\n" +
                "        structure_distributionMention: \"$professions.workSituations.structure.distributionMention\",\n" +
                "        structure_cedexOffice: \"$professions.workSituations.structure.cedexOffice\",\n" +
                "        structure_postalCode: \"$professions.workSituations.structure.postalCode\",\n" +
                "        structure_communeCode: \"$professions.workSituations.structure.communeCode\",\n" +
                "        structure_countryCode: \"$professions.workSituations.structure.countryCode\",\n" +
                "        structure_phone: \"$professions.workSituations.structure.phone\",\n" +
                "        structure_phone2: \"$professions.workSituations.structure.phone2\",\n" +
                "        structure_fax: \"$professions.workSituations.structure.fax\",\n" +
                "        structure_email: \"$professions.workSituations.structure.email\",\n" +
                "        structure_departmentCode: \"$professions.workSituations.structure.departmentCode\",\n" +
                "        structure_oldStructureId: \"$professions.workSituations.structure.oldStructureId\",\n" +
                "        profession_situation_registrationAuthority: \"$professions.workSituations.registrationAuthority\",\n" +
                "        profession_situation_activityKindCode: \"$professions.workSituations.activityKindCode\",\n" +
                "        otherIds: {$reduce: {input: \"$otherIds\", initialValue: \"\", in: {$concat: [\"$$value\",\" \",\"$$this\"]}}}\n" +
                "    }},\n" +
                "    {$out :\"extractRass\"}\n" +
                "], {allowDiskUse: true});\n" +
                "quit();\n";

        String cmd = "/usr/bin/mongosh --host=" + mongoHost + " --port=" + mongoPort + " --username=" + mongoUserName + " --password=" + mongoPassword
                + " --authenticationDatabase=" + mongoAdminDatabase + " " + "--eval "+ script + mongodbName;

        String[] cmdArr = {
                "/bin/sh",
                "-c",
                cmd
        };

        log.info("Executing command : {}", Arrays.toString(cmdArr));

        // TO DO : create custom Exception to handle  process failures
        Process p = Runtime.getRuntime().exec(cmdArr);
        if (p.waitFor() == 0) {
            log.info("finished aggregation");
        } else {
            StringBuilder infoBuilder = new StringBuilder();
            StringBuilder errorBuilder = new StringBuilder();
            try (Reader infoReader = new BufferedReader(new InputStreamReader
                    (p.getInputStream(), Charset.forName(StandardCharsets.UTF_8.name())));
                 Reader errorReader = new BufferedReader(new InputStreamReader
                         (p.getErrorStream(), Charset.forName(StandardCharsets.UTF_8.name())))) {
                int c;
                while ((c = infoReader.read()) != -1) {
                    infoBuilder.append((char) c);
                }
                while ((c = errorReader.read()) != -1) {
                    errorBuilder.append((char) c);
                }
            }
            log.error("inputstream : {}", infoBuilder);
            log.error("errorstream : {}", errorBuilder);
            log.error("exit value : {}", p.exitValue());

            throw new RuntimeException("mongosh command failed : " + errorBuilder);
        }
    }

}
