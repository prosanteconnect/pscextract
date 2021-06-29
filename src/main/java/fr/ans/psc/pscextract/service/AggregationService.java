package fr.ans.psc.pscextract.service;

import fr.ans.psc.pscextract.model.LinkToOtherIds;
import fr.ans.psc.pscextract.model.PsLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.stereotype.Service;

/**
 * The type Aggregation service.
 */
@Service
public class AggregationService {

    /**
     * logger.
     */
    private static final Logger log = LoggerFactory.getLogger(AggregationService.class);

    @Autowired
    private MongoTemplate mongoTemplate;

    @Value("${mongodb.outCollection}")
    private String outCollection;

    @Value("${mongodb.inCollection}")
    private String inCollection;

    /**
     * Instantiates a new Extraction service.
     */
    private AggregationService() {
    }

    /**
     * Aggregate.
     */
    public void aggregate() {
        aggregateLink();
        aggregatePsLines();
    }

    private void aggregateLink() {
        log.info("aggregating extractOtherIds");
        String group = "{$group: {_id: '$nationalId', otherIdsArr: {$addToSet: '$nationalIdRef'}}}";
        String project = "{$project: {otherIds: {$reduce: {input: '$otherIdsArr', initialValue: '', in: {$concat: ['$$value', ' ', '$$this']}}}}}";
        OutOperation out = Aggregation.out("extractOtherIds");
        TypedAggregation<LinkToOtherIds> aggregation = Aggregation.newAggregation(
                LinkToOtherIds.class,
                new CustomAggregationOperation(group),
                new CustomAggregationOperation(project),
                out
        ).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
        // allowDiskUse is so that aggregation stages can write data to the _tmp subdirectory in the dbPath directory.
        // need to set it to true otherwise cant use 'group' on documents > 16MB

        mongoTemplate.aggregate(aggregation, inCollection, LinkToOtherIds.class);
    }

    private void aggregatePsLines() {
        log.info("aggregating into {}", outCollection);
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

}
