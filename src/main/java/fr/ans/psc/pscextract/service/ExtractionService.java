package fr.ans.psc.pscextract.service;

import fr.ans.psc.pscextract.model.PsLine;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.OutOperation;
import org.springframework.data.mongodb.core.aggregation.ProjectionOperation;
import org.springframework.stereotype.Service;

/**
 * The type Extraction service.
 */
@Service
public class ExtractionService {

    /**
     * The Mongo template.
     */
    @Autowired
    MongoTemplate mongoTemplate;

    @Value("${mongodb.outCollection}")
    private String outCollection;

    @Value("${mongodb.inCollection}")
    private String inCollection;

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

}
