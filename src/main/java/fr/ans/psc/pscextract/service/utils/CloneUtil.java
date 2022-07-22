package fr.ans.psc.pscextract.service.utils;

import fr.ans.psc.model.Expertise;
import fr.ans.psc.model.FirstName;
import fr.ans.psc.model.Profession;
import fr.ans.psc.model.Ps;
import fr.ans.psc.model.Structure;
import fr.ans.psc.model.WorkSituation;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class CloneUtil {

  public static Ps clonePs(Ps sourcePs, Profession sourceProfession, Expertise sourceExpertise, WorkSituation sourceWorkSituation){
    Ps targetPs = new Ps();
    targetPs.setIdType(sourcePs.getIdType());
    targetPs.setId(sourcePs.getId());
    targetPs.setNationalId(sourcePs.getNationalId());
    targetPs.setLastName(sourcePs.getLastName());

    List<FirstName> targetFirstNames = new ArrayList<>();
    for(FirstName firstName : sourcePs.getFirstNames()){
      targetFirstNames.add(new FirstName(firstName.getFirstName(), firstName.getOrder()));
    }
    targetPs.setFirstNames(targetFirstNames);

    targetPs.setDateOfBirth(sourcePs.getDateOfBirth());
    targetPs.setBirthAddressCode(sourcePs.getBirthAddressCode());
    targetPs.setBirthCountryCode(sourcePs.getBirthCountryCode());
    targetPs.setBirthAddress(sourcePs.getBirthAddress());
    targetPs.setGenderCode(sourcePs.getGenderCode());
    targetPs.setPhone(sourcePs.getPhone());
    targetPs.setEmail(sourcePs.getEmail());
    targetPs.setSalutationCode(sourcePs.getSalutationCode());

    List<Profession> targetProfessions = new ArrayList<>();
    targetProfessions.set(0, cloneProfession(sourceProfession, sourceExpertise, sourceWorkSituation));

    List<String> targetIds = new ArrayList<>();
    for(String id : sourcePs.getIds()){
      targetIds.add(id);
    }
    targetPs.setIds(targetIds);

    targetPs.setActivated(sourcePs.getActivated());
    targetPs.setDeactivated(sourcePs.getDeactivated());

    return targetPs;
  }

  public static Profession cloneProfession(Profession sourceProfession, Expertise sourceExpertise, WorkSituation sourceWorkSituation){
    Profession targetProfession = new Profession();

    targetProfession.setExProId(sourceProfession.getExProId());
    targetProfession.setCode(sourceProfession.getCode());
    targetProfession.setCategoryCode(sourceProfession.getCategoryCode());
    targetProfession.setSalutationCode(sourceProfession.getSalutationCode());
    targetProfession.setLastName(sourceProfession.getLastName());
    targetProfession.setFirstName(sourceProfession.getFirstName());

    List<Expertise> targetExpertises = new ArrayList<>();
    targetExpertises.set(0, cloneExpertise(sourceExpertise));

    List<WorkSituation> targetWorkSituations = new ArrayList<>();
    targetWorkSituations.set(0, cloneWorkSituation(sourceWorkSituation));

    return targetProfession;
  }

  public static Expertise cloneExpertise(Expertise sourceExpertise){
    Expertise targetExpertise = new Expertise();

    targetExpertise.setExpertiseId(sourceExpertise.getExpertiseId());
    targetExpertise.setTypeCode(sourceExpertise.getTypeCode());
    targetExpertise.setCode(sourceExpertise.getCode());

    return targetExpertise;
  }

  public static WorkSituation cloneWorkSituation(WorkSituation sourceWorkSituation){
    WorkSituation targetWorkSituation = new WorkSituation();

    targetWorkSituation.setSituId(sourceWorkSituation.getSituId());
    targetWorkSituation.setModeCode(sourceWorkSituation.getModeCode());
    targetWorkSituation.setActivitySectorCode(sourceWorkSituation.getActivitySectorCode());
    targetWorkSituation.setPharmacistTableSectionCode(sourceWorkSituation.getPharmacistTableSectionCode());
    targetWorkSituation.setRoleCode(sourceWorkSituation.getRoleCode());
    targetWorkSituation.setActivityKindCode(sourceWorkSituation.getActivityKindCode());
    targetWorkSituation.setRegistrationAuthority(sourceWorkSituation.getRegistrationAuthority());
    targetWorkSituation.setStructure(cloneStructure(sourceWorkSituation.getStructure()));

    return targetWorkSituation;
  }

  public static Structure cloneStructure(Structure sourceStructure){
    Structure targetStructure = new Structure();

    targetStructure.setSiteSIRET(sourceStructure.getSiteSIRET());
    targetStructure.setSiteSIREN(sourceStructure.getSiteSIREN());
    targetStructure.setSiteFINESS(sourceStructure.getSiteFINESS());
    targetStructure.setLegalEstablishmentFINESS(sourceStructure.getLegalEstablishmentFINESS());
    targetStructure.setStructureTechnicalId(sourceStructure.getStructureTechnicalId());
    targetStructure.setLegalCommercialName(sourceStructure.getLegalCommercialName());
    targetStructure.setPublicCommercialName(sourceStructure.getPublicCommercialName());
    targetStructure.setRecipientAdditionalInfo(sourceStructure.getRecipientAdditionalInfo());
    targetStructure.setGeoLocationAdditionalInfo(sourceStructure.getGeoLocationAdditionalInfo());
    targetStructure.setStreetNumber(sourceStructure.getStreetNumber());
    targetStructure.setStreetNumberRepetitionIndex(sourceStructure.getStreetNumberRepetitionIndex());
    targetStructure.setStreetCategoryCode(sourceStructure.getStreetCategoryCode());
    targetStructure.setStreetLabel(sourceStructure.getStreetLabel());
    targetStructure.setDistributionMention(sourceStructure.getDistributionMention());
    targetStructure.setCedexOffice(sourceStructure.getCedexOffice());
    targetStructure.setPostalCode(sourceStructure.getPostalCode());
    targetStructure.setCommuneCode(sourceStructure.getCommuneCode());
    targetStructure.setCountryCode(sourceStructure.getCountryCode());
    targetStructure.setPhone(sourceStructure.getPhone());
    targetStructure.setPhone2(sourceStructure.getPhone2());
    targetStructure.setFax(sourceStructure.getFax());
    targetStructure.setEmail(sourceStructure.getEmail());
    targetStructure.setDepartmentCode(sourceStructure.getDepartmentCode());
    targetStructure.setOldStructureId(sourceStructure.getOldStructureId());

    return targetStructure;
  }


}
