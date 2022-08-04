package fr.ans.psc.pscextract.service;

import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.ObjectRowProcessor;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import fr.ans.psc.model.Expertise;
import fr.ans.psc.model.FirstName;
import fr.ans.psc.model.Profession;
import fr.ans.psc.model.Ps;
import fr.ans.psc.model.Structure;
import fr.ans.psc.model.WorkSituation;
import fr.ans.psc.pscextract.controller.ExtractionController;
import fr.ans.psc.pscextract.service.utils.CloneUtil;
import fr.ans.psc.pscextract.service.utils.FileNamesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StreamUtils;
import org.springframework.web.client.RestClientException;

import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@Service
public class TransformationService {

  /**
   * logger.
   */
  private static final Logger log = LoggerFactory.getLogger(TransformationService.class);

  @Value("${extract.name}")
  private String extractName;

  @Value("${extract.test.name}")
  public String extractTestName;

  @Value("${files.directory}")
  private String filesDirectory;

  @Value("${working.directory}")
  private String workingDirectory;

  @Value("${first.name.count}")
  private Integer firstNameCount;

  private String extractTime = "197001010001";

  public String getLinkString(String id) {
    if (id.isEmpty()) {
      return "";
    }
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

  public String transformIdsToString(List<String> ids) {
    if (ids == null)
      return "";

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < ids.size(); i++) {
      sb.append(getLinkString(ids.get(i)));
      if (i != ids.size() - 1) {
        sb.append(";");
      }
    }
    return sb.toString();
  }

  public String transformFirstNamesToStringWithApostrophes(List<FirstName> firstNames) {
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

  public String getFileNameWithExtension(String fileExtension) {
    return extractName + "_" + extractTime + fileExtension;
  }

  public ArrayList<Ps> unwind(List<Ps> psList) {
    ArrayList<Ps> unwoundPsList = new ArrayList<>();
    Ps tempPs;
    for (Ps ps : psList) {
      if (ps.getDeactivated() == null || ps.getActivated() > ps.getDeactivated()) {
        if (ps.getProfessions() == null) {
          tempPs = CloneUtil.clonePs(ps, null, null, null);
          unwoundPsList.add(tempPs);
        } else
          for (Profession profession : ps.getProfessions()) {
            if (profession.getExpertises() == null && profession.getWorkSituations() == null) {
              tempPs = CloneUtil.clonePs(ps, profession, null, null);
              unwoundPsList.add(tempPs);
            } else if (profession.getExpertises() == null && profession.getWorkSituations() != null) {
              for (WorkSituation workSituation : profession.getWorkSituations()) {
                tempPs = CloneUtil.clonePs(ps, profession, null, workSituation);
                unwoundPsList.add(tempPs);
              }
            } else
              for (Expertise expertise : profession.getExpertises()) {
                if (profession.getWorkSituations() == null) {
                  tempPs = CloneUtil.clonePs(ps, profession, expertise, null);
                  unwoundPsList.add(tempPs);
                } else
                  for (WorkSituation workSituation : profession.getWorkSituations()) {
                    tempPs = CloneUtil.clonePs(ps, profession, expertise, workSituation);
                    unwoundPsList.add(tempPs);
                  }
              }
          }
      }
    }
    return unwoundPsList;
  }

  public String transformPsToLine(Ps ps) {
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

    if (ps.getProfessions() != null && ps.getProfessions().get(0) != null) {
      Profession profession = ps.getProfessions().get(0);
      sb.append(Optional.ofNullable(profession.getCode()).orElse("")).append("|");
      sb.append(Optional.ofNullable(profession.getCategoryCode()).orElse("")).append("|");
      sb.append(Optional.ofNullable(profession.getSalutationCode()).orElse("")).append("|");
      sb.append(Optional.ofNullable(profession.getLastName()).orElse("")).append("|");
      sb.append(Optional.ofNullable(profession.getFirstName()).orElse("")).append("|");

      if (profession.getExpertises() != null && profession.getExpertises().get(0) != null) {
        Expertise expertise = profession.getExpertises().get(0);
        sb.append(Optional.ofNullable(expertise.getTypeCode()).orElse("")).append("|");
        sb.append(Optional.ofNullable(expertise.getCode()).orElse("")).append("|");
      } else {
        sb.append("|".repeat(2));
      }

      if (profession.getWorkSituations() != null && profession.getWorkSituations().get(0) != null) {
        WorkSituation workSituation = profession.getWorkSituations().get(0);
        sb.append(Optional.ofNullable(workSituation.getModeCode()).orElse("")).append("|");
        sb.append(Optional.ofNullable(workSituation.getActivitySectorCode()).orElse("")).append("|");
        sb.append(Optional.ofNullable(workSituation.getPharmacistTableSectionCode()).orElse("")).append("|");
        sb.append(Optional.ofNullable(workSituation.getRoleCode()).orElse("")).append("|");

        if (workSituation.getStructure() != null) {
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
        } else {
          sb.append("|".repeat(24));
        }
        sb.append(Optional.ofNullable(workSituation.getRegistrationAuthority()).orElse("")).append("|");
        activityCode = (Optional.ofNullable(workSituation.getActivityKindCode()).orElse(""));

      } else {
        sb.append("|".repeat(29));
      }
    } else {
      sb.append("|".repeat(36));
    }
    sb.append(Optional.ofNullable(transformIdsToString(ps.getIds())).orElse("")).append("|");
    sb.append(Optional.ofNullable(activityCode).orElse("")).append("|");
    sb.append("\n");

    return sb.toString();
  }

  public void setExtractionTime() {
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMddHHmm");
    LocalDateTime now = LocalDateTime.now();
    extractTime = dtf.format(now);
  }

  public File extractToCsv(ExtractionController extractionController) throws IOException {
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

    int page = 0;
    List<Ps> responsePsList;
    List<Ps> tempPsList;

    log.info("Starting extraction at " + extractionController.getApiBaseUrl());

    try {
      BigDecimal size = BigDecimal.valueOf(extractionController.getPageSize());
      List<Ps> response = extractionController.getPsApi().getPsByPage(BigDecimal.valueOf(page), size);
      log.info("Page " + page + " of size " + size + " received");
      boolean outOfPages = false;

      do {
        responsePsList = response;
        tempPsList = unwind(responsePsList);

        for (Ps ps : tempPsList) {
          bw.write(transformPsToLine(ps));
          log.info("Ps " + ps.getId() + " transformed and written");
        }
        page++;
        try {
          response = extractionController.getPsApi().getPsByPage(BigDecimal.valueOf(page), size);
          log.info("Page " + page + " of size" + size + " received");
        } catch (RestClientException e) {
          log.warn("Out of pages: " + e.getMessage());
          outOfPages = true;
        }
      } while (!outOfPages);
    } catch (RestClientException e) {
      log.error("No pages found :", e);
      return null;
    } finally {
      bw.close();
      log.info("BufferedWriter closed");
    }

    InputStream fileContent = new FileInputStream(tempExtractFile);
    log.info("File content read");

    ZipEntry zipEntry = new ZipEntry(getFileNameWithExtension(extractionController.getTXT_EXTENSION()));
    zipEntry.setTime(System.currentTimeMillis());
    ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(FileNamesUtil.getFilePath(extractionController.getWorkingDirectory(), getFileNameWithExtension(extractionController.getZIP_EXTENSION()))));
    zos.putNextEntry(zipEntry);
    StreamUtils.copy(fileContent, zos);

    fileContent.close();
    zos.closeEntry();
    zos.finish();
    zos.close();


    if (tempExtractFile.delete()) {
      log.info("Temp file at " + tempExtractFile.getAbsolutePath() + " deleted");
    } else {
      log.warn("Temp file at " + tempExtractFile.getAbsolutePath() + " not deleted");
    }

    Files.move(Path.of(FileNamesUtil.getFilePath(extractionController.getWorkingDirectory(), getFileNameWithExtension(extractionController.getZIP_EXTENSION()))),
            Path.of(FileNamesUtil.getFilePath(extractionController.getFilesDirectory(), getFileNameWithExtension(extractionController.getZIP_EXTENSION()))));
    log.info("File at " + FileNamesUtil.getFilePath(extractionController.getWorkingDirectory(), getFileNameWithExtension(extractionController.getZIP_EXTENSION())) + " moved to " + FileNamesUtil.getFilePath(extractionController.getFilesDirectory(), getFileNameWithExtension(extractionController.getZIP_EXTENSION())));
    return FileNamesUtil.getLatestExtract(extractionController.getFilesDirectory(), getFileNameWithExtension(extractionController.getZIP_EXTENSION()));
  }
}
