package fr.ans.psc.pscextract.service;

import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.ObjectRowProcessor;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import fr.ans.psc.pscextract.service.utils.FileNamesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Service;
import org.springframework.util.StreamUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
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

    @Value("${files.directory}")
    private String filesDirectory;

    private String extractTime ="197001010001";

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
                "Télécopie (coord. structure)|Adresse e-mail (coord. structure)|Code département (structure)|" +
                "Ancien identifiant de la structure|Autorité d'enregistrement|Autres identifiants|";
        setExtractionTime();
        Files.write(Paths.get(FileNamesUtil.getFilePath(
                filesDirectory, FileNamesUtil.extractRASSName(extractName, extractTime))), Collections.singleton(header),
                StandardCharsets.UTF_8);

        FileWriter f = new FileWriter(FileNamesUtil.getFilePath(
                filesDirectory, FileNamesUtil.extractRASSName(extractName, extractTime)), true);
        BufferedWriter b = new BufferedWriter(f);
        PrintWriter p = new PrintWriter(b, true);

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
        parser.parse(new BufferedReader(new FileReader(FileNamesUtil.getFilePath(filesDirectory, extractName))));
        p.close();b.close();f.close();
        log.info("transformation complete!");
    }

    /**
     * Zip file.
     *
     * @param out the OutputStream
     */
    public void zipFile(OutputStream out) {
        FileSystemResource resource = new FileSystemResource(
                FileNamesUtil.getFilePath(filesDirectory, FileNamesUtil.extractRASSName(extractName, extractTime)));
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

    private String[] getLineArray(Object[] objects) {
        String[] lineArr = Arrays.asList(objects).toArray(new String[objects.length]);
        lineArr[0] = String.valueOf(lineArr[2].charAt(0)); // first number of nationalId
        lineArr[1] = lineArr[2].substring(1);              // nationalId without first number
        String[] linkElementArr = lineArr[lineArr.length - 1].trim().split(" ");  // last element split to array
        for (int i=0; i<linkElementArr.length; i++) {
            linkElementArr[i] = getLinkString(linkElementArr[i]);  // building each section
        }
        lineArr[lineArr.length-1] = String.join(";", linkElementArr);  // putting it back together
        return lineArr;
    }

    private String getLinkString(String s) {
        switch (s.charAt(0)) {
            case ('1'):
                // if (s.charAt(1) == '0') return s+','+"MSSante"+','+'1';
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

    private void setExtractionTime() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMddHHmm");
        LocalDateTime now = LocalDateTime.now();
        extractTime = dtf.format(now);
    }

    /**
     * Zip name string.
     *
     * @return the string
     */
    public String zipName() {
        return extractName + "_" + extractTime + ".zip";
    }

}
