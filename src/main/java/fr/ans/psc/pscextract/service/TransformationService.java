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
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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

    private String extractTime ="197001010001";

    private final String ZIP_EXTENSION = ".zip";
    private final String TXT_EXTENSION = ".txt";

    /**
     * Transform csv.
     *
     * @throws IOException the io exception
     */
    public void transformCsv() throws IOException {
        log.info("starting file transformation");
        File tempExtractFile = File.createTempFile("tempExtract", "tmp");
        log.info("File extracted in {}", tempExtractFile.getAbsolutePath());

        BufferedWriter bw = Files.newBufferedWriter(tempExtractFile.toPath(), StandardCharsets.UTF_8);
        log.info("BufferedWriter created");

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
                "Ancien identifiant de la structure|Autorité d'enregistrement|Autres identifiants|\n";
        log.info("Header created");

        bw.write(header);
        log.info("Header written");

        setExtractionTime();
        log.info("Extraction time set");

        // ObjectRowProcessor converts the parsed values and gives you the resulting row.
        ObjectRowProcessor rowProcessor = new ObjectRowProcessor() {
            @Override
            public void rowProcessed(Object[] objects, ParsingContext parsingContext) {
                log.info("Object array: {}", Arrays.toString(objects));
                String line = String.join("|", getLineArray(objects)) + "|\n";
                log.info("Line created : {}", line);
                try {
                    bw.write(line);
                    log.info("Line written: {}", line);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        log.info("RowProcessor created");

        CsvParserSettings parserSettings = new CsvParserSettings();
        log.info("CsvParserSettings created");

        parserSettings.getFormat().setLineSeparator("\n");
        parserSettings.getFormat().setDelimiter(',');
        parserSettings.setProcessor(rowProcessor);
        parserSettings.setHeaderExtractionEnabled(true);
        parserSettings.setNullValue("");
        log.info("CsvParserSettings configured");

        CsvParser parser = new CsvParser(parserSettings);
        log.info("CsvParser created");

        try {
            log.info("Parsing file");
            try {
                parser.parse(new BufferedReader(new FileReader(FileNamesUtil.getFilePath(filesDirectory, extractName))));
            } catch (Exception e) {
                e.printStackTrace();
            }
            log.info("File parsed");
            bw.close();
            log.info("BufferedWriter closed");
            InputStream fileContent = new FileInputStream(tempExtractFile);
            log.info("File content read");

            ZipEntry zipEntry = new ZipEntry(getFileNameWithExtension(TXT_EXTENSION));
            zipEntry.setTime(System.currentTimeMillis());
            ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(FileNamesUtil.getFilePath(workingDirectory, getFileNameWithExtension(ZIP_EXTENSION))));
            zos.putNextEntry(zipEntry);
            StreamUtils.copy(fileContent, zos);
            log.info("File content written in zip file");

            fileContent.close();
            zos.closeEntry();
            zos.finish();
            zos.close();
            log.info("Zip file created");

            Files.move(Path.of(FileNamesUtil.getFilePath(workingDirectory, getFileNameWithExtension(ZIP_EXTENSION))),
                    Path.of(FileNamesUtil.getFilePath(filesDirectory, getFileNameWithExtension(ZIP_EXTENSION))));

            tempExtractFile.delete();
            log.info("Temp file deleted");

            log.info("transformation complete!");
        } catch (FileNotFoundException e) {
            log.error("csv file unavailable for transformation");
            throw e;
        } catch (IOException ioe) {
            log.error("could not put zip entry in zip output stream");
            throw ioe;
        }

    }

    private String[] getLineArray(Object[] objects) {
        String[] lineArr = Arrays.asList(objects).toArray(new String[objects.length]);
        log.info("Line array created {}", Arrays.toString(lineArr));
        lineArr[0] = String.valueOf(lineArr[2].charAt(0)); // first number of nationalId
        lineArr[1] = lineArr[2].substring(1);              // nationalId without first number
        String[] linkElementArr = lineArr[lineArr.length - 1].trim().split(" ");  // last element split to array
        for (int i=0; i<linkElementArr.length; i++) {
            linkElementArr[i] = getLinkString(linkElementArr[i]);  // building each section
            log.info("getLineArray: linkElementArr[i] = " + linkElementArr[i]);
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
    public String getFileNameWithExtension(String fileExtension) {
        return extractName + "_" + extractTime + fileExtension;
    }

}
