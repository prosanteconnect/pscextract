package fr.ans.psc.pscextract.service;

import fr.ans.psc.pscextract.service.utils.FileNamesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Service;
import org.springframework.util.StreamUtils;

import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@Service
public class DownloadExtractService {

    private static final Logger log = LoggerFactory.getLogger(TransformationService.class);

    @Value("${extract.name}")
    private String extractName;

    @Value("${files.directory}")
    private String filesDirectory;

    @Value("${extract.test.name}")
    public String extractTestName;





    /**
     * Zip file.
     *
     * @param out the OutputStream
     */
    public void zipFile(OutputStream out, boolean prod) throws IOException {

        File extractFile = FileNamesUtil.getLatestExtract(filesDirectory, extractName);

        FileSystemResource resource = new FileSystemResource(FileNamesUtil.getFilePath(filesDirectory, prod ?
                extractFile.getName() :
                extractTestName + ".txt"));

        System.out.println(resource.getFilename());

        try (ZipOutputStream zippedOut = new ZipOutputStream(out);) {
            log.info(resource.getFilename());
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
        } catch (IOException e) {
            out.close();
            throw e;
        }
    }

    public String zipName() throws FileNotFoundException {
        File txtExtractFile = FileNamesUtil.getLatestExtract(filesDirectory, extractName);
        if (txtExtractFile == null) {
            log.error("No extract file is available for download");
            throw new FileNotFoundException("no extract file is available for download");
        }
        String txtFileName = txtExtractFile.getName();
        return txtFileName.substring(0, txtFileName.length() - 4) + ".zip";
    }
}
