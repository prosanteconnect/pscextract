package fr.ans.psc.pscextract.service.utils;

import org.springframework.stereotype.Component;

@Component
public class FileNamesUtil {

    private FileNamesUtil() {
    }

    /**
     * Extract rass string.
     *
     * @return the string
     */
    public static String extractRASSName(String extractName, String extractTime) {
        return extractName + "_" + extractTime + ".txt";
    }

    public static String getFilePath(String filesDirectory, String fileName) {
        if ("".equals(filesDirectory)) {
            return filesDirectory + fileName;
        } else {
            return filesDirectory + '/' + fileName;
        }
    }

}
