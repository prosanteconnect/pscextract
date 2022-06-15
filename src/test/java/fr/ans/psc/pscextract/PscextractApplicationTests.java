package fr.ans.psc.pscextract;

import fr.ans.psc.pscextract.service.TransformationService;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Arrays;

@SpringBootTest
class PscextractApplicationTests {

    @Autowired
    TransformationService transformationService;

    @Test
    @Disabled
    void contextLoads() {
    }

    @Test
    void testLineArray() {
        String[] line = {"0", "886168376", "0886168376", "AIGLE", "Nicolas''", "09/07/1986","" ,"" ,"" ,"" , "0647019320", "NICOLASAIGLE@GMAIL.COM", "M", "60", "C", "",
                "AIGLE", "Nicolas","" ,"" , "S", "SA01","" ,"" , "26880013300012","" , "880000047", "880780077", "F880000047", "CENTRE HOSPITALIER DE SAINT-DIE",
                "","" ,"" , "26", "", "R", "DU NOUVEL HOPITAL","" , "88100 ST DIE DES VOSGES", "88100", "88413", "", "0329528310", "", "0329528301",
                "direction@ch-saintdie.fr","" , "1880000047", "ARS/ARS/ARS", " 0886168376", ""};

        String[] lineArray = transformationService.getLineArray(line);
        Arrays.stream(lineArray).forEach(System.out::println);
    }

}
