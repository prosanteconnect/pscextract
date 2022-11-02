package fr.ans.psc.pscextract;

import fr.ans.psc.model.FirstName;
import fr.ans.psc.pscextract.service.TransformationService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@ContextConfiguration(classes = PscextractApplication.class)
public class TransformationServiceTest {

    @Autowired
    TransformationService transformationService;

    @DynamicPropertySource
    static void registerPgProperties(DynamicPropertyRegistry propertiesRegistry) {
        propertiesRegistry.add("page.size", () -> "1");
    }

    @Test
    public void transformFirstNamesTest() {
        FirstName fn1 = new FirstName("KADER", 0);
        FirstName fn2 = new FirstName("HASSAN", 1);
        FirstName fn3 = new FirstName("JOHNNY", 2);

        List<FirstName> fnList = new ArrayList<>();
        fnList.add(fn1);
        fnList.add(fn3);
        fnList.add(fn2);

        String namesString = transformationService.transformFirstNamesToStringWithApostrophes(fnList);
        assertEquals("KADER'HASSAN'JOHNNY", namesString);
    }
}
