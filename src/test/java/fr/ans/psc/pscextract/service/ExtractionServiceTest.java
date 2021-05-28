package fr.ans.psc.pscextract.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class ExtractionServiceTest {

    @Autowired
    ExtractionService es;

    @Test
    void aggregateTest() {
        es.aggregate();
    }

}
