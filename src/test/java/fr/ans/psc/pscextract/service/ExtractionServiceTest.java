package fr.ans.psc.pscextract.service;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

@SpringBootTest
class ExtractionServiceTest {

    @Autowired
    ExtractionService es;

    @Test
    @Disabled
    void aggregateTest() {
        es.aggregate();
    }

    @Test
    @Disabled
    void extractTest() throws IOException {
        es.extract();
    }

    @Test
    @Disabled
    void transformTest() throws IOException {
        es.transformCsv();
    }

    @Test
    @Disabled
    void cleanupTest() {
    }

}
