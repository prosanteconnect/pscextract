package fr.ans.psc.pscextract.service;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
@Disabled
@SpringBootTest
class ExportServiceTest {


    @Autowired
    ExportService es;

    @Autowired
    AggregationService as;

    @Autowired
    TransformationService ts;

    @Test
    @Disabled
    void aggregateTest() throws Exception {
        as.aggregate();
    }

    @Test
    @Disabled
    void extractTest() throws IOException, InterruptedException {
        es.export();
    }

    @Test
    @Disabled
    void transformTest() throws IOException {
        ts.transformCsv();
    }

    @Test
    @Disabled
    void cleanupTest() {
    }

}
