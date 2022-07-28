package fr.ans.psc.pscextract;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import fr.ans.psc.model.Ps;
import fr.ans.psc.pscextract.controller.ExtractionController;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

@SpringBootTest
@ContextConfiguration(classes = PscextractApplication.class)
@AutoConfigureMockMvc
class ExtractionControllerTests {

  @Autowired
    private ExtractionController controller;

  /** The http mock server. */
  @RegisterExtension
  static WireMockExtension httpMockServer = WireMockExtension.newInstance()
          .options(wireMockConfig().dynamicPort().usingFilesUnderClasspath("wiremock")).build();

  /**
   * Register pg properties.
   *
   * @param propertiesRegistry the properties registry
   */
  // For use with mockMvc
  @DynamicPropertySource
  static void registerPgProperties(DynamicPropertyRegistry propertiesRegistry) {
    propertiesRegistry.add("api.base.url",
            () -> httpMockServer.baseUrl());
    propertiesRegistry.add("working.directory", () -> "src/test/resources/work");
    propertiesRegistry.add("files.directory", () -> "src/test/resources/work");
    propertiesRegistry.add("page.size", () -> "1");
    propertiesRegistry.add("first.name.count", () -> "3");

  }

  @Test
  void extractGenerationTest() throws IOException {

    String responseFilename = "3p";
    String responsePath = Thread.currentThread().getContextClassLoader().getResource("wiremock/api/mappings/" + responseFilename + ".json")
            .getPath();
    byte[] responseByteArray = readFileToBytes(responsePath);

//    httpMockServer.stubFor(get("/v2/ps?page=0&size=1")
//            .willReturn(aResponse()
//                    .withStatus(200)
//                    .withHeader("Content-Type", "application/json")
//                    .withBodyFile("page1size1.json")));
//    httpMockServer.stubFor(get("/v2/ps?page=1&size=1")
//            .willReturn(aResponse()
//                    .withStatus(200)
//                    .withHeader("Content-Type", "application/json")
//                    .withBodyFile("page2size1.json")));
//    httpMockServer.stubFor(get("/v2/ps?page=2&size=1")
//            .willReturn(aResponse()
//                    .withStatus(200)
//                    .withHeader("Content-Type", "application/json")
//                    .withBodyFile("page3size1.json")));
//    httpMockServer.stubFor(get("/v2/ps?page=3&size=1").willReturn(aResponse().withStatus(410)));

    httpMockServer.stubFor(get("/v2/ps?page=0&size=1")
            .willReturn(aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBodyFile("multiple-work-situations.json")));
    httpMockServer.stubFor(get("/v2/ps?page=1&size=1")
            .willReturn(aResponse()
                    .withStatus(420)));

    ResponseEntity<List<Ps>> response = controller.generateExtractAndGetFile();

    System.out.println(response.getBody());
  }

  private static byte[] readFileToBytes(String filePath) throws IOException {

    File file = new File(filePath);
    byte[] bytes = new byte[(int) file.length()];

    // funny, if can use Java 7, please uses Files.readAllBytes(path)
    try (FileInputStream fis = new FileInputStream(file)) {
      fis.read(bytes);
    }
    return bytes;
  }

}
