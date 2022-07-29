package fr.ans.psc.pscextract;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import fr.ans.psc.model.Ps;
import fr.ans.psc.pscextract.controller.ExtractionController;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

@SpringBootTest
@ContextConfiguration(classes = PscextractApplication.class)
@AutoConfigureMockMvc
class ExtractionControllerTests {

  @Autowired
  private ExtractionController controller;

  /**
   * The http mock server.
   */
  @RegisterExtension
  static WireMockExtension httpMockServer = WireMockExtension.newInstance()
          .options(wireMockConfig().dynamicPort().usingFilesUnderClasspath("wiremock")).build();

  /**
   * Register pg properties.
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
  void singlePageExtractionAndResultConformityTest() throws IOException {

    String responseFilename = "multiple-work-situations-result";
    String responsePath = Thread.currentThread().getContextClassLoader().getResource("wiremock/__files/" + responseFilename + ".txt")
            .getPath();
    byte[] expectedResponseBytes = readFileToBytes(responsePath);

    httpMockServer.stubFor(get("/v2/ps?page=0&size=1")
            .willReturn(aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBodyFile("multiple-work-situations.json")));
    httpMockServer.stubFor(get("/v2/ps?page=1&size=1")
            .willReturn(aResponse()
                    .withStatus(420)));

    ResponseEntity<FileSystemResource> response = controller.generateExtractAndGetFile();

    ZipFile zipFile = new ZipFile(response.getBody().getFile());
    Enumeration<? extends ZipEntry> enumeration = zipFile.entries();
    InputStream stream = null;

    while (enumeration.hasMoreElements()) {
      ZipEntry zipEntry = enumeration.nextElement();
      stream = zipFile.getInputStream(zipEntry);
    }
    byte[] responseBytes = stream.readAllBytes();

    zipFile.close();
    stream.close();

    String expected = new String(expectedResponseBytes, StandardCharsets.UTF_8);
    String actual = new String(responseBytes, StandardCharsets.UTF_8);

    Assertions.assertEquals(expected, actual);
  }

  @Test
  void multiplePagesExtractionAndResultConformityTest() throws IOException {
    String responseFilename = "multiple-pages-result";
    String responsePath = Thread.currentThread().getContextClassLoader().getResource("wiremock/__files/" + responseFilename + ".txt")
            .getPath();
    byte[] expectedResponseBytes = readFileToBytes(responsePath);

    httpMockServer.stubFor(get("/v2/ps?page=0&size=1")
            .willReturn(aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBodyFile("page1size1.json")));
    httpMockServer.stubFor(get("/v2/ps?page=1&size=1")
            .willReturn(aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBodyFile("page2size1.json")));
    httpMockServer.stubFor(get("/v2/ps?page=2&size=1")
            .willReturn(aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBodyFile("page3size1.json")));
    httpMockServer.stubFor(get("/v2/ps?page=3&size=1")
            .willReturn(aResponse()
                    .withStatus(410)));

    ResponseEntity<FileSystemResource> response = controller.generateExtractAndGetFile();

    ZipFile zipFile = new ZipFile(response.getBody().getFile());
    Enumeration<? extends ZipEntry> enumeration = zipFile.entries();
    InputStream stream = null;

    while (enumeration.hasMoreElements()) {
      ZipEntry zipEntry = enumeration.nextElement();
      stream = zipFile.getInputStream(zipEntry);
    }
    byte[] responseBytes = stream.readAllBytes();

    zipFile.close();
    stream.close();

    String expected = new String(expectedResponseBytes, StandardCharsets.UTF_8);
    String actual = new String(responseBytes, StandardCharsets.UTF_8);

    Assertions.assertEquals(expected, actual);
  }

  @Test
  void noPagesExtractionTest() throws IOException {
    httpMockServer.stubFor(get("/v2/ps?page=0&size=1")
            .willReturn(aResponse()
                    .withStatus(410)));

      ResponseEntity<FileSystemResource> response = controller.generateExtractAndGetFile();
      Assertions.assertThrows(NullPointerException.class, () -> {
        response.getBody().toString();
      });
  }

  @Test
  void emptyPsExtractionTest() throws IOException {
    httpMockServer.stubFor(get("/v2/ps?page=0&size=1")
            .willReturn(aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBodyFile("empty-ps.json")));

    ResponseEntity<FileSystemResource> response = controller.generateExtractAndGetFile();

    ZipFile zipFile = new ZipFile(response.getBody().getFile());
    Enumeration<? extends ZipEntry> enumeration = zipFile.entries();
    InputStream stream = null;

    while (enumeration.hasMoreElements()) {
      ZipEntry zipEntry = enumeration.nextElement();
      stream = zipFile.getInputStream(zipEntry);
    }
    byte[] responseBytes = stream.readAllBytes();

    zipFile.close();
    stream.close();

    String actual = new String(responseBytes, StandardCharsets.UTF_8);

    System.out.println(actual);
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
