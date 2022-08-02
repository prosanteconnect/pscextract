package fr.ans.psc.pscextract;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import fr.ans.psc.pscextract.controller.ExtractionController;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
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


import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.Objects;
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

  @BeforeEach
  private void Clean() {
    controller.cleanAll();
  }

  @Test
  void singlePageExtractionAndResultConformityTest() throws IOException {

    String responseFilename = "multiple-work-situations-result";
    String responsePath = Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("wiremock/__files/" + responseFilename + ".txt"))
            .getPath().substring(1);
    byte[] expectedResponseBytes = Files.readAllBytes(Paths.get(responsePath));

    httpMockServer.stubFor(get("/v2/ps?page=0&size=1")
            .willReturn(aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBodyFile("multiple-work-situations.json")));
    httpMockServer.stubFor(get("/v2/ps?page=1&size=1")
            .willReturn(aResponse()
                    .withStatus(420)));

    Assertions.assertThrows(org.springframework.mail.MailAuthenticationException.class, () -> controller.generateExtract(null));
    ResponseEntity<FileSystemResource> response = controller.getFile();

    ZipFile zipFile = new ZipFile(Objects.requireNonNull(response.getBody()).getFile());
    Enumeration<? extends ZipEntry> enumeration = zipFile.entries();
    InputStream stream = null;

    while (enumeration.hasMoreElements()) {
      ZipEntry zipEntry = enumeration.nextElement();
      stream = zipFile.getInputStream(zipEntry);
    }
    assert stream != null;
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
    String responsePath = Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("wiremock/__files/" + responseFilename + ".txt"))
            .getPath().substring(1);
    byte[] expectedResponseBytes = Files.readAllBytes(Paths.get(responsePath));

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

    Assertions.assertThrows(org.springframework.mail.MailAuthenticationException.class, () -> controller.generateExtract(null));
    ResponseEntity<FileSystemResource> response = controller.getFile();

    ZipFile zipFile = new ZipFile(Objects.requireNonNull(response.getBody()).getFile());
    Enumeration<? extends ZipEntry> enumeration = zipFile.entries();
    InputStream stream = null;

    while (enumeration.hasMoreElements()) {
      ZipEntry zipEntry = enumeration.nextElement();
      stream = zipFile.getInputStream(zipEntry);
    }
    assert stream != null;
    byte[] responseBytes = stream.readAllBytes();

    zipFile.close();
    stream.close();

    String expected = new String(expectedResponseBytes, StandardCharsets.UTF_8);
    String actual = new String(responseBytes, StandardCharsets.UTF_8);

    Assertions.assertEquals(expected, actual);
  }

  @Test
  void noPagesExtractionTest() {
    httpMockServer.stubFor(get("/v2/ps?page=0&size=1")
            .willReturn(aResponse()
                    .withStatus(410)));

    Assertions.assertThrows(org.springframework.mail.MailAuthenticationException.class, () -> controller.generateExtract(null));
    ResponseEntity<FileSystemResource> response = controller.getFile();
    Assertions.assertThrows(NullPointerException.class, () -> System.out.println(Objects.requireNonNull(response.getBody())));
  }

  @Test
  void emptyPsExtractionTest() throws IOException {

    String responseFilename = "empty-ps-result";
    String responsePath = Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("wiremock/__files/" + responseFilename + ".txt"))
            .getPath().substring(1);
    byte[] expectedResponseBytes = Files.readAllBytes(Paths.get(responsePath));

    httpMockServer.stubFor(get("/v2/ps?page=0&size=1")
            .willReturn(aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBodyFile("empty-ps.json")));
    httpMockServer.stubFor(get("/v2/ps?page=1&size=1")
            .willReturn(aResponse()
                    .withStatus(420)));

    Assertions.assertThrows(org.springframework.mail.MailAuthenticationException.class, () -> controller.generateExtract(null));
    ResponseEntity<FileSystemResource> response = controller.getFile();

    ZipFile zipFile = new ZipFile(Objects.requireNonNull(response.getBody()).getFile());
    Enumeration<? extends ZipEntry> enumeration = zipFile.entries();
    InputStream stream = null;

    while (enumeration.hasMoreElements()) {
      ZipEntry zipEntry = enumeration.nextElement();
      stream = zipFile.getInputStream(zipEntry);
    }
    assert stream != null;
    byte[] responseBytes = stream.readAllBytes();

    zipFile.close();
    stream.close();

    String expected = new String(expectedResponseBytes, StandardCharsets.UTF_8);
    String actual = new String(responseBytes, StandardCharsets.UTF_8);

    Assertions.assertEquals(expected, actual);
  }

  @Test
  void fullyEmptyPsExtractionTest() throws IOException {

    String responseFilename = "very-empty-ps-result";
    String responsePath = Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("wiremock/__files/" + responseFilename + ".txt"))
            .getPath().substring(1);
    byte[] expectedResponseBytes = Files.readAllBytes(Paths.get(responsePath));

    httpMockServer.stubFor(get("/v2/ps?page=0&size=1")
            .willReturn(aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBodyFile("very-empty-ps.json")));
    httpMockServer.stubFor(get("/v2/ps?page=1&size=1")
            .willReturn(aResponse()
                    .withStatus(420)));

    Assertions.assertThrows(org.springframework.mail.MailAuthenticationException.class, () -> controller.generateExtract(null));
    ResponseEntity<FileSystemResource> response = controller.getFile();

    ZipFile zipFile = new ZipFile(Objects.requireNonNull(response.getBody()).getFile());
    Enumeration<? extends ZipEntry> enumeration = zipFile.entries();
    InputStream stream = null;

    while (enumeration.hasMoreElements()) {
      ZipEntry zipEntry = enumeration.nextElement();
      stream = zipFile.getInputStream(zipEntry);
    }
    assert stream != null;
    byte[] responseBytes = stream.readAllBytes();

    zipFile.close();
    stream.close();

    String expected = new String(expectedResponseBytes, StandardCharsets.UTF_8);
    String actual = new String(responseBytes, StandardCharsets.UTF_8);

    Assertions.assertEquals(expected, actual);
  }

  @Test
  void generateExtractTest() throws IOException {
    String responseFilename = "multiple-pages-result";
    String responsePath = Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("wiremock/__files/" + responseFilename + ".txt"))
            .getPath().substring(1);
    byte[] expectedResponseBytes = Files.readAllBytes(Paths.get(responsePath));

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

    Assertions.assertThrows(org.springframework.mail.MailAuthenticationException.class, () -> controller.generateExtract(null));
    ResponseEntity<FileSystemResource> response = controller.getFile();

    ZipFile zipFile = new ZipFile(Objects.requireNonNull(response.getBody()).getFile());
    Enumeration<? extends ZipEntry> enumeration = zipFile.entries();
    InputStream stream = null;

    while (enumeration.hasMoreElements()) {
      ZipEntry zipEntry = enumeration.nextElement();
      stream = zipFile.getInputStream(zipEntry);
    }
    assert stream != null;
    byte[] responseBytes = stream.readAllBytes();

    zipFile.close();
    stream.close();

    String expected = new String(expectedResponseBytes, StandardCharsets.UTF_8);
    String actual = new String(responseBytes, StandardCharsets.UTF_8);

    Assertions.assertEquals(expected, actual);
  }
}
