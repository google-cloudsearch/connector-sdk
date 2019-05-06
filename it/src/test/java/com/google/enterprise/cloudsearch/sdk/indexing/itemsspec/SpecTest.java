package com.google.enterprise.cloudsearch.sdk.indexing.itemsspec;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for Spec.
 */
public class SpecTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void toJson_happyPath() throws IOException {
    File specFile = folder.newFile();
    Spec.toJson(FULL_SPEC, specFile.getPath());
    String contents = String.join("", Files.readAllLines(specFile.toPath()));
    assertEquals(FULL_SPEC_JSON_STRING, contents);
  }

  @Test
  public void toJson_emptyFields_nullsAndEmptiesNotSerialized() throws IOException {
    Spec spec = new Spec.Builder()
        .setItems(ImmutableList.of(
            new SpecItem.Builder("MockName").build()))
        .build();
    File specFile = folder.newFile();
    Spec.toJson(spec, specFile.getPath());
    String contents = String.join("", Files.readAllLines(specFile.toPath()));
    assertEquals("{  \"items\" : [ {    \"name\" : \"MockName\"  } ]}", contents);
  }

  @Test
  public void fromJson() throws IOException {
    File specFile = folder.newFile();
    Files.write(specFile.toPath(), FULL_SPEC_JSON_STRING.getBytes());
    Spec spec = Spec.fromJson(specFile.getPath());
    assertEquals(FULL_SPEC, spec);
  }

  private static final Spec FULL_SPEC = new Spec.Builder()
      .setItems(ImmutableList.of(
          new SpecItem.Builder("MockName")
              .setMetadata(new SpecMetadata.Builder()
                  .setMimeType("MOCK_TYPE")
                  .setContentLanguage("MOCK_LANGUAGE")
                  .setSourceRepositoryUrl("MOCK_URL")
                  .setTitle("MOCK_TITLE")
                  .build())
              .setUserAccess(new SpecUserAccess.Builder()
                  .setAllowed(Collections.singletonList("MOCK_ALLOWED"))
                  .setDenied(Collections.singletonList("MOCK_DENIED"))
                  .build())
              .setReaders(Collections.singletonList("MOCK_READER"))
              .setStructuredData(Collections.singletonMap("MOCK_KEY1", "MOCK_VAL1"))
              .build()))
      .build();

  private static final String FULL_SPEC_JSON_STRING =
      "{"
          + "  \"items\" : ["
          + " {"
          + "    \"name\" : \"MockName\","
          + "    \"metadata\" : {"
          + "      \"title\" : \"MOCK_TITLE\","
          + "      \"source_repository_url\" : \"MOCK_URL\","
          + "      \"mime_type\" : \"MOCK_TYPE\","
          + "      \"content_language\" : \"MOCK_LANGUAGE\""
          + "    },"
          + "    \"structured_data\" : {"
          + "      \"MOCK_KEY1\" : \"MOCK_VAL1\""
          + "    },"
          + "    \"readers\" : [ \"MOCK_READER\" ],"
          + "    \"user_access\" : {"
          + "      \"allowed\" : [ \"MOCK_ALLOWED\" ],"
          + "      \"denied\" : [ \"MOCK_DENIED\" ]"
          + "    }"
          + "  }"
          + " ]}";
}