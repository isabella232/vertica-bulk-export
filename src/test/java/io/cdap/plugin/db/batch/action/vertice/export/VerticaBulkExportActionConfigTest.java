/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.db.batch.action.vertice.export;

import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.db.batch.action.vertica.export.VerticaExportConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Unit tests for {@link VerticaExportConfig}
 */
public class VerticaBulkExportActionConfigTest {

  private static final String MOCK_STAGE = "mockStage";
  private static final VerticaExportConfig VALID_CONFIG = new VerticaExportConfig(
    "jdbc:vertica://localhost:5433/test",
    "dbadmin",
    "testpassword",
    "select * from testTable",
    ",",
    "/tmp/vertica/vertice_test.csv");

  @Test
  public void testValidConfig() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateConnectionStringNull() {
    VerticaExportConfig config = VerticaExportConfig.builder(VALID_CONFIG)
      .setConnectionString(null)
      .build();
    List<List<String>> paramName =
      Collections.singletonList(Collections.singletonList(VerticaExportConfig.CONNECTION_STRING));

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, paramName);
  }

  @Test
  public void testValidateConnectionStringEmpty() {
    VerticaExportConfig config = VerticaExportConfig.builder(VALID_CONFIG)
      .setConnectionString("")
      .build();
    List<List<String>> paramName =
      Collections.singletonList(Collections.singletonList(VerticaExportConfig.CONNECTION_STRING));

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, paramName);
  }

  @Test
  public void testValidateIncorrectConnectionString() {
    VerticaExportConfig config = VerticaExportConfig.builder(VALID_CONFIG)
      .setConnectionString("jdbc:connect_my")
      .build();
    List<List<String>> paramName =
      Collections.singletonList(Collections.singletonList(VerticaExportConfig.CONNECTION_STRING));

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, paramName);
  }

  @Test
  public void testValidatePathNull() {
    VerticaExportConfig config = VerticaExportConfig.builder(VALID_CONFIG)
      .setPath(null)
      .build();
    List<List<String>> paramName = Collections.singletonList(Collections.singletonList(VerticaExportConfig.PATH));

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, paramName);
  }

  @Test
  public void testValidatePathEmpty() {
    VerticaExportConfig config = VerticaExportConfig.builder(VALID_CONFIG)
      .setPath("")
      .build();
    List<List<String>> paramName = Collections.singletonList(Collections.singletonList(VerticaExportConfig.PATH));

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, paramName);
  }

  @Test
  public void testValidateUserNullAndPasswordNotNull() {
    VerticaExportConfig config = VerticaExportConfig.builder(VALID_CONFIG)
      .setUser(null)
      .build();
    List<List<String>> paramName = Collections.singletonList(
      Arrays.asList(VerticaExportConfig.USER, VerticaExportConfig.PASSWORD));

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, paramName);
  }

  @Test
  public void testValidateUserEmptyAndPasswordNotNull() {
    VerticaExportConfig config = VerticaExportConfig.builder(VALID_CONFIG)
      .setUser("")
      .build();
    List<List<String>> paramName = Collections.singletonList(
      Arrays.asList(VerticaExportConfig.USER, VerticaExportConfig.PASSWORD));

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, paramName);
  }

  @Test
  public void testValidateUserNotNullAndPasswordNull() {
    VerticaExportConfig config = VerticaExportConfig.builder(VALID_CONFIG)
      .setPassword(null)
      .build();
    List<List<String>> paramName = Collections.singletonList(
      Arrays.asList(VerticaExportConfig.USER, VerticaExportConfig.PASSWORD));

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, paramName);
  }

  @Test
  public void testValidateUserNotNullAndPasswordEmpty() {
    VerticaExportConfig config = VerticaExportConfig.builder(VALID_CONFIG)
      .setPassword("")
      .build();
    List<List<String>> paramName = Collections.singletonList(
      Arrays.asList(VerticaExportConfig.USER, VerticaExportConfig.PASSWORD));

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, paramName);
  }

  @Test
  public void testValidateSeveralFailedConfig() {
    VerticaExportConfig config = VerticaExportConfig.builder(VALID_CONFIG)
      .setConnectionString(null)
      .setPath(null)
      .build();
    List<List<String>> paramName = Arrays.asList(
      Collections.singletonList(VerticaExportConfig.PATH),
      Collections.singletonList(VerticaExportConfig.CONNECTION_STRING));

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, paramName);
  }

  private static void assertValidationFailed(MockFailureCollector failureCollector, List<List<String>> paramNames) {
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();
    Assert.assertEquals(paramNames.size(), failureList.size());
    Iterator<List<String>> paramNameIterator = paramNames.iterator();
    failureList.stream().map(failure -> failure.getCauses()
      .stream()
      .filter(cause -> cause.getAttribute(CauseAttributes.STAGE_CONFIG) != null)
      .collect(Collectors.toList()))
      .filter(causeList -> paramNameIterator.hasNext())
      .forEach(causeList -> {
        List<String> parameters = paramNameIterator.next();
        Assert.assertEquals(parameters.size(), causeList.size());
        IntStream.range(0, parameters.size()).forEach(i -> {
          ValidationFailure.Cause cause = causeList.get(i);
          Assert.assertEquals(parameters.get(i), cause.getAttribute(CauseAttributes.STAGE_CONFIG));
        });
      });
  }
}
