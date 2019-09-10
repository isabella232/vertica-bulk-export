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
package io.cdap.plugin.db.batch.action.vertica.export;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;

import javax.annotation.Nullable;

/**
 * Vertica Export config
 */
public class VerticaExportConfig extends PluginConfig {
  public static final String CONNECTION_STRING = "connectionString";
  public static final String USER = "user";
  public static final String PASSWORD = "password";
  public static final String SELECT_STATEMENT = "selectStatement";
  public static final String DELIMITER = "delimiter";
  public static final String PATH = "path";

  private static final String CONNECTION_STRING_PREFIX = "jdbc:vertica://";

  @Name(CONNECTION_STRING)
  @Description("JDBC connection string including database name.")
  @Macro
  private String connectionString;

  @Name(USER)
  @Description("User to use to connect to the specified database. Required for databases that " +
    "need authentication. Optional for databases that do not require authentication.")
  @Nullable
  @Macro
  private String user;

  @Name(PASSWORD)
  @Description("Password to use to connect to the specified database. Required for databases that " +
    "need authentication. Optional for databases that do not require authentication.")
  @Nullable
  @Macro
  private String password;

  @Name(SELECT_STATEMENT)
  @Description("Select command to select values from a vertica table")
  @Macro
  @Nullable
  private String selectStatement;

  @Name(DELIMITER)
  @Description("Delimiter in the output file. Values in each column is separated by this delimiter while writing to" +
    " output file")
  @Nullable
  @Macro
  private String delimiter;

  @Name(PATH)
  @Description("HDFS File path where exported data will be written")
  @Macro
  private String path;

  public VerticaExportConfig(String connectionString, String user, String password, String selectStatement, String
    delimiter, String path) {
    this.connectionString = connectionString;
    this.user = user;
    this.password = password;
    this.selectStatement = selectStatement;
    this.delimiter = delimiter;
    this.path = path;
  }

  private VerticaExportConfig(Builder builder) {
    connectionString = builder.connectionString;
    user = builder.user;
    password = builder.password;
    selectStatement = builder.selectStatement;
    delimiter = builder.delimiter;
    path = builder.path;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(VerticaExportConfig copy) {
    return builder()
      .setConnectionString(copy.connectionString)
      .setUser(copy.user)
      .setPassword(copy.password)
      .setSelectStatement(copy.selectStatement)
      .setDelimiter(copy.delimiter)
      .setPath(copy.path);
  }

  public String getConnectionString() {
    return connectionString;
  }

  @Nullable
  public String getUser() {
    return user;
  }

  @Nullable
  public String getPassword() {
    return password;
  }

  @Nullable
  public String getSelectStatement() {
    return selectStatement;
  }

  @Nullable
  public String getDelimiter() {
    return delimiter;
  }

  public String getPath() {
    return path;
  }

  public void validate(FailureCollector failureCollector) {
    if (!containsMacro(PATH) && Strings.isNullOrEmpty(path)) {
      failureCollector.addFailure("File Path must be specified.",null)
        .withConfigProperty(PATH);
    }
    if (!containsMacro(CONNECTION_STRING)) {
      if (Strings.isNullOrEmpty(connectionString)) {
        failureCollector.addFailure("Connection String must be specified.", null)
          .withConfigProperty(CONNECTION_STRING);
      } else if (!connectionString.startsWith(CONNECTION_STRING_PREFIX)) {
        failureCollector.addFailure(
          "Connection String has incorrect format.",
          "Ensure the connection string is of format jdbc:vertica://<VerticaHost>:<portNumber>/<databaseName>")
          .withConfigProperty(CONNECTION_STRING);
      }
    }
    if (!containsMacro(USER) || !containsMacro(PASSWORD)) {
      if (Strings.isNullOrEmpty(user) && !Strings.isNullOrEmpty(password)) {
        failureCollector.addFailure(
          "Username is not specified.",
          "Please provide both Username and Password if database requires " +
            "authentication. If not, please remove Password and retry.")
          .withConfigProperty(USER).withConfigProperty(PASSWORD);
      }
      if (!Strings.isNullOrEmpty(user) && Strings.isNullOrEmpty(password)) {
        failureCollector.addFailure(
          "Password is not specified.",
          "Please provide both user Username and Password if database requires " +
            "authentication. If not, please remove Username and retry.")
          .withConfigProperty(USER).withConfigProperty(PASSWORD);
      }
    }
  }

  /**
   * Builder for creating a {@link VerticaExportConfig}.
   */
  public static final class Builder {
    private String connectionString;
    private String user;
    private String password;
    private String selectStatement;
    private String delimiter;
    private String path;

    private Builder() {
    }

    public Builder setConnectionString(String connectionString) {
      this.connectionString = connectionString;
      return this;
    }

    public Builder setUser(String user) {
      this.user = user;
      return this;
    }

    public Builder setPassword(String password) {
      this.password = password;
      return this;
    }

    public Builder setSelectStatement(String selectStatement) {
      this.selectStatement = selectStatement;
      return this;
    }

    public Builder setDelimiter(String delimiter) {
      this.delimiter = delimiter;
      return this;
    }

    public Builder setPath(String path) {
      this.path = path;
      return this;
    }

    public VerticaExportConfig build() {
      return new VerticaExportConfig(this);
    }
  }
}
