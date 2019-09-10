/*
 * Copyright © 2017 Cask Data, Inc.
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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Runs a select query after a pipeline run.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("VerticaBulkExportAction")
@Description("Vertica export plugin")
public class VerticaBulkExportAction extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(VerticaBulkExportAction.class);
  private final VerticaExportConfig config;

  public VerticaBulkExportAction(VerticaExportConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(failureCollector);
  }

  @Override
  public void run(ActionContext context) throws Exception {
    FailureCollector failureCollector = context.getFailureCollector();
    config.validate(failureCollector);
    failureCollector.getOrThrowException();

    Object driver = Class.forName("com.vertica.jdbc.Driver").newInstance();
    DriverManager.registerDriver((Driver) driver);

    try (Connection connection = DriverManager.getConnection(config.getConnectionString(), config.getUser(),
                                                             config.getPassword())) {
      Statement statement = connection.createStatement();
      ResultSet resultSet = statement.executeQuery(config.getSelectStatement());
      ResultSetMetaData metadata = resultSet.getMetaData();
      int columnCount = metadata.getColumnCount();

      Configuration conf = new Configuration();
      FileSystem fileSystem = FileSystem.get(conf);
      Path exportFile = new Path(config.getPath());
      Path exportDir = exportFile.getParent();
      fileSystem.mkdirs(exportDir);
      BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fileSystem.create(exportFile, false)));

      // write columns to file
      List<String> values = new ArrayList<>();
      for (int i = 1; i <= columnCount; i++) {
        values.add(metadata.getColumnName(i));
      }

      br.write(StringUtils.join(values, config.getDelimiter()));
      br.newLine();

      while (resultSet.next()) {
        List<String> rowValues = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
          // if the column value is null, then use "null" string while writing to file
          if (resultSet.getString(i) == null) {
            rowValues.add("null");
          } else {
            rowValues.add(resultSet.getString(i));
          }
        }

        br.write(StringUtils.join(rowValues, config.getDelimiter()));
        br.newLine();
      }

      br.close();
      DriverManager.deregisterDriver((Driver) driver);
    }
  }

}
