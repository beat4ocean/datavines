/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.datavines.engine.spark.config;

import io.datavines.common.config.EnvConfig;
import io.datavines.common.config.SinkConfig;
import io.datavines.common.config.SourceConfig;
import io.datavines.common.config.enums.SinkType;
import io.datavines.common.config.enums.SourceType;
import io.datavines.common.entity.ConnectorParameter;
import io.datavines.common.entity.job.BaseJobParameter;
import io.datavines.common.exception.DataVinesException;
import io.datavines.common.utils.JSONUtils;
import io.datavines.common.utils.StringUtils;
import io.datavines.connector.api.ConnectorFactory;
import io.datavines.engine.common.utils.ParserUtils;
import io.datavines.engine.config.BaseJobConfigurationBuilder;
import io.datavines.metric.api.ExpectedValue;
import io.datavines.spi.PluginLoader;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.util.*;
import java.util.stream.Collectors;

import static io.datavines.common.CommonConstants.*;
import static io.datavines.common.ConfigConstants.*;
import static io.datavines.common.ConfigConstants.TABLE;

@Slf4j
public abstract class BaseSparkConfigurationBuilder extends BaseJobConfigurationBuilder {

    @Override
    protected EnvConfig getEnvConfig() {
        EnvConfig envConfig = new EnvConfig();
        envConfig.setEngine(jobExecutionInfo.getEngineType());
        Map<String,Object> configMap = envConfig.getConfig();
        if (configMap == null) {
            configMap = new HashMap<>();
        }

        ConnectorParameter connectorParameter = jobExecutionParameter.getConnectorParameter();
        String srcConnectorType = "";
        boolean isEnableSparkHiveSupport = false;
        if (connectorParameter != null) {
            srcConnectorType = connectorParameter.getType();
            if (connectorParameter.getParameters().get(ENABLE_SPARK_HIVE_SUPPORT) != null) {
                isEnableSparkHiveSupport = Boolean.parseBoolean((String) connectorParameter.getParameters().get(ENABLE_SPARK_HIVE_SUPPORT));
            }
        }

        ConnectorParameter connectorParameter2 = jobExecutionParameter.getConnectorParameter2();

        String srcConnectorType2 = "";
        boolean isEnableSparkHiveSupport2 = false;
        if (connectorParameter2 != null) {
            srcConnectorType2 = connectorParameter2.getType();
            if (connectorParameter2.getParameters().get(ENABLE_SPARK_HIVE_SUPPORT) != null) {
                isEnableSparkHiveSupport2 = Boolean.parseBoolean((String) connectorParameter2.getParameters().get(ENABLE_SPARK_HIVE_SUPPORT));
            }
        }

        if ((HIVE.equalsIgnoreCase(srcConnectorType) && isEnableSparkHiveSupport) ||
                (HIVE.equalsIgnoreCase(srcConnectorType2) && isEnableSparkHiveSupport2)) {
            configMap.put(ENABLE_SPARK_HIVE_SUPPORT, Boolean.TRUE);
        }

        envConfig.setConfig(configMap);
        return envConfig;
    }

    @Override
    protected List<SourceConfig> getSourceConfigs() throws DataVinesException {
        List<SourceConfig> sourceConfigs = new ArrayList<>();
        List<BaseJobParameter> metricJobParameterList = jobExecutionParameter.getMetricParameterList();
        boolean isAddValidateResultDataSource = false;
        if (CollectionUtils.isNotEmpty(metricJobParameterList)) {
            Set<String> sourceConnectorSet = new HashSet<>();
            Set<String> targetConnectorSet = new HashSet<>();
            for (BaseJobParameter parameter : metricJobParameterList) {
                String metricUniqueKey = getMetricUniqueKey(parameter);
                Map<String, String> metricInputParameter = metric2InputParameter.get(metricUniqueKey);
                if (jobExecutionParameter.getConnectorParameter() != null) {
                    ConnectorParameter connectorParameter = jobExecutionParameter.getConnectorParameter();
                    SourceConfig sourceConfig = new SourceConfig();

                    Map<String, Object> connectorParameterMap = new HashMap<>(connectorParameter.getParameters());
                    connectorParameterMap.putAll(metricInputParameter);

                    if (connectorParameter.getParameters().get(SCHEMA) != null) {
                        metricInputParameter.put(SCHEMA, (String)connectorParameter.getParameters().get(SCHEMA));
                    }

                    metricInputParameter.put(DATABASE_NAME,metricInputParameter.get(DATABASE));
                    metricInputParameter.put(TABLE_NAME,metricInputParameter.get(TABLE));
                    metricInputParameter.put(COLUMN_NAME,metricInputParameter.get(COLUMN));

                    ConnectorFactory connectorFactory = PluginLoader
                            .getPluginLoader(ConnectorFactory.class)
                            .getNewPlugin(connectorParameter.getType());

                    connectorParameterMap.put(TABLE, metricInputParameter.get(TABLE));
                    connectorParameterMap.put(DATABASE, metricInputParameter.get(DATABASE));
                    connectorParameterMap = connectorFactory.getConnectorParameterConverter().converter(connectorParameterMap);
                    connectorParameterMap.put(PASSWORD, ParserUtils.encode((String)connectorParameterMap.get(PASSWORD)));

                    String outputTable = getOutputTable(metricInputParameter.get(DATABASE), metricInputParameter.get(SCHEMA), metricInputParameter.get(TABLE));
                    String tableAlias = getTableAlias(metricInputParameter.get(DATABASE), metricInputParameter.get(SCHEMA), metricInputParameter.get(TABLE), "1");
                    connectorParameterMap.put(OUTPUT_TABLE, outputTable);
                    connectorParameterMap.put(DRIVER, connectorFactory.getDialect().getDriver());

                    metricInputParameter.put(TABLE, outputTable);
                    metricInputParameter.put(TABLE_ALIAS, tableAlias);
                    metricInputParameter.put(COLUMN, metricInputParameter.get(COLUMN));
                    metricInputParameter.put(SRC_CONNECTOR_TYPE, connectorParameter.getType());
                    metricInputParameter.put(ENGINE_TYPE, SPARK);

                    String connectorUUID = connectorFactory.getConnectorParameterConverter().getConnectorUUID(connectorParameterMap);

                    if (sourceConnectorSet.contains(connectorUUID)) {
                        continue;
                    }

                    sourceConfig.setPlugin(connectorFactory.getCategory());
                    sourceConfig.setConfig(connectorParameterMap);
                    sourceConfig.setType(SourceType.SOURCE.getDescription());
                    sourceConfigs.add(sourceConfig);
                    sourceConnectorSet.add(connectorUUID);
                }

                if (jobExecutionParameter.getConnectorParameter2() != null
                        && jobExecutionParameter.getConnectorParameter2().getParameters() !=null) {
                    ConnectorParameter connectorParameter2 = jobExecutionParameter.getConnectorParameter2();
                    SourceConfig sourceConfig = new SourceConfig();

                    Map<String, Object> connectorParameterMap = new HashMap<>(connectorParameter2.getParameters());
                    connectorParameterMap.putAll(metricInputParameter);

                    if (connectorParameter2.getParameters().get(SCHEMA) != null) {
                        metricInputParameter.put(SCHEMA2, (String)connectorParameter2.getParameters().get(SCHEMA));
                    }

                    ConnectorFactory connectorFactory = PluginLoader
                            .getPluginLoader(ConnectorFactory.class)
                            .getNewPlugin(connectorParameter2.getType());

                    connectorParameterMap.put(TABLE, metricInputParameter.get(TABLE2));
                    connectorParameterMap.put(DATABASE, metricInputParameter.get(DATABASE2));
                    connectorParameterMap = connectorFactory.getConnectorParameterConverter().converter(connectorParameterMap);
                    connectorParameterMap.put(PASSWORD, ParserUtils.encode((String)connectorParameterMap.get(PASSWORD)));

                    String outputTable = getOutputTable(metricInputParameter.get(DATABASE2),
                                                        metricInputParameter.get(SCHEMA2),
                                                        metricInputParameter.get(TABLE2)) + "_2";
                    String tableAlias2 = getTableAlias(metricInputParameter.get(DATABASE2),
                                                        metricInputParameter.get(SCHEMA2),
                                                        metricInputParameter.get(TABLE2) ,"2");
                    connectorParameterMap.put(OUTPUT_TABLE, outputTable);
                    connectorParameterMap.put(DRIVER, connectorFactory.getDialect().getDriver());
                    metricInputParameter.put(TABLE2, outputTable);
                    metricInputParameter.put(TABLE2_ALIAS, tableAlias2);

                    String connectorUUID = connectorFactory.getConnectorParameterConverter().getConnectorUUID(connectorParameterMap);
                    if (targetConnectorSet.contains(connectorUUID)) {
                        continue;
                    }

                    sourceConfig.setPlugin(connectorFactory.getCategory());
                    sourceConfig.setConfig(connectorParameterMap);
                    sourceConfig.setType(SourceType.TARGET.getDescription());
                    sourceConfigs.add(sourceConfig);
                    targetConnectorSet.add(connectorUUID);
                }

                metricInputParameter.put("actual_value", "actual_value_" + metricUniqueKey);

                String expectedType = jobExecutionInfo.getEngineType() + "_" + parameter.getExpectedType();

                ExpectedValue expectedValue = PluginLoader
                        .getPluginLoader(ExpectedValue.class)
                        .getNewPlugin(expectedType);

                if (expectedValue.isNeedDefaultDatasource() && !isAddValidateResultDataSource) {
                    sourceConfigs.add(getValidateResultDataSourceConfig());
                    isAddValidateResultDataSource = true;
                }

                metric2InputParameter.put(metricUniqueKey, metricInputParameter);
            }
        }

        return sourceConfigs;
    }

    protected SinkConfig getErrorSinkConfig(Map<String, String> inputParameter) {
        SinkConfig errorDataSinkConfig = null;
        if (StringUtils.isNotEmpty(jobExecutionInfo.getErrorDataStorageType())
                && StringUtils.isNotEmpty(jobExecutionInfo.getErrorDataStorageParameter())) {
            errorDataSinkConfig = new SinkConfig();
            errorDataSinkConfig.setType(SinkType.ERROR_DATA.getDescription());

            Map<String, Object> connectorParameterMap = new HashMap<>(JSONUtils.toMap(jobExecutionInfo.getErrorDataStorageParameter(),String.class, Object.class));
            ConnectorFactory connectorFactory = PluginLoader
                    .getPluginLoader(ConnectorFactory.class)
                    .getNewPlugin(jobExecutionInfo.getErrorDataStorageType());

            if (connectorFactory == null) {
                return null;
            }

            String errorDataOutputToDataSourceDatabase = String.valueOf(connectorParameterMap.get(ERROR_DATA_OUTPUT_TO_DATASOURCE_DATABASE));

            if (connectorParameterMap.get(ERROR_DATA_OUTPUT_TO_DATASOURCE_DATABASE) == null) {
                connectorParameterMap = connectorFactory.getConnectorParameterConverter().converter(connectorParameterMap);
            } else {
                List<SourceConfig> sourceConfigs = getSourceConfigs()
                        .stream().filter(x->SourceType.SOURCE.getDescription().equalsIgnoreCase(x.getType())).collect(Collectors.toList());
                if (CollectionUtils.isEmpty(sourceConfigs)) {
                    return null;
                }

                connectorParameterMap = sourceConfigs.get(0).getConfig();
                connectorParameterMap.put(DATABASE, errorDataOutputToDataSourceDatabase);
            }

            errorDataSinkConfig.setPlugin(connectorFactory.getCategory());
            connectorParameterMap.put(DRIVER, connectorFactory.getDialect().getDriver());
            connectorParameterMap.put(ERROR_DATA_FILE_NAME, jobExecutionInfo.getErrorDataFileName());
            connectorParameterMap.put(TABLE, jobExecutionInfo.getErrorDataFileName());
            connectorParameterMap.put(SQL, "SELECT * FROM "+ inputParameter.get(INVALIDATE_ITEMS_TABLE));
            errorDataSinkConfig.setConfig(connectorParameterMap);
        }

        return errorDataSinkConfig;
    }

    public String getOutputTable(String database, String schema, String table) {

        if (!StringUtils.isEmptyOrNullStr(schema)) {
            table = schema + "_" + table;
        }

        if (!StringUtils.isEmptyOrNullStr(database)) {
            table = database + "_" + table;
        }

        return table;
    }

    public String getTableAlias(String database, String schema, String table, String order) {

        if (!StringUtils.isEmptyOrNullStr(schema)) {
            table = schema + "_" + table;
        }

        if (!StringUtils.isEmptyOrNullStr(database)) {
            table = database + "_" + table;
        }

        return table+ "_" + order;
    }
}
