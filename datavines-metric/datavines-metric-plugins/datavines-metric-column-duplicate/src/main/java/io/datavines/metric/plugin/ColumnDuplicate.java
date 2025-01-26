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
package io.datavines.metric.plugin;

import io.datavines.common.enums.DataVinesDataType;
import io.datavines.metric.api.MetricDimension;
import io.datavines.metric.api.MetricDirectionType;
import io.datavines.metric.api.MetricType;
import io.datavines.metric.plugin.base.BaseSingleTableColumnNotUseView;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ColumnDuplicate extends BaseSingleTableColumnNotUseView {

    public ColumnDuplicate(){
        super();
        invalidateItemsSql = new StringBuilder("select ${column} from ${table}");
    }

    @Override
    public String getName() {
        return "column_duplicate";
    }

    @Override
    public String getZhName() {
        return "重复值检查";
    }

    @Override
    public MetricDimension getDimension() {
        return MetricDimension.UNIQUENESS;
    }

    @Override
    public MetricType getType() {
        return MetricType.SINGLE_TABLE;
    }

    @Override
    public boolean isInvalidateItemsCanOutput() {
        return true;
    }

    @Override
    public void prepare(Map<String, String> config) {

        if (config.containsKey("filter") && StringUtils.isNotBlank(config.get("filter"))) {
            invalidateItemsSql.append(" where ").append(config.get("filter"));
        }

        if (config.containsKey("column")) {
            invalidateItemsSql.append(getConnectorFactory(config).getMetricScript().groupByHavingCountForDuplicate());
        }
    }

    @Override
    public List<DataVinesDataType> suitableType() {
        return Arrays.asList(DataVinesDataType.NUMERIC_TYPE, DataVinesDataType.STRING_TYPE, DataVinesDataType.DATE_TIME_TYPE);
    }

    @Override
    public MetricDirectionType getDirectionType() {
        return MetricDirectionType.NEGATIVE;
    }
}
