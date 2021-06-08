/*
 * Copyright 2020 TiDB Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.tidb.bigdata.flink.tidb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.flink.table.factories.CatalogFactory;
import java.util.List;
import java.util.Map;

import static io.tidb.bigdata.tidb.ClientConfig.*;

public abstract class TiDBBaseCatalogFactory implements CatalogFactory {
    
    public static final String CATALOG_TYPE_VALUE_TIDB = "tidb";
    
    @Override
    public Map<String, String> requiredContext() {
        return ImmutableMap.of(
                "default-database", CATALOG_TYPE_VALUE_TIDB,
                "property-version", "1"
        );
    }
    
    @Override
    public List<String> supportedProperties() {
        return ImmutableList.of(
                USERNAME,
                PASSWORD,
                DATABASE_URL,
                MAX_POOL_SIZE,
                MIN_IDLE_SIZE,
                TIDB_WRITE_MODE,
                TIDB_REPLICA_READ,
                TIDB_FILTER_PUSH_DOWN
        );
    }
}
