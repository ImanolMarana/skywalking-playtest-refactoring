/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.oap.server.storage.plugin.elasticsearch.base;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.library.elasticsearch.exception.ResponseException;
import org.apache.skywalking.oap.server.core.analysis.DownSampling;
import org.apache.skywalking.oap.server.core.storage.IHistoryDeleteDAO;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.library.client.elasticsearch.ElasticSearchClient;
import org.joda.time.DateTime;

@Slf4j
public class HistoryDeleteEsDAO extends EsDAO implements IHistoryDeleteDAO {
    private final Map<String, Long> indexLatestSuccess;

    public HistoryDeleteEsDAO(ElasticSearchClient client) {
        super(client);
        this.indexLatestSuccess = new HashMap<>();
    }

    @Override
    public void deleteHistory(Model model, String timeBucketColumnName, int ttl) {
        ElasticSearchClient client = getClient();

        if (shouldSkipDeletion(model)) {
            return;
        }

        long deadline = Long.parseLong(new DateTime().plusDays(-ttl).toString("yyyyMMdd"));
        String tableName = IndexController.INSTANCE.getTableName(model);

        if (skipDeletionBasedOnLatestSuccess(tableName, deadline)) {
            return;
        }

        ensureLatestIndexExists(client, model);

        Collection<String> indices = client.retrievalIndexByAliases(tableName);
        if (log.isDebugEnabled()) {
            log.debug("Deadline = {}, indices = {}, ttl = {}", deadline, indices, ttl);
        }

        List<String> prepareDeleteIndexes = findIndicesToDelete(indices, deadline);
        if (log.isDebugEnabled()) {
            log.debug("Indices to be deleted: {}", prepareDeleteIndexes);
        }

        deleteIndices(client, prepareDeleteIndexes);
        this.indexLatestSuccess.put(tableName, deadline);
    }
    
    private boolean shouldSkipDeletion(Model model) {
        if (!model.isRecord()) {
            if (!DownSampling.Minute.equals(model.getDownsampling())) {
                /*
                 * In ElasticSearch storage, the TTL triggers the index deletion directly.
                 * As all metrics data in different down sampling rule of one day are in the same index, the deletion operation
                 * is only required to run once.
                 */
                return true;
            }
        }
        return false;
    }

    private boolean skipDeletionBasedOnLatestSuccess(String tableName, long deadline) {
        Long latestSuccessDeadline = this.indexLatestSuccess.get(tableName);
        if (latestSuccessDeadline != null && deadline <= latestSuccessDeadline) {
            if (log.isDebugEnabled()) {
                log.debug("Index = {} already deleted, skip, deadline = {}, ttl = {}", tableName, deadline, ttl);
            }
            return true;
        }
        return false;
    }

    private void ensureLatestIndexExists(ElasticSearchClient client, Model model) {
        String latestIndex = TimeSeriesUtils.latestWriteIndexName(model);
        if (!client.isExistsIndex(latestIndex)) {
            try {
                client.createIndex(latestIndex);
                if (log.isDebugEnabled()) {
                    log.debug("Latest index = {} is not exist, create.", latestIndex);
                }
            } catch (ResponseException e) {
                if (e.getStatusCode() == 400 && client.isExistsIndex(latestIndex)) {
                    if (log.isDebugEnabled()) {
                        log.debug("Failed to create index {}, index is already created.", latestIndex);
                    }
                } else {
                    throw e;
                }
            }
        }
    }

    private List<String> findIndicesToDelete(Collection<String> indices, long deadline) {
        List<String> prepareDeleteIndexes = new ArrayList<>();
        for (String index : indices) {
            long timeSeries = TimeSeriesUtils.isolateTimeFromIndexName(index);
            if (deadline >= timeSeries) {
                prepareDeleteIndexes.add(index);
            }
        }
        return prepareDeleteIndexes;
    }

    private void deleteIndices(ElasticSearchClient client, List<String> prepareDeleteIndexes) {
        for (String prepareDeleteIndex : prepareDeleteIndexes) {
            client.deleteByIndexName(prepareDeleteIndex);
        }
    }

//Refactoring end
}
