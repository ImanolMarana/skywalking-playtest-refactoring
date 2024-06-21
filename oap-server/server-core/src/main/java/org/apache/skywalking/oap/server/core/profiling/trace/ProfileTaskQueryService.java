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

package org.apache.skywalking.oap.server.core.profiling.trace;

import com.google.common.base.Objects;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.protobuf.InvalidProtocolBufferException;
import io.vavr.Tuple;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.apm.network.language.agent.v3.SegmentObject;
import org.apache.skywalking.oap.server.core.CoreModule;
import org.apache.skywalking.oap.server.core.CoreModuleConfig;
import org.apache.skywalking.oap.server.core.analysis.IDManager;
import org.apache.skywalking.oap.server.core.analysis.manual.segment.SegmentRecord;
import org.apache.skywalking.oap.server.core.cache.NetworkAddressAliasCache;
import org.apache.skywalking.oap.server.core.config.IComponentLibraryCatalogService;
import org.apache.skywalking.oap.server.core.profiling.trace.analyze.ProfileAnalyzer;
import org.apache.skywalking.oap.server.core.query.input.SegmentProfileAnalyzeQuery;
import org.apache.skywalking.oap.server.core.query.type.KeyValue;
import org.apache.skywalking.oap.server.core.query.type.LogEntity;
import org.apache.skywalking.oap.server.core.query.type.ProfileAnalyzation;
import org.apache.skywalking.oap.server.core.query.type.ProfileTask;
import org.apache.skywalking.oap.server.core.query.type.ProfileTaskLog;
import org.apache.skywalking.oap.server.core.query.type.ProfiledTraceSegments;
import org.apache.skywalking.oap.server.core.query.type.ProfiledSpan;
import org.apache.skywalking.oap.server.core.query.type.Ref;
import org.apache.skywalking.oap.server.core.query.type.RefType;
import org.apache.skywalking.oap.server.core.storage.StorageModule;
import org.apache.skywalking.oap.server.core.storage.profiling.trace.IProfileTaskLogQueryDAO;
import org.apache.skywalking.oap.server.core.storage.profiling.trace.IProfileTaskQueryDAO;
import org.apache.skywalking.oap.server.core.storage.profiling.trace.IProfileThreadSnapshotQueryDAO;
import org.apache.skywalking.oap.server.core.storage.query.ITraceQueryDAO;
import org.apache.skywalking.oap.server.library.module.ModuleManager;
import org.apache.skywalking.oap.server.library.module.Service;
import org.apache.skywalking.oap.server.library.util.CollectionUtils;

import static java.util.Objects.isNull;

/**
 * handle profile task queries
 */
@Slf4j
public class ProfileTaskQueryService implements Service {
    private final ModuleManager moduleManager;
    private IProfileTaskQueryDAO profileTaskQueryDAO;
    private IProfileTaskLogQueryDAO profileTaskLogQueryDAO;
    private IProfileThreadSnapshotQueryDAO profileThreadSnapshotQueryDAO;
    private NetworkAddressAliasCache networkAddressAliasCache;
    private IComponentLibraryCatalogService componentLibraryCatalogService;
    private ITraceQueryDAO traceQueryDAO;

    private final ProfileAnalyzer profileAnalyzer;

    public ProfileTaskQueryService(ModuleManager moduleManager, CoreModuleConfig moduleConfig) {
        this.moduleManager = moduleManager;
        this.profileAnalyzer = new ProfileAnalyzer(
            moduleManager, moduleConfig.getMaxPageSizeOfQueryProfileSnapshot(),
            moduleConfig.getMaxSizeOfAnalyzeProfileSnapshot()
        );
    }

    private IProfileTaskQueryDAO getProfileTaskDAO() {
        if (isNull(profileTaskQueryDAO)) {
            this.profileTaskQueryDAO = moduleManager.find(StorageModule.NAME)
                                                    .provider()
                                                    .getService(IProfileTaskQueryDAO.class);
        }
        return profileTaskQueryDAO;
    }

    private IProfileTaskLogQueryDAO getProfileTaskLogQueryDAO() {
        if (isNull(profileTaskLogQueryDAO)) {
            profileTaskLogQueryDAO = moduleManager.find(StorageModule.NAME)
                                                  .provider()
                                                  .getService(IProfileTaskLogQueryDAO.class);
        }
        return profileTaskLogQueryDAO;
    }

    private IProfileThreadSnapshotQueryDAO getProfileThreadSnapshotQueryDAO() {
        if (isNull(profileThreadSnapshotQueryDAO)) {
            profileThreadSnapshotQueryDAO = moduleManager.find(StorageModule.NAME)
                                                         .provider()
                                                         .getService(IProfileThreadSnapshotQueryDAO.class);
        }
        return profileThreadSnapshotQueryDAO;
    }

    private NetworkAddressAliasCache getNetworkAddressAliasCache() {
        if (networkAddressAliasCache == null) {
            this.networkAddressAliasCache = moduleManager.find(CoreModule.NAME)
                                                         .provider()
                                                         .getService(NetworkAddressAliasCache.class);
        }
        return networkAddressAliasCache;
    }

    private IComponentLibraryCatalogService getComponentLibraryCatalogService() {
        if (componentLibraryCatalogService == null) {
            this.componentLibraryCatalogService = moduleManager.find(CoreModule.NAME)
                                                               .provider()
                                                               .getService(IComponentLibraryCatalogService.class);
        }
        return componentLibraryCatalogService;
    }

    private ITraceQueryDAO getTraceQueryDAO() {
        if (traceQueryDAO == null) {
            this.traceQueryDAO = moduleManager.find(StorageModule.NAME)
                                              .provider()
                                              .getService(ITraceQueryDAO.class);
        }
        return traceQueryDAO;
    }

    /**
     * search profile task list
     *
     * @param serviceId    monitor service
     * @param endpointName endpoint name to monitored
     */
    public List<ProfileTask> getTaskList(String serviceId, String endpointName) throws IOException {
        final List<ProfileTask> tasks = getProfileTaskDAO().getTaskList(serviceId, endpointName, null, null, null);

        // query all and filter on task to match logs
        List<ProfileTaskLog> taskLogList = getProfileTaskLogQueryDAO().getTaskLogList();
        if (taskLogList == null) {
            taskLogList = Collections.emptyList();
        }

        // add service name
        if (CollectionUtils.isNotEmpty(tasks)) {

            for (ProfileTask task : tasks) {
                final IDManager.ServiceID.ServiceIDDefinition serviceIDDefinition = IDManager.ServiceID.analysisId(
                    task.getServiceId());
                task.setServiceName(serviceIDDefinition.getName());

                // filter all task logs
                task.setLogs(findMatchedLogs(task.getId(), taskLogList));
            }
        }

        return tasks;
    }

    /**
     * query all task logs
     */
    public List<ProfileTaskLog> getProfileTaskLogs(final String taskID) throws IOException {
        // query all and filter on task to match logs
        List<ProfileTaskLog> taskLogList = getProfileTaskLogQueryDAO().getTaskLogList();
        if (CollectionUtils.isEmpty(taskLogList)) {
            return Collections.emptyList();
        }

        return findMatchedLogs(taskID, taskLogList);
    }

    public ProfileAnalyzation getProfileAnalyze(final List<SegmentProfileAnalyzeQuery> queries) throws IOException {
        return profileAnalyzer.analyze(queries);
    }

    public List<SegmentRecord> getTaskSegments(String taskId) throws IOException {
        final List<String> profiledSegmentIdList = getProfileThreadSnapshotQueryDAO().queryProfiledSegmentIdList(taskId);
        return getTraceQueryDAO().queryBySegmentIdList(profiledSegmentIdList);
    }

    public List<ProfiledTraceSegments> getProfileTaskSegments(String taskId) throws IOException {
        // query all profiled segments
        final List<String> profiledSegmentIdList = getProfileThreadSnapshotQueryDAO().queryProfiledSegmentIdList(taskId);
        final List<SegmentRecord> segmentRecords = getTraceQueryDAO().queryBySegmentIdList(profiledSegmentIdList);
        if (CollectionUtils.isEmpty(segmentRecords)) {
            return Collections.emptyList();
        }
        final Map<String, List<String>> traceWithInstances = segmentRecords.stream().collect(Collectors.toMap(
            SegmentRecord::getTraceId,
            s -> new ArrayList(List.of(s.getServiceInstanceId())),
            (s1, s2) -> {
                s1.addAll(s2);
                return s1;
            }));

        // query all profiled segments related segments(same traceId and instanceId)
        final Set<String> traceIdList = new HashSet<>(segmentRecords.size());
        final Set<String> instanceIdList = new HashSet<>(segmentRecords.size());
        for (SegmentRecord segment : segmentRecords) {
            traceIdList.add(segment.getTraceId());
            instanceIdList.add(segment.getServiceInstanceId());
        }
        final List<SegmentRecord> traceRelatedSegments = getTraceQueryDAO().queryByTraceIdWithInstanceId(
            new ArrayList<>(traceIdList),
            new ArrayList<>(instanceIdList));

        // group by the traceId + service instanceId
        final Map<String, List<SegmentRecord>> instanceTraceWithSegments = traceRelatedSegments.stream().filter(s -> {
            final List<String> includingInstances = traceWithInstances.get(s.getTraceId());
            return includingInstances.contains(s.getServiceInstanceId());
        }).collect(Collectors.toMap(
            s -> s.getTraceId() + s.getServiceInstanceId(),
            s -> new ArrayList<>(List.of(s)),
            (s1, s2) -> {
                s1.addAll(s2);
                return s1;
            }));

        // build result
        return instanceTraceWithSegments.values().stream()
            .flatMap(s -> buildProfiledSegmentsList(s, profiledSegmentIdList).stream())
            .collect(Collectors.toList());
    }

    protected List<ProfiledTraceSegments> buildProfiledSegmentsList(List<SegmentRecord> segmentRecords, List<String> profiledSegmentIdList) {
        final Map<String, ProfiledTraceSegments> segments = segmentRecords.stream()
                .map(this::parseSegmentRecord)
                .filter(java.util.Objects::nonNull)
                .filter(s -> CollectionUtils.isNotEmpty(s._2.getSpansList()))
                .collect(Collectors.toMap(
                        tuple -> tuple._1.getSegmentId(),
                        tuple -> createProfiledTraceSegments(tuple, profiledSegmentIdList),
                        (s1, s2) -> s1 // Keep first occurrence in case of duplicate keys
                ));

        return mergeSegmentsAndFilter(segments);
    }

    private Tuple<SegmentRecord, SegmentObject> parseSegmentRecord(SegmentRecord s) {
        try {
            return Tuple.of(s, SegmentObject.parseFrom(s.getDataBinary()));
        } catch (InvalidProtocolBufferException e) {
            log.warn("parsing segment data error", e);
            return null;
        }
    }

    private ProfiledTraceSegments createProfiledTraceSegments(Tuple<SegmentRecord, SegmentObject> tuple, List<String> profiledSegmentIdList) {
        final IDManager.ServiceInstanceID.InstanceIDDefinition serviceInstance = IDManager.ServiceInstanceID.analysisId(tuple._1.getServiceInstanceId());
        final ProfiledTraceSegments seg = new ProfiledTraceSegments();
        final boolean profiled = profiledSegmentIdList.contains(tuple._1.getSegmentId());
        seg.setTraceId(tuple._1.getTraceId());
        seg.setInstanceId(tuple._1.getServiceInstanceId());
        seg.setInstanceName(serviceInstance.getName());
        seg.getEndpointNames().add(IDManager.EndpointID.analysisId(tuple._1.getEndpointId()).getEndpointName());
        seg.setDuration(tuple._1.getLatency());
        seg.setStart(String.valueOf(tuple._1.getStartTime()));
        seg.getSpans().addAll(buildProfiledSpanList(tuple._2, profiled));
        seg.setContainsProfiled(profiled);
        return seg;
    }

    private List<ProfiledTraceSegments> mergeSegmentsAndFilter(Map<String, ProfiledTraceSegments> segments) {
        final ArrayList<ProfiledTraceSegments> results = new ArrayList<>();
        final Set<ProfiledSpan> mergedSpans = new HashSet<>();
        segments.entrySet().removeIf(entry -> {
            ProfiledTraceSegments current = entry.getValue();
            return mergeSegmentIfPossible(segments, results, mergedSpans, current);
        });
        results.addAll(segments.values());
        return results.stream().filter(ProfiledTraceSegments::isContainsProfiled).peek(this::removeAllCrossProcessRef).collect(Collectors.toList());
    }

    private boolean mergeSegmentIfPossible(Map<String, ProfiledTraceSegments> segments, ArrayList<ProfiledTraceSegments> results, Set<ProfiledSpan> mergedSpans, ProfiledTraceSegments current) {
        for (ProfiledSpan span : current.getSpans()) {
            if (mergedSpans.contains(span) || CollectionUtils.isEmpty(span.getRefs())) {
                continue;
            }
            // keep segment if ref type is not same process(analyze only match with the same process)
            final Ref ref = span.getRefs().get(0);
            if (RefType.CROSS_PROCESS.equals(ref.getType())) {
                results.add(current);
                return true;
            }
            // find parent segment if exist
            final ProfiledTraceSegments parentSegments = segments.get(ref.getParentSegmentId());
            if (parentSegments != null) {
                // append merged spans
                mergedSpans.addAll(current.getSpans());
                // add current segments into parent
                parentSegments.merge(current);
                return true;
            }
        }
        return false;
    }
//Refactoring end