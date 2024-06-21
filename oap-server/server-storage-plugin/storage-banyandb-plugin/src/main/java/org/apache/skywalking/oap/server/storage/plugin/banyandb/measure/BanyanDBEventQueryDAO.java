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

package org.apache.skywalking.oap.server.storage.plugin.banyandb.measure;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.skywalking.banyandb.v1.client.AbstractCriteria;
import org.apache.skywalking.banyandb.v1.client.DataPoint;
import org.apache.skywalking.banyandb.v1.client.MeasureQuery;
import org.apache.skywalking.banyandb.v1.client.MeasureQueryResponse;
import org.apache.skywalking.banyandb.v1.client.PairQueryCondition;
import org.apache.skywalking.oap.server.core.analysis.Layer;
import org.apache.skywalking.oap.server.core.query.PaginationUtils;
import org.apache.skywalking.oap.server.core.query.enumeration.Order;
import org.apache.skywalking.oap.server.core.query.input.Duration;
import org.apache.skywalking.oap.server.core.query.type.event.EventQueryCondition;
import org.apache.skywalking.oap.server.core.query.type.event.EventType;
import org.apache.skywalking.oap.server.core.query.type.event.Events;
import org.apache.skywalking.oap.server.core.query.type.event.Source;
import org.apache.skywalking.oap.server.core.analysis.metrics.Event;
import org.apache.skywalking.oap.server.core.storage.query.IEventQueryDAO;
import org.apache.skywalking.oap.server.storage.plugin.banyandb.BanyanDBStorageClient;
import org.apache.skywalking.oap.server.storage.plugin.banyandb.stream.AbstractBanyanDBDAO;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.isNull;

public class BanyanDBEventQueryDAO extends AbstractBanyanDBDAO implements IEventQueryDAO {
    private static final Set<String> TAGS = ImmutableSet.of(
            Event.UUID, Event.SERVICE, Event.SERVICE_INSTANCE, Event.ENDPOINT, Event.NAME,
            Event.MESSAGE, Event.TYPE, Event.START_TIME, Event.END_TIME, Event.PARAMETERS, Event.LAYER);

    public BanyanDBEventQueryDAO(final BanyanDBStorageClient client) {
        super(client);
    }

    @Override
    public Events queryEvents(EventQueryCondition condition) throws Exception {
        MeasureQueryResponse resp = query(Event.INDEX_NAME, TAGS,
                Collections.emptySet(), buildQuery(Collections.singletonList(condition)));
        Events events = new Events();
        if (resp.size() == 0) {
            return events;
        }
        for (final DataPoint dataPoint : resp.getDataPoints()) {
            events.getEvents().add(buildEventView(dataPoint));
        }
        return events;
    }

    @Override
    public Events queryEvents(List<EventQueryCondition> conditionList) throws Exception {
        MeasureQueryResponse resp = query(Event.INDEX_NAME, TAGS,
                Collections.emptySet(), buildQuery(conditionList));
        Events events = new Events();
        if (resp.size() == 0) {
            return events;
        }
        for (final DataPoint dataPoint : resp.getDataPoints()) {
            events.getEvents().add(buildEventView(dataPoint));
        }
        return events;
    }

    public QueryBuilder<MeasureQuery> buildQuery(final List<EventQueryCondition> conditionList) {
        EventQueryCondition condition = conditionList.get(0);
        final Order queryOrder = isNull(condition.getOrder()) ? Order.DES : condition.getOrder();
        final PaginationUtils.Page page = PaginationUtils.INSTANCE.exchange(condition.getPaging());

        return new QueryBuilder<MeasureQuery>() {
            @Override
            protected void apply(MeasureQuery query) {
                List<AbstractCriteria> eventsQueryConditions = new ArrayList<>(conditionList.size());
                query.limit(page.getLimit());
                query.offset(page.getFrom());
                setOrderBy(query, queryOrder);

                for (final EventQueryCondition condition : conditionList) {
                    eventsQueryConditions.add(buildEventQueryCriteria(condition));
                }

                applyCriteria(query, eventsQueryConditions);
            }
        };
    }

    private void setOrderBy(MeasureQuery query, Order queryOrder) {
        if (queryOrder == Order.ASC) {
            query.setOrderBy(asc(Event.START_TIME));
        } else {
            query.setOrderBy(desc(Event.START_TIME));
        }
    }

    private AbstractCriteria buildEventQueryCriteria(EventQueryCondition condition) {
        List<PairQueryCondition<?>> queryConditions = new ArrayList<>();
        addUuidCondition(queryConditions, condition);
        addSourceConditions(queryConditions, condition.getSource());
        addNameCondition(queryConditions, condition);
        addTypeCondition(queryConditions, condition);
        addTimeConditions(queryConditions, condition.getTime());
        addLayerCondition(queryConditions, condition);
        return and(queryConditions);
    }

    private void addUuidCondition(List<PairQueryCondition<?>> queryConditions, EventQueryCondition condition) {
        if (!isNullOrEmpty(condition.getUuid())) {
            queryConditions.add(eq(Event.UUID, condition.getUuid()));
        }
    }

    private void addSourceConditions(List<PairQueryCondition<?>> queryConditions, Source source) {
        if (source != null) {
            if (!isNullOrEmpty(source.getService())) {
                queryConditions.add(eq(Event.SERVICE, source.getService()));
            }
            if (!isNullOrEmpty(source.getServiceInstance())) {
                queryConditions.add(eq(Event.SERVICE_INSTANCE, source.getServiceInstance()));
            }
            if (!isNullOrEmpty(source.getEndpoint())) {
                queryConditions.add(eq(Event.ENDPOINT, source.getEndpoint()));
            }
        }
    }

    private void addNameCondition(List<PairQueryCondition<?>> queryConditions, EventQueryCondition condition) {
        if (!isNullOrEmpty(condition.getName())) {
            queryConditions.add(eq(Event.NAME, condition.getName()));
        }
    }

    private void addTypeCondition(List<PairQueryCondition<?>> queryConditions, EventQueryCondition condition) {
        if (condition.getType() != null) {
            queryConditions.add(eq(Event.TYPE, condition.getType().name()));
        }
    }

    private void addTimeConditions(List<PairQueryCondition<?>> queryConditions, Duration startTime) {
        if (startTime != null) {
            if (startTime.getStartTimestamp() > 0) {
                queryConditions.add(gte(Event.START_TIME, startTime.getStartTimestamp()));
            }
            if (startTime.getEndTimestamp() > 0) {
                queryConditions.add(lte(Event.END_TIME, startTime.getEndTimestamp()));
            }
        }
    }

    private void addLayerCondition(List<PairQueryCondition<?>> queryConditions, EventQueryCondition condition) {
        if (!isNullOrEmpty(condition.getLayer())) {
            queryConditions.add(eq(Event.LAYER, Layer.valueOf(condition.getLayer()).value()));
        }
    }

    private void applyCriteria(MeasureQuery query, List<AbstractCriteria> eventsQueryConditions) {
        if (eventsQueryConditions.size() == 1) {
            query.criteria(eventsQueryConditions.get(0));
        } else if (eventsQueryConditions.size() > 1) {
            query.criteria(or(eventsQueryConditions));
        }
    }

//Refactoring end
        };
    }

    protected org.apache.skywalking.oap.server.core.query.type.event.Event buildEventView(
            final DataPoint dataPoint) {
        final org.apache.skywalking.oap.server.core.query.type.event.Event event =
                new org.apache.skywalking.oap.server.core.query.type.event.Event();

        event.setUuid(dataPoint.getTagValue(Event.UUID));

        String service = getValueOrDefault(dataPoint, Event.SERVICE, "");
        String serviceInstance = getValueOrDefault(dataPoint, Event.SERVICE_INSTANCE, "");
        String endpoint = getValueOrDefault(dataPoint, Event.ENDPOINT, "");
        event.setSource(new Source(service, serviceInstance, endpoint));

        event.setName(dataPoint.getTagValue(Event.NAME));
        event.setType(EventType.parse(dataPoint.getTagValue(Event.TYPE)));
        event.setMessage(dataPoint.getTagValue(Event.MESSAGE));
        event.setParameters((String) dataPoint.getTagValue(Event.PARAMETERS));
        event.setStartTime(dataPoint.getTagValue(Event.START_TIME));
        event.setEndTime(dataPoint.getTagValue(Event.END_TIME));

        event.setLayer(Layer.valueOf(((Number) dataPoint.getTagValue(Event.LAYER)).intValue()).name());

        return event;
    }

    private <T> T getValueOrDefault(DataPoint dataPoint, String tagName, T defaultValue) {
        T v = dataPoint.getTagValue(tagName);
        return v == null ? defaultValue : v;
    } 
}
