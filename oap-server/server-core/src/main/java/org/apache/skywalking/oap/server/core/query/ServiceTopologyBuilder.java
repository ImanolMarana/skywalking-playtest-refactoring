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

package org.apache.skywalking.oap.server.core.query;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.oap.server.core.Const;
import org.apache.skywalking.oap.server.core.CoreModule;
import org.apache.skywalking.oap.server.core.analysis.IDManager;
import org.apache.skywalking.oap.server.core.analysis.Layer;
import org.apache.skywalking.oap.server.core.analysis.manual.networkalias.NetworkAddressAlias;
import org.apache.skywalking.oap.server.core.cache.NetworkAddressAliasCache;
import org.apache.skywalking.oap.server.core.config.IComponentLibraryCatalogService;
import org.apache.skywalking.oap.server.core.query.type.Call;
import org.apache.skywalking.oap.server.core.query.type.Node;
import org.apache.skywalking.oap.server.core.query.type.Service;
import org.apache.skywalking.oap.server.core.query.type.Topology;
import org.apache.skywalking.oap.server.core.source.DetectPoint;
import org.apache.skywalking.oap.server.library.module.ModuleManager;
import org.apache.skywalking.oap.server.library.util.StringUtil;

@Slf4j
class ServiceTopologyBuilder {
    private final IComponentLibraryCatalogService componentLibraryCatalogService;
    private final NetworkAddressAliasCache networkAddressAliasCache;
    private final String userID;
    private final ModuleManager moduleManager;
    private MetadataQueryService metadataQueryService;

    ServiceTopologyBuilder(ModuleManager moduleManager) {
        this.componentLibraryCatalogService = moduleManager.find(CoreModule.NAME)
                                                           .provider()
                                                           .getService(IComponentLibraryCatalogService.class);
        this.networkAddressAliasCache = moduleManager.find(CoreModule.NAME)
                                                     .provider()
                                                     .getService(NetworkAddressAliasCache.class);
        this.userID = IDManager.ServiceID.buildId(Const.USER_SERVICE_NAME, false);
        this.moduleManager = moduleManager;
    }

    private MetadataQueryService getMetadataQueryService() {
        if (metadataQueryService == null) {
            this.metadataQueryService = moduleManager.find(CoreModule.NAME)
                                                     .provider()
                                                     .getService(MetadataQueryService.class);
        }
        return metadataQueryService;
    }

    Topology build(List<Call.CallDetail> serviceRelationClientCalls, List<Call.CallDetail> serviceRelationServerCalls) {
    Map<String, Node> nodes = new HashMap<>();
    List<Call> calls = new LinkedList<>();
    HashMap<String, Call> callMap = new HashMap<>();

    processClientCalls(serviceRelationClientCalls, nodes, callMap, calls);
    processServerCalls(serviceRelationServerCalls, nodes, callMap, calls);

    Topology topology = new Topology();
    topology.getCalls().addAll(calls);
    topology.getNodes().addAll(nodes.values());
    return topology;
  }

  private void processClientCalls(List<Call.CallDetail> serviceRelationClientCalls, Map<String, Node> nodes,
      HashMap<String, Call> callMap, List<Call> calls) {
    for (Call.CallDetail clientCall : serviceRelationClientCalls) {
      String sourceServiceId = clientCall.getSource();
      IDManager.ServiceID.ServiceIDDefinition sourceService = IDManager.ServiceID.analysisId(sourceServiceId);
      String targetServiceId = processTargetService(clientCall, nodes);

      if (!nodes.containsKey(sourceServiceId)) {
        nodes.put(sourceServiceId, buildNode(sourceServiceId, sourceService));
      }

      processCall(sourceServiceId, targetServiceId, callMap, calls, clientCall, DetectPoint.CLIENT);
    }
  }

  private void processServerCalls(List<Call.CallDetail> serviceRelationServerCalls, Map<String, Node> nodes,
      HashMap<String, Call> callMap, List<Call> calls) {
    for (Call.CallDetail serverCall : serviceRelationServerCalls) {
      IDManager.ServiceID.ServiceIDDefinition sourceService = IDManager.ServiceID.analysisId(
          serverCall.getSource());
      IDManager.ServiceID.ServiceIDDefinition destService = IDManager.ServiceID.analysisId(
          serverCall.getTarget());

      Node clientSideNode = nodes.getOrDefault(serverCall.getSource(), buildNode(serverCall.getSource(), sourceService));
      nodes.put(serverCall.getSource(), clientSideNode);

      if (!clientSideNode.isReal()) {
        clientSideNode.setType(
            componentLibraryCatalogService.getServerNameBasedOnComponent(serverCall.getComponentId()));
      }

      if (userID.equals(serverCall.getSource())) {
        nodes.get(userID).setType(Const.USER_SERVICE_NAME.toUpperCase());
      }

      if (!nodes.containsKey(serverCall.getTarget())) {
        nodes.put(serverCall.getTarget(), buildNode(serverCall.getTarget(), destService));
      }

      processServerSideNode(serverCall, nodes);

      processCall(serverCall.getSource(), serverCall.getTarget(), callMap, calls, serverCall, DetectPoint.SERVER);
    }
  }

  private String processTargetService(Call.CallDetail clientCall, Map<String, Node> nodes) {
    String targetServiceId = clientCall.getTarget();
    IDManager.ServiceID.ServiceIDDefinition destService = IDManager.ServiceID.analysisId(targetServiceId);

    if (networkAddressAliasCache.get(destService.getName()) != null) {
      NetworkAddressAlias networkAddressAlias = networkAddressAliasCache.get(destService.getName());
      destService = IDManager.ServiceID.analysisId(networkAddressAlias.getRepresentServiceId());
      targetServiceId = IDManager.ServiceID.buildId(destService.getName(), true);
    }

    if (!nodes.containsKey(targetServiceId)) {
      Node conjecturalNode = buildNode(targetServiceId, destService);
      nodes.put(targetServiceId, conjecturalNode);
      if (!conjecturalNode.isReal() && StringUtil.isEmpty(conjecturalNode.getType())) {
        conjecturalNode.setType(
            componentLibraryCatalogService.getServerNameBasedOnComponent(clientCall.getComponentId()));
      }
    }
    return targetServiceId;
  }

  private void processCall(String sourceServiceId, String targetServiceId, HashMap<String, Call> callMap,
      List<Call> calls, Call.CallDetail callDetail, DetectPoint detectPoint) {
    String relationId = IDManager.ServiceID.buildRelationId(
        new IDManager.ServiceID.ServiceRelationDefine(sourceServiceId, targetServiceId));

    if (!callMap.containsKey(relationId)) {
      Call call = new Call();
      callMap.put(relationId, call);
      call.setSource(sourceServiceId);
      call.setTarget(targetServiceId);
      call.setId(relationId);
      call.addDetectPoint(detectPoint);
      addComponent(call, callDetail, detectPoint);
      calls.add(call);
    } else {
      Call call = callMap.get(relationId);
      call.addDetectPoint(detectPoint);
      addComponent(call, callDetail, detectPoint);
    }
  }

  private void processServerSideNode(Call.CallDetail serverCall, Map<String, Node> nodes) {
    final Node serverSideNode = nodes.get(serverCall.getTarget());
    final String nodeType = serverSideNode.getType();
    if (nodeType == null || !serverSideNode.hasSetOnceAtServerSide()) {
      serverSideNode.setTypeFromServerSide(
          componentLibraryCatalogService.getComponentName(serverCall.getComponentId()));
    } else {
      final Integer componentId = componentLibraryCatalogService.getComponentId(nodeType);
      if (componentId != null) {
        if (componentLibraryCatalogService.compare(componentId, serverCall.getComponentId())) {
          serverSideNode.setTypeFromServerSide(
              componentLibraryCatalogService.getComponentName(serverCall.getComponentId()));
        }
      } else {
        serverSideNode.setTypeFromServerSide(
            componentLibraryCatalogService.getComponentName(serverCall.getComponentId()));
      }
    }
  }

  private void addComponent(Call call, Call.CallDetail callDetail, DetectPoint detectPoint) {
    if (detectPoint == DetectPoint.CLIENT) {
      call.addSourceComponent(componentLibraryCatalogService.getComponentName(callDetail.getComponentId()));
    } else {
      call.addTargetComponent(componentLibraryCatalogService.getComponentName(callDetail.getComponentId()));
    }
  }

  @SneakyThrows
  private Node buildNode(String sourceId, IDManager.ServiceID.ServiceIDDefinition sourceService) {
    Node serviceNode = new Node();
    serviceNode.setId(sourceId);
    serviceNode.setName(sourceService.getName());
    serviceNode.setReal(sourceService.isReal());
    Service service = getMetadataQueryService().getService(sourceId);
    if (service != null) {
      serviceNode.getLayers().addAll(service.getLayers());
    } else {
      serviceNode.getLayers().add(Layer.UNDEFINED.name());
    }
    return serviceNode;
  }
//Refactoring end
}
