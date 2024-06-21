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

package org.apache.skywalking.oap.server.library.module;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.oap.server.library.util.CollectionUtils;

@Slf4j
class BootstrapFlow {
    private Map<String, ModuleDefine> loadedModules;
    private List<ModuleProvider> startupSequence;

    BootstrapFlow(Map<String, ModuleDefine> loadedModules) throws CycleDependencyException, ModuleNotFoundException {
        this.loadedModules = loadedModules;
        startupSequence = new ArrayList<>();

        makeSequence();
    }

    @SuppressWarnings("unchecked")
    void start(
        ModuleManager moduleManager) throws ModuleNotFoundException, ServiceNotProvidedException, ModuleStartException {
        for (ModuleProvider provider : startupSequence) {
            log.info("start the provider {} in {} module.", provider.name(), provider.getModuleName());
            provider.requiredCheck(provider.getModule().services());

            provider.start();
        }
    }

    void notifyAfterCompleted() throws ServiceNotProvidedException, ModuleStartException {
        for (ModuleProvider provider : startupSequence) {
            provider.notifyAfterCompleted();
        }
    }

    private void makeSequence() throws CycleDependencyException, ModuleNotFoundException {
        List<ModuleProvider> allProviders = new ArrayList<>();
        for (final ModuleDefine module : loadedModules.values()) {
          validateRequiredModules(module);
          allProviders.add(module.provider());
        }
    
        sequencingModules(allProviders);
      }
    
      private void validateRequiredModules(ModuleDefine module) throws ModuleNotFoundException {
        String[] requiredModules = module.provider().requiredModules();
        if (requiredModules != null) {
          for (String requiredModule : requiredModules) {
            if (!loadedModules.containsKey(requiredModule)) {
              throw new ModuleNotFoundException(
                  requiredModule + " module is required by "
                      + module.provider().getModuleName() + "."
                      + module.provider().name() + ", but not found.");
            }
          }
        }
      }
    
      private void sequencingModules(List<ModuleProvider> allProviders) throws CycleDependencyException {
        do {
          int numOfToBeSequenced = allProviders.size();
          for (int i = 0; i < allProviders.size(); i++) {
            ModuleProvider provider = allProviders.get(i);
            if (canBeStarted(provider)) {
              startupSequence.add(provider);
              allProviders.remove(i);
              i--;
            }
          }
    
          if (numOfToBeSequenced == allProviders.size()) {
            throw new CycleDependencyException("Exist cycle module dependencies in \n" + getUnsequencedProviders(allProviders));
          }
        }
        while (allProviders.size() != 0);
      }
    
      private boolean canBeStarted(ModuleProvider provider) {
        String[] requiredModules = provider.requiredModules();
        if (CollectionUtils.isNotEmpty(requiredModules)) {
          for (String module : requiredModules) {
            if (!isModuleStarted(module)) {
              return false;
            }
          }
        }
        return true;
      }
    
      private boolean isModuleStarted(String module) {
        for (ModuleProvider moduleProvider : startupSequence) {
          if (moduleProvider.getModuleName().equals(module)) {
            return true;
          }
        }
        return false;
      }
    
      private String getUnsequencedProviders(List<ModuleProvider> allProviders) {
        StringBuilder unSequencedProviders = new StringBuilder();
        allProviders.forEach(provider -> unSequencedProviders.append(provider.getModuleName())
            .append("[provider=")
            .append(provider.getClass().getName())
            .append("]\n"));
        return unSequencedProviders.substring(0, unSequencedProviders.length() - 1);
      }
//Refactoring end