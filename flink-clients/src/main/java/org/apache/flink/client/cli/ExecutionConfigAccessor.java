/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.client.cli;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Accessor that exposes config settings that are relevant for execution from an underlying {@link
 * Configuration}.
 */
@Internal
public class ExecutionConfigAccessor {

    private final Configuration configuration;

    private ExecutionConfigAccessor(final Configuration configuration) {
        this.configuration = checkNotNull(configuration);
    }

    /** Creates an {@link ExecutionConfigAccessor} based on the provided {@link Configuration}. */
    public static ExecutionConfigAccessor fromConfiguration(final Configuration configuration) {
        return new ExecutionConfigAccessor(checkNotNull(configuration));
    }

    /**
     * Creates an {@link ExecutionConfigAccessor} based on the provided {@link ProgramOptions} as
     * provided by the user through the CLI.
     */
    public static <T> ExecutionConfigAccessor fromProgramOptions(
            final ProgramOptions options, final List<T> jobJars) {
        checkNotNull(options);
        checkNotNull(jobJars);

        final Configuration configuration = new Configuration();

        options.applyToConfiguration(configuration);
        // 这里会把用户提交的jar(包括lib目录下的jar)列表设置到pipeline.jars
        ConfigUtils.encodeCollectionToConfig(
                configuration, PipelineOptions.JARS, jobJars, Object::toString);

        return new ExecutionConfigAccessor(configuration);
    }

    public Configuration applyToConfiguration(final Configuration baseConfiguration) {
        baseConfiguration.addAll(configuration);
        return baseConfiguration;
    }

    public List<URL> getJars() throws MalformedURLException {
        return ConfigUtils.decodeListFromConfig(configuration, PipelineOptions.JARS, URL::new);
    }

    public List<URL> getClasspaths() throws MalformedURLException {
        return ConfigUtils.decodeListFromConfig(
                configuration, PipelineOptions.CLASSPATHS, URL::new);
    }

    public int getParallelism() {
        return configuration.get(CoreOptions.DEFAULT_PARALLELISM);
    }

    public boolean getDetachedMode() {
        return !configuration.get(DeploymentOptions.ATTACHED);
    }

    public SavepointRestoreSettings getSavepointRestoreSettings() {
        return SavepointRestoreSettings.fromConfiguration(configuration);
    }

    public boolean isShutdownOnAttachedExit() {
        return configuration.get(DeploymentOptions.SHUTDOWN_IF_ATTACHED);
    }
}
