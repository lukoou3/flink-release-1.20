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

package org.apache.flink.client.deployment.executors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.program.PerJobMiniClusterFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.executiongraph.DefaultExecutionGraphBuilder;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmaster.ExecutionDeploymentTracker;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.jobmaster.JobMasterServiceLeadershipRunner;
import org.apache.flink.runtime.jobmaster.SlotPoolServiceSchedulerFactory;
import org.apache.flink.runtime.jobmaster.factories.DefaultJobMasterServiceFactory;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.scheduler.DefaultExecutionDeployer;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.VertexParallelismStore;

import java.net.MalformedURLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** An {@link PipelineExecutor} for executing a {@link Pipeline} locally. */
@Internal
public class LocalExecutor implements PipelineExecutor {

    public static final String NAME = "local";

    private final Configuration configuration;
    private final Function<MiniClusterConfiguration, MiniCluster> miniClusterFactory;

    public static LocalExecutor create(Configuration configuration) {
        return new LocalExecutor(configuration, MiniCluster::new);
    }

    public static LocalExecutor createWithFactory(
            Configuration configuration,
            Function<MiniClusterConfiguration, MiniCluster> miniClusterFactory) {
        return new LocalExecutor(configuration, miniClusterFactory);
    }

    private LocalExecutor(
            Configuration configuration,
            Function<MiniClusterConfiguration, MiniCluster> miniClusterFactory) {
        this.configuration = configuration;
        this.miniClusterFactory = miniClusterFactory;
    }

    @Override
    public CompletableFuture<JobClient> execute(
            Pipeline pipeline, Configuration configuration, ClassLoader userCodeClassloader)
            throws Exception {
        checkNotNull(pipeline);
        checkNotNull(configuration);

        Configuration effectiveConfig = new Configuration();
        effectiveConfig.addAll(this.configuration);
        effectiveConfig.addAll(configuration);

        // we only support attached execution with the local executor.
        checkState(configuration.get(DeploymentOptions.ATTACHED));

        final JobGraph jobGraph = getJobGraph(pipeline, effectiveConfig, userCodeClassloader);

        /**
         * local模式提交jobGraph:
         *  @see PerJobMiniClusterFactory#submitJob(JobGraph, ClassLoader)
         *  @see MiniCluster#submitJob(JobGraph)
         *  @see DispatcherGateway#submitJob(JobGraph, Time): 提交rpc请求到Dispatcher服务端
         *
         * 服务端处理:
         *  @see Dispatcher#submitJob(JobGraph, Time)
         *  @see Dispatcher#internalSubmitJob(JobGraph)
         *  @see Dispatcher#persistAndRunJob(JobGraph)
         *  @see Dispatcher#runJob(JobManagerRunner, Dispatcher.ExecutionType)
         *  @see JobMasterServiceLeadershipRunner#start(): JobManagerRunner会竞争leader,一旦被选举为 leader,就会启动一个 JobMaster
         *  @see JobMasterServiceLeadershipRunner#grantLeadership(UUID): 被选举为leader, 启动JobMaster
         *  @see DefaultJobMasterServiceFactory#internalCreateJobMasterService(UUID, OnCompletionActions)
         *    jobMaster = new JobMaster: JobMaster内会初始化schedulerNG(生成ExecutionGraph), schedulerNG负责调度部署Task
         *    jobMaster.start(): 启动JobMaster, 转到JobMaster的onStart()方法, 因为JobMaster是一个RpcEndpoint
         *
         * JobMaster的初始化：
         * @see JobMaster#createScheduler(SlotPoolServiceSchedulerFactory, ExecutionDeploymentTracker, JobManagerJobMetricGroup, JobStatusListener):
         *    SchedulerBase(DefaultScheduler)持有executionGraph的引用：ExecutionGraph executionGraph
         * jobGraph => executionGraph：
         * @see SchedulerBase#createAndRestoreExecutionGraph
         * @see DefaultExecutionGraphBuilder#buildGraph: TODO 构建ExecutionGraph的核心方法
         *
         * JobMaster的start：
         * @see JobMaster#onStart(): startJobExecution() -> startScheduling() -> schedulerNG.startScheduling()
         * @see JobMaster#startJobExecution()
         * @see SchedulerBase#startScheduling()
         * @see DefaultExecutionDeployer#allocateSlotsAndDeploy(List, Map)
         * @see DefaultExecutionDeployer#deployAll(List)
         * @see Execution#deploy(): 部署一个subTask
         *
         */
        return PerJobMiniClusterFactory.createWithFactory(effectiveConfig, miniClusterFactory)
                .submitJob(jobGraph, userCodeClassloader);
    }

    private JobGraph getJobGraph(
            Pipeline pipeline, Configuration configuration, ClassLoader userCodeClassloader)
            throws MalformedURLException {
        // This is a quirk in how LocalEnvironment used to work. It sets the default parallelism
        // to <num taskmanagers> * <num task slots>. Might be questionable but we keep the behaviour
        // for now.
        if (pipeline instanceof Plan) {
            Plan plan = (Plan) pipeline;
            final int slotsPerTaskManager =
                    configuration.get(
                            TaskManagerOptions.NUM_TASK_SLOTS, plan.getMaximumParallelism());
            final int numTaskManagers =
                    configuration.get(TaskManagerOptions.MINI_CLUSTER_NUM_TASK_MANAGERS);

            plan.setDefaultParallelism(slotsPerTaskManager * numTaskManagers);
        }

        return PipelineExecutorUtils.getJobGraph(pipeline, configuration, userCodeClassloader);
    }
}
