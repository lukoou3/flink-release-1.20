/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rpc.pekko;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.exceptions.RecipientUnreachableException;
import org.apache.flink.util.SerializedValue;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for remote {@link PekkoRpcActor}s. */
class RemotePekkoRpcActorTest {

    // RpcService类似于spark的RpcEnv
    private static PekkoRpcService rpcService;
    private static PekkoRpcService otherRpcService;

    private static final Configuration configuration = new Configuration();

    @BeforeAll
    static void setupClass() throws Exception {
        rpcService =
                PekkoRpcServiceUtils.createRemoteRpcService(
                        configuration, "localhost", "0", null, Optional.empty());

        otherRpcService =
                PekkoRpcServiceUtils.createRemoteRpcService(
                        configuration, "localhost", "0", null, Optional.empty());
    }

    @AfterAll
    static void teardownClass() throws InterruptedException, ExecutionException, TimeoutException {
        RpcUtils.terminateRpcService(rpcService, otherRpcService);
    }

    @Test
    void canRespondWithNullValueRemotely() throws Exception {
        try (final PekkoRpcActorTest.NullRespondingEndpoint nullRespondingEndpoint =
                new PekkoRpcActorTest.NullRespondingEndpoint(rpcService)) {
            nullRespondingEndpoint.start();

            final PekkoRpcActorTest.NullRespondingGateway rpcGateway =
                    otherRpcService
                            .connect(
                                    nullRespondingEndpoint.getAddress(),
                                    PekkoRpcActorTest.NullRespondingGateway.class)
                            .join();

            final CompletableFuture<Integer> nullValuedResponseFuture = rpcGateway.foobar();

            assertThat(nullValuedResponseFuture.join()).isNull();
        }
    }

    @Test
    void canRespondWithSynchronousNullValueRemotely() throws Exception {
        try (final PekkoRpcActorTest.NullRespondingEndpoint nullRespondingEndpoint =
                new PekkoRpcActorTest.NullRespondingEndpoint(rpcService)) {
            nullRespondingEndpoint.start();

            final PekkoRpcActorTest.NullRespondingGateway rpcGateway =
                    otherRpcService
                            .connect(
                                    nullRespondingEndpoint.getAddress(),
                                    PekkoRpcActorTest.NullRespondingGateway.class)
                            .join();

            final Integer value = rpcGateway.synchronousFoobar();

            assertThat(value).isNull();
        }
    }

    /**
     * RpcService类似于spark的RpcEnv
     * RpcEndpoint类似于spark的RpcEndpoint，创建spark的RpcEndpoint需要传入RpcEnv，同样创建flink的RpcEndpoint需要传入RpcService
     * RpcGateway类似于spark的RpcEndpointRef
     *      不同的是saprk的ref不需要为每个RpcEndpointRef新建不同的类，两边通过约定发送send/ask请求，服务端通过模式匹配处理请求
     *      flink的gateway需要为每个RpcGateway新建不同的接口，服务端实现这个接口，客户端获取这个RpcGateway的代理对象，直接调用特定的方法
     *      应该是由scala和java语言的特性决定的吧，scala使用模式匹配偏函数易用开发效率快，java不太擅长这种处理
     *      Endpoint和gateway都会实现gateway的接口，每种通信需要实现不同的RpcGateway。客户端获取的就是RpcGateway接口的代理对象
     *
     */
    @Test
    void canRespondWithSerializedValueRemotely() throws Exception {
        // RpcEndpoint
        try (final PekkoRpcActorTest.SerializedValueRespondingEndpoint endpoint =
                new PekkoRpcActorTest.SerializedValueRespondingEndpoint(rpcService)) {
            // 调用start, 会调用onStart()方法
            endpoint.start();

            // RpcGateway(RpcEndpointRef)
            final PekkoRpcActorTest.SerializedValueRespondingGateway remoteGateway =
                    otherRpcService
                            .connect(
                                    endpoint.getAddress(),
                                    PekkoRpcActorTest.SerializedValueRespondingGateway.class)
                            .join();

            SerializedValue<String> response = remoteGateway.getSerializedValueSynchronously();
            System.out.println(response);

            assertThat(remoteGateway.getSerializedValueSynchronously())
                    .isEqualTo(
                            PekkoRpcActorTest.SerializedValueRespondingEndpoint.SERIALIZED_VALUE);

            final CompletableFuture<SerializedValue<String>> responseFuture =
                    remoteGateway.getSerializedValue();

            assertThat(responseFuture.get())
                    .isEqualTo(
                            PekkoRpcActorTest.SerializedValueRespondingEndpoint.SERIALIZED_VALUE);
        }
    }

    @Test
    void failsRpcResultImmediatelyIfEndpointIsStopped() throws Exception {
        try (final PekkoRpcActorTest.SerializedValueRespondingEndpoint endpoint =
                new PekkoRpcActorTest.SerializedValueRespondingEndpoint(rpcService)) {
            endpoint.start();

            final PekkoRpcActorTest.SerializedValueRespondingGateway gateway =
                    otherRpcService
                            .connect(
                                    endpoint.getAddress(),
                                    PekkoRpcActorTest.SerializedValueRespondingGateway.class)
                            .join();

            endpoint.close();

            // the rpc result should not fail because of a TimeoutException
            assertThatThrownBy(() -> gateway.getSerializedValue().join())
                    .satisfies(
                            FlinkAssertions.anyCauseMatches(RecipientUnreachableException.class));
        }
    }

    @Test
    void failsRpcResultImmediatelyIfRemoteRpcServiceIsNotAvailable() throws Exception {
        final PekkoRpcService toBeClosedRpcService =
                PekkoRpcServiceUtils.createRemoteRpcService(
                        configuration, "localhost", "0", null, Optional.empty());
        try (final PekkoRpcActorTest.SerializedValueRespondingEndpoint endpoint =
                new PekkoRpcActorTest.SerializedValueRespondingEndpoint(toBeClosedRpcService)) {
            endpoint.start();

            final PekkoRpcActorTest.SerializedValueRespondingGateway gateway =
                    otherRpcService
                            .connect(
                                    endpoint.getAddress(),
                                    PekkoRpcActorTest.SerializedValueRespondingGateway.class)
                            .join();

            toBeClosedRpcService.closeAsync().join();

            // the rpc result should not fail because of a TimeoutException
            assertThatThrownBy(() -> gateway.getSerializedValue().join())
                    .satisfies(
                            FlinkAssertions.anyCauseMatches(RecipientUnreachableException.class));
        } finally {
            RpcUtils.terminateRpcService(toBeClosedRpcService);
        }
    }
}
