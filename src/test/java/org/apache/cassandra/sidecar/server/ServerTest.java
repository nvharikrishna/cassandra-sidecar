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

package org.apache.cassandra.sidecar.server;

import java.nio.file.Path;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import static org.apache.cassandra.sidecar.common.ResourceUtils.writeResourceToPath;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;

/**
 * Unit tests for {@link Server} lifecycle
 */
@ExtendWith(VertxExtension.class)
class ServerTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerTest.class);
    @TempDir
    private Path confPath;

    private Server server;
    private Vertx vertx;
    private WebClient client;

    @BeforeEach
    void setup()
    {
        ClassLoader classLoader = ServerTest.class.getClassLoader();
        Path yamlPath = writeResourceToPath(classLoader, confPath, "config/sidecar_single_instance.yaml");
        Injector injector = Guice.createInjector(new MainModule(yamlPath));
        server = injector.getInstance(Server.class);
        vertx = injector.getInstance(Vertx.class);
        client = WebClient.create(vertx);
    }

    @Test
    @DisplayName("Server should start and stop Sidecar successfully")
    void startStopServer(VertxTestContext context)
    {
        Checkpoint serverStarted = context.checkpoint();
        Checkpoint serverStopped = context.checkpoint();

        vertx.eventBus().localConsumer(SidecarServerEvents.ON_SERVER_START.address(), message -> serverStarted.flag());
        vertx.eventBus().localConsumer(SidecarServerEvents.ON_SERVER_STOP.address(), message -> serverStopped.flag());

        server.start()
              .compose(this::validateHealthEndpoint)
              .compose(deploymentId -> server.stop(deploymentId))
              .onFailure(context::failNow);
    }

    @Test
    @DisplayName("Server should restart successfully")
    void testServerRestarts(VertxTestContext context)
    {
        Checkpoint serverStarted = context.checkpoint(2);
        Checkpoint serverStopped = context.checkpoint(2);

        vertx.eventBus().localConsumer(SidecarServerEvents.ON_SERVER_START.address(), message -> serverStarted.flag());
        vertx.eventBus().localConsumer(SidecarServerEvents.ON_SERVER_STOP.address(), message -> serverStopped.flag());

        server.start()
              .compose(this::validateHealthEndpoint)
              .compose(deploymentId -> server.stop(deploymentId))
              .compose(v -> server.start())
              .compose(this::validateHealthEndpoint)
              .compose(restartDeploymentId -> server.stop(restartDeploymentId))
              .onFailure(context::failNow);
    }

    @Test
    @DisplayName("Server should start and close Sidecar successfully")
    void startCloseServer(VertxTestContext context)
    {
        Checkpoint serverStarted = context.checkpoint();
        Checkpoint serverStopped = context.checkpoint();

        vertx.eventBus().localConsumer(SidecarServerEvents.ON_SERVER_START.address(), message -> serverStarted.flag());
        vertx.eventBus().localConsumer(SidecarServerEvents.ON_SERVER_STOP.address(), message -> serverStopped.flag());

        server.start()
              .compose(this::validateHealthEndpoint)
              .compose(deploymentId -> server.close())
              .onFailure(context::failNow);
    }

    @Test
    @DisplayName("Server should start and close Sidecar successfully and start is no longer allowed")
    void startCloseServerShouldNotStartAgain(VertxTestContext context)
    {
        Checkpoint serverStarted = context.checkpoint();
        Checkpoint serverStopped = context.checkpoint();

        vertx.eventBus().localConsumer(SidecarServerEvents.ON_SERVER_START.address(), message -> serverStarted.flag());
        vertx.eventBus().localConsumer(SidecarServerEvents.ON_SERVER_STOP.address(), message -> serverStopped.flag());

        server.start()
              .compose(this::validateHealthEndpoint)
              .compose(deploymentId -> server.close())
              .onSuccess(v -> assertThatException().isThrownBy(() -> server.start())
                                                   .withMessageContaining("Vert.x closed"))
              .onFailure(context::failNow);
    }

    Future<String> validateHealthEndpoint(String deploymentId)
    {
        LOGGER.info("Checking server health 127.0.0.1:{}/api/v1/__health", server.actualPort());
        return client.get(server.actualPort(), "127.0.0.1", "/api/v1/__health")
                     .send()
                     .compose(response -> {
                         assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                         assertThat(response.bodyAsJsonObject().getString("status")).isEqualTo("OK");
                         return Future.succeededFuture(deploymentId);
                     });
    }
}
