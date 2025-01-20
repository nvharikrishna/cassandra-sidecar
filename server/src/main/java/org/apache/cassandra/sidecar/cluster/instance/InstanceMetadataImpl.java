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

package org.apache.cassandra.sidecar.cluster.instance;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.codahale.metrics.MetricRegistry;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException;
import org.apache.cassandra.sidecar.metrics.instance.InstanceMetrics;
import org.apache.cassandra.sidecar.metrics.instance.InstanceMetricsImpl;
import org.apache.cassandra.sidecar.utils.FileUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException.Service.CQL_AND_JMX;

/**
 * Local implementation of InstanceMetadata.
 */
public class InstanceMetadataImpl implements InstanceMetadata
{
    private static final String DEFAULT_CDC_RAW_DIR = "cdc_raw";
    private static final String DEFAULT_COMMITLOG_DIR = "commitlog";
    private static final String DEFAULT_HINTS_DIR = "hints";
    private static final String DEFAULT_SAVED_CACHES_DIR = "saved_caches";

    private final int id;
    private final String host;
    private final int port;
    private final String cassandraHomeDir;
    private final List<String> dataDirs;
    private final String stagingDir;
    private final String cdcDir;
    private final String commitlogDir;
    private final String hintsDir;
    private final String savedCachesDir;
    private final String localSystemDataFileDir;
    @Nullable
    private final CassandraAdapterDelegate delegate;
    private final InstanceMetrics metrics;

    protected InstanceMetadataImpl(Builder builder)
    {
        id = builder.id;
        host = builder.host;
        port = builder.port;
        cassandraHomeDir = FileUtils.maybeResolveHomeDirectory(builder.cassandraHomeDir);
        Path cassandraHomeDirPath = cassandraHomeDir == null ?  null : Paths.get(cassandraHomeDir);
        dataDirs = builder.dataDirs.stream()
                                   .map(FileUtils::maybeResolveHomeDirectory)
                                   .collect(Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList));
        stagingDir = FileUtils.maybeResolveHomeDirectory(builder.stagingDir);
        cdcDir = builder.cdcDir != null
                 ? FileUtils.maybeResolveHomeDirectory(builder.cdcDir)
                 : cassandraHomeDirPath == null
                   ? null
                   : cassandraHomeDirPath.resolve(DEFAULT_CDC_RAW_DIR).toAbsolutePath().toString();
        commitlogDir = builder.commitlogDir != null
                       ? FileUtils.maybeResolveHomeDirectory(builder.commitlogDir)
                       : cassandraHomeDirPath == null
                         ? null
                         : cassandraHomeDirPath.resolve(DEFAULT_COMMITLOG_DIR).toAbsolutePath().toString();
        hintsDir = builder.hintsDir != null
                   ? FileUtils.maybeResolveHomeDirectory(builder.hintsDir)
                   : cassandraHomeDirPath == null
                     ? null
                     : cassandraHomeDirPath.resolve(DEFAULT_HINTS_DIR).toAbsolutePath().toString();
        savedCachesDir = builder.savedCachesDir != null
                         ? FileUtils.maybeResolveHomeDirectory(builder.savedCachesDir)
                         : cassandraHomeDirPath == null
                           ? null
                           : cassandraHomeDirPath.resolve(DEFAULT_SAVED_CACHES_DIR).toAbsolutePath().toString();
        localSystemDataFileDir = FileUtils.maybeResolveHomeDirectory(builder.localSystemDataFileDir);
        delegate = builder.delegate;
        metrics = builder.metrics;
    }

    @Override
    public int id()
    {
        return id;
    }

    @Override
    public String host()
    {
        return host;
    }

    @Override
    public int port()
    {
        return port;
    }

    @Override
    public String cassandraHomeDir()
    {
        return cassandraHomeDir;
    }
    @Override
    public List<String> dataDirs()
    {
        return dataDirs;
    }

    @Override
    public String stagingDir()
    {
        return stagingDir;
    }

    @Override
    public String cdcDir()
    {
        return cdcDir;
    }

    @Override
    @NotNull
    public CassandraAdapterDelegate delegate() throws CassandraUnavailableException
    {
        if (delegate == null)
        {
            throw new CassandraUnavailableException(CQL_AND_JMX, "CassandraAdapterDelegate is null");
        }
        return delegate;
    }

    public @Nullable String commitlogDir()
    {
        return commitlogDir;
    }

    @Override
    public @Nullable String hintsDir()
    {
        return hintsDir;
    }

    @Override
    public @Nullable String savedCachesDir()
    {
        return savedCachesDir;
    }

    @Override
    public @Nullable String localSystemDataFileDir()
    {
        return localSystemDataFileDir;
    }

    @Override
    @NotNull
    public InstanceMetrics metrics()
    {
        return metrics;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    @Override
    public String toString()
    {
        return "InstanceMetadataImpl{" +
               "id=" + id +
               ", host='" + host + '\'' +
               ", port=" + port +
               '}';
    }

    /**
     * {@code InstanceMetadataImpl} builder static inner class.
     */
    public static class Builder implements DataObjectBuilder<Builder, InstanceMetadataImpl>
    {
        protected Integer id;
        protected String host;
        protected int port;
        protected String cassandraHomeDir;
        protected List<String> dataDirs;
        protected String stagingDir;
        protected String cdcDir;
        protected String commitlogDir;
        protected String hintsDir;
        protected String savedCachesDir;
        protected String localSystemDataFileDir;
        protected CassandraAdapterDelegate delegate;
        protected MetricRegistry metricRegistry;
        protected InstanceMetrics metrics;

        protected Builder()
        {
        }

        protected Builder(InstanceMetadataImpl instanceMetadata)
        {
            id = instanceMetadata.id;
            host = instanceMetadata.host;
            port = instanceMetadata.port;
            cassandraHomeDir = instanceMetadata.cassandraHomeDir;
            dataDirs = new ArrayList<>(instanceMetadata.dataDirs);
            stagingDir = instanceMetadata.stagingDir;
            cdcDir = instanceMetadata.cdcDir;
            commitlogDir = instanceMetadata.commitlogDir;
            hintsDir = instanceMetadata.hintsDir;
            savedCachesDir = instanceMetadata.savedCachesDir;
            localSystemDataFileDir = instanceMetadata.localSystemDataFileDir;
            delegate = instanceMetadata.delegate;
            metrics = instanceMetadata.metrics;
        }

        @Override
        public Builder self()
        {
            return this;
        }

        /**
         * Sets the {@code id} and returns a reference to this Builder enabling method chaining.
         *
         * @param id the {@code id} to set
         * @return a reference to this Builder
         */
        public Builder id(int id)
        {
            return update(b -> b.id = id);
        }

        /**
         * Sets the {@code host} and returns a reference to this Builder enabling method chaining.
         *
         * @param host the {@code host} to set
         * @return a reference to this Builder
         */
        public Builder host(String host)
        {
            return update(b -> b.host = host);
        }

        /**
         * Sets the {@code port} and returns a reference to this Builder enabling method chaining.
         *
         * @param port the {@code port} to set
         * @return a reference to this Builder
         */
        public Builder port(int port)
        {
            return update(b -> b.port = port);
        }

        /**
         * Sets the {@code cassandraHomeDir} and returns a reference to this Builder enabling method chaining.
         *
         * @param cassandraHomeDir that {@code cassandraHomeDir} to set
         * @return a reference to this Builder
         */
        public Builder cassandraHomeDir(String cassandraHomeDir)
        {
            return update(b -> b.cassandraHomeDir = cassandraHomeDir);
        }

        /**
         * Sets the {@code dataDirs} and returns a reference to this Builder enabling method chaining.
         *
         * @param dataDirs the {@code dataDirs} to set
         * @return a reference to this Builder
         */
        public Builder dataDirs(List<String> dataDirs)
        {
            return update(b -> b.dataDirs = dataDirs);
        }

        /**
         * Sets the {@code stagingDir} and returns a reference to this Builder enabling method chaining.
         *
         * @param stagingDir the {@code stagingDir} to set
         * @return a reference to this Builder
         */
        public Builder stagingDir(String stagingDir)
        {
            return update(b -> b.stagingDir = stagingDir);
        }

        /**
         * Sets the {@code cdcDir} and returns a reference to this Builder enabling method chaining.
         *
         * @param cdcDir the {@code cdcDir} to set
         * @return a reference to this Builder
         */
        public Builder cdcDir(String cdcDir)
        {
            return update(b -> b.cdcDir = cdcDir);
        }

        /**
         * Sets the {@code commitlogDir} and returns a reference to this Builder enabling method chaining.
         *
         * @param commitlogDir the {@code commitlogDir} to set
         * @return a reference to this Builder
         */
        public Builder commitlogDir(String commitlogDir)
        {
            return update(b -> b.commitlogDir = commitlogDir);
        }

        /**
         * Sets the {@code hintsDir} and returns a reference to this Builder enabling method chaining.
         *
         * @param hintsDir the {@code hintsDir} to set
         * @return a reference to this Builder
         */
        public Builder hintsDir(String hintsDir)
        {
            return update(b -> b.hintsDir = hintsDir);
        }

        /**
         * Sets the {@code savedCachesDir} and returns a reference to this Builder enabling method chaining.
         *
         * @param savedCachesDir the {@code savedCachesDir} to set
         * @return a reference to this Builder
         */
        public Builder savedCachesDir(String savedCachesDir)
        {
            return update(b -> b.savedCachesDir = savedCachesDir);
        }

        /**
         * Sets the {@code localSystemDataFileDir} and return a reference to this Builder enabling method chaining.
         *
         * @param localSystemDataFileDir the {@code localSystemDataFileDir} to set
         * @return a reference to this Builder
         */
        public Builder localSystemDataFileDir(String localSystemDataFileDir)
        {
            return update(b -> b.localSystemDataFileDir = localSystemDataFileDir);
        }

        /**
         * Sets the {@code delegate} and returns a reference to this Builder enabling method chaining.
         *
         * @param delegate the {@code delegate} to set
         * @return a reference to this Builder
         */
        public Builder delegate(CassandraAdapterDelegate delegate)
        {
            return update(b -> b.delegate = delegate);
        }

        /**
         * Sets the {@code metricRegistry} and returns a reference to this Builder enabling method chaining.
         *
         * @param metricRegistry instance specific metric registry
         * @return a reference to this Builder
         */
        public Builder metricRegistry(MetricRegistry metricRegistry)
        {
            return update(b -> b.metricRegistry = metricRegistry);
        }

        /**
         * Returns a {@code InstanceMetadataImpl} built from the parameters previously set.
         *
         * @return a {@code InstanceMetadataImpl} built with parameters of this {@code InstanceMetadataImpl.Builder}
         */
        @Override
        public InstanceMetadataImpl build()
        {
            Objects.requireNonNull(id);
            Objects.requireNonNull(metricRegistry);

            metrics = new InstanceMetricsImpl(metricRegistry);

            return new InstanceMetadataImpl(this);
        }
    }
}
