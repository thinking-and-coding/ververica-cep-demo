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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KTD, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.ververica.cep.demo.dynamic;

import org.apache.flink.cep.dynamic.processor.PatternProcessor;
import org.apache.flink.cep.dynamic.processor.PatternProcessorDiscoverer;

import javax.annotation.Nullable;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * The JDBC implementation of the {@link PeriodicPatternProcessorDiscovererFactory} that creates the
 * {@link JDBCPeriodicPatternProcessorDiscoverer} instance.
 *
 * @param <T> Base type of the elements appearing in the pattern.
 */
public class MongoPeriodicPatternProcessorDiscovererFactory<T> extends PeriodicPatternProcessorDiscovererFactory<T> {

    private final String uri;
    private final String database;
    private final String collection;

    public MongoPeriodicPatternProcessorDiscovererFactory(final String uri, final String database, final String collection, @Nullable final List<PatternProcessor<T>> initialPatternProcessors, @Nullable final Long intervalMillis) {
        super(initialPatternProcessors, intervalMillis);
        this.uri = requireNonNull(uri);
        this.database = requireNonNull(database);
        this.collection = requireNonNull(collection);
    }

    @Override
    public PatternProcessorDiscoverer<T> createPatternProcessorDiscoverer(ClassLoader userCodeClassLoader) throws Exception {
        return new MongoPeriodicPatternProcessorDiscoverer<>(uri, database, collection, userCodeClassLoader, this.getInitialPatternProcessors(), getIntervalMillis());
    }
}
