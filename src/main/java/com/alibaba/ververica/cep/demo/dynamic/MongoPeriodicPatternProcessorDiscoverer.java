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

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.cep.dynamic.impl.json.deserializer.ConditionSpecStdDeserializer;
import org.apache.flink.cep.dynamic.impl.json.deserializer.NodeSpecStdDeserializer;
import org.apache.flink.cep.dynamic.impl.json.deserializer.TimeStdDeserializer;
import org.apache.flink.cep.dynamic.impl.json.spec.ConditionSpec;
import org.apache.flink.cep.dynamic.impl.json.spec.GraphSpec;
import org.apache.flink.cep.dynamic.impl.json.spec.NodeSpec;
import org.apache.flink.cep.dynamic.processor.PatternProcessor;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.StringUtils;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

/**
 * The Mongo implementation of the {@link PeriodicPatternProcessorDiscoverer} that periodically
 * discovers the rule updates from the database by using Mongo.
 *
 * @param <T> Base type of the elements appearing in the pattern.
 */
public class MongoPeriodicPatternProcessorDiscoverer<T> extends PeriodicPatternProcessorDiscoverer<T> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoPeriodicPatternProcessorDiscoverer.class);

    private final List<PatternProcessor<T>> initialPatternProcessors;

    private final ClassLoader userCodeClassLoader;

    private final MongoClient mongoClient;

    private final MongoCollection<PatternRecord> collection;

    private Map<String, Tuple4<String, Integer, String, String>> latestPatternProcessors;

    /**
     * Creates a new using the given initial {@link PatternProcessor} and the time interval how
     * often to check the pattern processor updates.
     *
     * @param connectionString         The Mongo connectionString of the databaseName.
     * @param databaseName             The databaseName to use.
     * @param collectionName           The collectionName to use.
     * @param userCodeClassLoader      The userCodeClassLoader to use.
     * @param initialPatternProcessors The list of the initial {@link PatternProcessor}.
     * @param intervalMillis           Time interval in milliseconds how often to check updates.
     */
    public MongoPeriodicPatternProcessorDiscoverer(final String connectionString, final String databaseName, final String collectionName, final ClassLoader userCodeClassLoader, @Nullable final List<PatternProcessor<T>> initialPatternProcessors, @Nullable final Long intervalMillis) throws Exception {
        super(intervalMillis);
        this.initialPatternProcessors = initialPatternProcessors;
        this.userCodeClassLoader = userCodeClassLoader;
        try {
            CodecRegistry pojoCodecRegistry = fromRegistries(
                    MongoClientSettings.getDefaultCodecRegistry(),
                    fromProviders(PojoCodecProvider.builder().automatic(true).build()));
            MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(connectionString))
                    .codecRegistry(pojoCodecRegistry)
                    .build();
            this.mongoClient = MongoClients.create(mongoClientSettings);
            this.collection = mongoClient.getDatabase(databaseName).getCollection(collectionName, PatternRecord.class);
        } catch (Exception e) {
            LOG.error("Create Mongo Connection failed.", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean arePatternProcessorsUpdated() {
        if (latestPatternProcessors == null && !CollectionUtil.isNullOrEmpty(initialPatternProcessors)) {
            return true;
        }

        if (collection == null) {
            return false;
        }
        try {
            Map<String, Tuple4<String, Integer, String, String>> currentPatternProcessors = new HashMap<>();
            FindIterable<PatternRecord> patternRecords = collection.find();
            for (PatternRecord record : patternRecords) {
                if (currentPatternProcessors.containsKey(record.getId()) && currentPatternProcessors.get(record.getId()).f1 >= record.getVersion()) {
                    continue;
                }
                currentPatternProcessors.put(record.getId(), new Tuple4<>(requireNonNull(record.getId()), record.getVersion(), requireNonNull(record.getPattern()), record.getFunction()));
            }
            if (latestPatternProcessors == null || isPatternProcessorUpdated(currentPatternProcessors)) {
                latestPatternProcessors = currentPatternProcessors;
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            LOG.warn("Pattern processor discoverer failed to check rule changes, will recreate connection - ", e);
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<PatternProcessor<T>> getLatestPatternProcessors() {
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(
                        new SimpleModule()
                                .addDeserializer(ConditionSpec.class, ConditionSpecStdDeserializer.INSTANCE)
                                .addDeserializer(Time.class, TimeStdDeserializer.INSTANCE)
                                .addDeserializer(NodeSpec.class, NodeSpecStdDeserializer.INSTANCE)
                );

        return latestPatternProcessors.values()
                .stream()
                .map(patternProcessor -> {
                    try {
                        String patternStr = patternProcessor.f2;
                        GraphSpec graphSpec = objectMapper.readValue(patternStr, GraphSpec.class);
                        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
                        System.out.println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(graphSpec));
                        PatternProcessFunction<T, ?> patternProcessFunction = null;
                        String id = patternProcessor.f0;
                        int version = patternProcessor.f1;

                        if (!StringUtils.isNullOrWhitespaceOnly(patternProcessor.f3)) {
                            patternProcessFunction = (PatternProcessFunction<T, ?>) this.userCodeClassLoader.loadClass(patternProcessor.f3).getConstructor(String.class, int.class).newInstance(id, version);
                        }
                        LOG.warn(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(patternProcessor.f2));
                        return new DefaultPatternProcessor<>(patternProcessor.f0, patternProcessor.f1, patternStr, patternProcessFunction, this.userCodeClassLoader);
                    } catch (Exception e) {
                        LOG.error("Get the latest pattern processors of the discoverer failure. - ", e);
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    @Override
    public void close() throws IOException {
        super.close();
        if (mongoClient != null) {
            mongoClient.close();
            LOG.info("Mongo connection is closed.");
        }
    }

    private boolean isPatternProcessorUpdated(Map<String, Tuple4<String, Integer, String, String>> currentPatternProcessors) {
        return latestPatternProcessors.size() != currentPatternProcessors.size() || !currentPatternProcessors.equals(latestPatternProcessors);
    }
}


class PatternRecord {

    private String id;

    private int version;

    private String pattern;

    private String function;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    public String getFunction() {
        return function;
    }

    public void setFunction(String function) {
        this.function = function;
    }
}
