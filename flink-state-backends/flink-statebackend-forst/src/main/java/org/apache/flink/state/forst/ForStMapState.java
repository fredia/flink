/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forst;

import org.apache.flink.api.common.state.v2.MapState;
import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.state.InternalStateFuture;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.v2.InternalMapState;
import org.apache.flink.runtime.state.v2.MapStateDescriptor;
import org.apache.flink.runtime.state.v2.StateDescriptor;
import org.apache.flink.state.forst.ForStDBIterRequest.ResultType;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ColumnFamilyHandle;

import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The {@link InternalMapState} implement for ForStDB.
 *
 * @param <K> The type of the key.
 * @param <UK> The type of the user key.
 * @param <UV> The type of the user value.
 */
public class ForStMapState<K, UK, UV> extends InternalMapState<K, UK, UV>
        implements MapState<UK, UV>, ForStInnerTable<ContextKey<K>, UV> {

    /** The column family which this internal value state belongs to. */
    private final ColumnFamilyHandle columnFamilyHandle;

    /** The serialized key builder which should be thread-safe. */
    private final ThreadLocal<SerializedCompositeKeyBuilder<K>> serializedKeyBuilder;

    /** The data outputStream used for value serializer, which should be thread-safe. */
    final ThreadLocal<DataOutputSerializer> valueSerializerView;

    final ThreadLocal<DataInputDeserializer> keyDeserializerView;

    /** The data inputStream used for value deserializer, which should be thread-safe. */
    final ThreadLocal<DataInputDeserializer> valueDeserializerView;

    /** Serializer for the user keys. */
    final TypeSerializer<UK> userKeySerializer;

    /** Serializer for the user values. */
    final TypeSerializer<UV> userValueSerializer;

    /** Number of bytes required to prefix the key groups. */
    private final int keyGroupPrefixBytes;

    public ForStMapState(
            StateRequestHandler stateRequestHandler,
            ColumnFamilyHandle columnFamily,
            MapStateDescriptor<UK, UV> stateDescriptor,
            Supplier<SerializedCompositeKeyBuilder<K>> serializedKeyBuilderInitializer,
            Supplier<DataOutputSerializer> valueSerializerViewInitializer,
            Supplier<DataInputDeserializer> keyDeserializerViewInitializer,
            Supplier<DataInputDeserializer> valueDeserializerViewInitializer,
            int keyGroupPrefixBytes) {
        super(stateRequestHandler, stateDescriptor);
        this.columnFamilyHandle = columnFamily;
        this.serializedKeyBuilder = ThreadLocal.withInitial(serializedKeyBuilderInitializer);
        this.valueSerializerView = ThreadLocal.withInitial(valueSerializerViewInitializer);
        this.keyDeserializerView = ThreadLocal.withInitial(keyDeserializerViewInitializer);
        this.valueDeserializerView = ThreadLocal.withInitial(valueDeserializerViewInitializer);
        this.userKeySerializer = stateDescriptor.getUserKeySerializer();
        this.userValueSerializer = stateDescriptor.getSerializer();
        this.keyGroupPrefixBytes = keyGroupPrefixBytes;
    }

    public int getKeyGroupPrefixBytes() {
        return keyGroupPrefixBytes;
    }

    public ColumnFamilyHandle getColumnFamilyHandle() {
        return columnFamilyHandle;
    }

    @Override
    public byte[] serializeKey(ContextKey<K> contextKey) throws IOException {
        contextKey.resetExtra();
        return contextKey.getOrCreateSerializedKey(
                ctxKey -> {
                    SerializedCompositeKeyBuilder<K> builder = serializedKeyBuilder.get();
                    builder.setKeyAndKeyGroup(ctxKey.getRawKey(), ctxKey.getKeyGroup());
                    builder.setNamespace(VoidNamespace.get(), VoidNamespaceSerializer.INSTANCE);
                    if (contextKey.getUserKey() == null) {
                        return builder.build();
                    }
                    UK userKey = (UK) contextKey.getUserKey();
                    return builder.buildCompositeKeyUserKey(userKey, userKeySerializer);
                });
    }

    @Override
    public byte[] serializeValue(UV value) throws IOException {
        DataOutputSerializer outputView = valueSerializerView.get();
        outputView.clear();
        userValueSerializer.serialize(value, outputView);
        return outputView.getCopyOfBuffer();
    }

    public UK deserializeUserKey(byte[] userKeyBytes, int userKeyOffset) throws IOException {
        DataInputDeserializer inputView = keyDeserializerView.get();
        inputView.setBuffer(userKeyBytes, userKeyOffset, userKeyBytes.length - userKeyOffset);
        return userKeySerializer.deserialize(inputView);
    }

    @Override
    public UV deserializeValue(byte[] valueBytes) throws IOException {
        DataInputDeserializer inputView = valueDeserializerView.get();
        inputView.setBuffer(valueBytes);
        return userValueSerializer.deserialize(inputView);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public ForStDBGetRequest<ContextKey<K>, UV> buildDBGetRequest(
            StateRequest<?, ?, ?> stateRequest) {
        Preconditions.checkArgument(
                stateRequest.getRequestType() == StateRequestType.MAP_GET
                        || stateRequest.getRequestType() == StateRequestType.MAP_CONTAINS
                        || stateRequest.getRequestType() == StateRequestType.MAP_IS_EMPTY);
        ContextKey<K> contextKey =
                new ContextKey<>(
                        (RecordContext<K>) stateRequest.getRecordContext(),
                        stateRequest.getPayload());
        return ForStDBGetRequest.of(
                contextKey,
                this,
                stateRequest.getFuture(),
                stateRequest.getRequestType() != StateRequestType.MAP_GET,
                stateRequest.getRequestType() == StateRequestType.MAP_IS_EMPTY,
                stateRequestHandler.getDisposer());
    }

    @Override
    @SuppressWarnings("rawtypes")
    public ForStDBPutRequest<ContextKey<K>, UV> buildDBPutRequest(
            StateRequest<?, ?, ?> stateRequest) {
        ContextKey<K> contextKey =
                new ContextKey<>(
                        (RecordContext<K>) stateRequest.getRecordContext(),
                        ((Tuple2<UK, UV>) stateRequest.getPayload()).f0);
        UV value = null;
        if (stateRequest.getRequestType() == StateRequestType.MAP_PUT) {
            value = ((Tuple2<UK, UV>) stateRequest.getPayload()).f1;
        }

        return ForStDBPutRequest.of(
                contextKey,
                value,
                this,
                (InternalStateFuture<Void>) stateRequest.getFuture(),
                stateRequestHandler.getDisposer());
    }

    /**
     * Build a request for bunch put. Maily used for {@link StateRequestType#MAP_PUT_ALL} and {@link
     * StateRequestType#CLEAR}.
     *
     * @param stateRequest The state request.
     * @return The {@code ForStDBBunchPutRequest}.
     */
    @SuppressWarnings("rawtypes")
    public ForStDBBunchPutRequest<ContextKey<K>> buildDBBunchPutRequest(
            StateRequest<?, ?, ?> stateRequest) {
        ContextKey<K> contextKey =
                new ContextKey<>((RecordContext<K>) stateRequest.getRecordContext(), null);
        Map<UK, UV> value = (Map<UK, UV>) stateRequest.getPayload();
        return new ForStDBBunchPutRequest(
                contextKey,
                value,
                this,
                stateRequest.getFuture(),
                stateRequestHandler.getDisposer());
    }

    /**
     * Build a request for iterator. Used for {@link StateRequestType#MAP_ITER}, {@link
     * StateRequestType#MAP_ITER_KEY}, {@link StateRequestType#MAP_ITER_VALUE} and {@link
     * StateRequestType#ITERATOR_LOADING}.
     *
     * @param stateRequest The state request.
     * @return The {@code ForStDBIterRequest}.
     */
    @SuppressWarnings("rawtypes,unchecked")
    public ForStDBIterRequest buildDBIterRequest(StateRequest<?, ?, ?> stateRequest) {
        Preconditions.checkArgument(
                stateRequest.getRequestType() == StateRequestType.MAP_ITER
                        || stateRequest.getRequestType() == StateRequestType.MAP_ITER_KEY
                        || stateRequest.getRequestType() == StateRequestType.MAP_ITER_VALUE
                        || stateRequest.getRequestType() == StateRequestType.ITERATOR_LOADING);
        ContextKey<K> contextKey =
                new ContextKey<>((RecordContext<K>) stateRequest.getRecordContext(), null);
        byte[] seekBytes = null;
        ResultType resultType;
        switch (stateRequest.getRequestType()) {
            case MAP_ITER:
                resultType = ResultType.ENTRY;
                break;
            case MAP_ITER_KEY:
                resultType = ResultType.KEY;
                break;
            case MAP_ITER_VALUE:
                resultType = ResultType.VALUE;
                break;
            case ITERATOR_LOADING:
                Tuple2<ResultType, byte[]> payload =
                        (Tuple2<ResultType, byte[]>) stateRequest.getPayload();
                resultType = payload.f0;
                seekBytes = payload.f1;
                break;
            default:
                throw new IllegalArgumentException(
                        "Unknown request type: " + stateRequest.getRequestType());
        }
        return new ForStDBIterRequest(
                resultType,
                contextKey,
                this,
                stateRequestHandler,
                stateRequest.getFuture(),
                seekBytes,
                stateRequestHandler.getDisposer());
    }

    static <UK, UV, K, SV, S extends State> S create(
            StateDescriptor<SV> stateDescriptor,
            StateRequestHandler stateRequestHandler,
            ColumnFamilyHandle columnFamily,
            Supplier<SerializedCompositeKeyBuilder<K>> serializedKeyBuilderInitializer,
            Supplier<DataOutputSerializer> valueSerializerViewInitializer,
            Supplier<DataInputDeserializer> keyDeserializerViewInitializer,
            Supplier<DataInputDeserializer> valueDeserializerViewInitializer,
            int keyGroupPrefixBytes) {
        return (S)
                new ForStMapState<>(
                        stateRequestHandler,
                        columnFamily,
                        (MapStateDescriptor<UK, UV>) stateDescriptor,
                        serializedKeyBuilderInitializer,
                        valueSerializerViewInitializer,
                        keyDeserializerViewInitializer,
                        valueDeserializerViewInitializer,
                        keyGroupPrefixBytes);
    }
}
