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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.HeapPriorityQueuesManager;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.state.forst.iterator.ForStStateKeysAndNamespaceIterator;
import org.apache.flink.state.forst.iterator.ForStStateKeysIterator;
import org.apache.flink.state.forst.ttl.ForStTtlCompactFiltersManager;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ResourceGuard;
import org.apache.flink.util.StateMigrationException;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RunnableFuture;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * An {@link AbstractKeyedStateBackend} that stores its state in {@code ForSt} and serializes state
 * to streams provided by a {@link org.apache.flink.runtime.state.CheckpointStreamFactory} upon
 * checkpointing. This state backend can store very large state that exceeds memory and spills to
 * disk. Except for the snapshotting, this class should be accessed as if it is not threadsafe.
 *
 * <p>This class follows the rules for closing/releasing native ForSt resources as described in + <a
 * href="https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#opening-a-database-with-column-families">
 * this document</a>. TODO: Support the new interface of KeyedStateBackend.
 */
public class ForStKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

    private static final Logger LOG = LoggerFactory.getLogger(ForStKeyedStateBackend.class);

    /**
     * The name of the merge operator in ForSt. Do not change except you know exactly what you do.
     */
    public static final String MERGE_OPERATOR_NAME = "stringappendtest";

    private static final Map<StateDescriptor.Type, StateCreateFactory> STATE_CREATE_FACTORIES =
            Collections.emptyMap();

    private static final Map<StateDescriptor.Type, StateUpdateFactory> STATE_UPDATE_FACTORIES =
            Collections.emptyMap();

    private interface StateCreateFactory {
        <K, N, SV, S extends State, IS extends S> IS createState(
                StateDescriptor<S, SV> stateDesc,
                Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
                        registerResult,
                ForStKeyedStateBackend<K> backend)
                throws Exception;
    }

    private interface StateUpdateFactory {
        <K, N, SV, S extends State, IS extends S> IS updateState(
                StateDescriptor<S, SV> stateDesc,
                Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
                        registerResult,
                IS existingState)
                throws Exception;
    }

    /** Factory function to create column family options from state name. */
    private final Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory;

    /** The container of ForSt option factory and predefined options. */
    private final ForStResourceContainer optionsContainer;

    /** Path where this configured instance stores its data directory. */
    private final File instanceBasePath;

    /**
     * Protects access to ForSt in other threads, like the checkpointing thread from parallel call
     * that disposes the ForSt object.
     */
    private final ResourceGuard forStResourceGuard;

    /**
     * The read options to use when creating iterators. We ensure total order seek in case user
     * misuse, see FLINK-17800 for more details.
     */
    private final ReadOptions readOptions;

    /** Map of created k/v states. */
    private final Map<String, State> createdKVStates;

    /**
     * Information about the k/v states, maintained in the order as we create them. This is used to
     * retrieve the column family that is used for a state and also for sanity checks when
     * restoring.
     */
    private final LinkedHashMap<String, ForStKvStateInfo> kvStateInformation;

    private final HeapPriorityQueuesManager heapPriorityQueuesManager;

    /** Number of bytes required to prefix the key groups. */
    private final int keyGroupPrefixBytes;

    /**
     * We are not using the default column family for Flink state ops, but we still need to remember
     * this handle so that we can close it properly when the backend is closed. Note that the one
     * returned by {@link RocksDB#open(String)} is different from that by {@link
     * RocksDB#getDefaultColumnFamily()}, probably it's a bug of RocksDB java API.
     */
    private final ColumnFamilyHandle defaultColumnFamily;

    /** Shared wrapper for batch writes to the ForSt instance. */
    private final ForStWriteBatchWrapper writeBatchWrapper;

    /** The native metrics monitor. */
    private final ForStNativeMetricMonitor nativeMetricMonitor;

    /** Factory for priority queue state. */
    private final PriorityQueueSetFactory priorityQueueFactory;

    /**
     * Helper to build the byte arrays of composite keys to address data in ForSt. Shared across all
     * states.
     */
    private final SerializedCompositeKeyBuilder<K> sharedRocksKeyBuilder;

    /**
     * Our ForSt database. The different k/v states that we have don't each have their own ForSt
     * instance. They all write to this instance but to their own column family.
     */
    protected final RocksDB db;

    // mark whether this backend is already disposed and prevent duplicate disposing
    private boolean disposed = false;

    private final ForStTtlCompactFiltersManager ttlCompactFiltersManager;

    public ForStKeyedStateBackend(
            ClassLoader userCodeClassLoader,
            File instanceBasePath,
            ForStResourceContainer optionsContainer,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            RocksDB db,
            LinkedHashMap<String, ForStKvStateInfo> kvStateInformation,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            int keyGroupPrefixBytes,
            CloseableRegistry cancelStreamRegistry,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            ResourceGuard forStResourceGuard,
            ForStWriteBatchWrapper writeBatchWrapper,
            ColumnFamilyHandle defaultColumnFamilyHandle,
            ForStNativeMetricMonitor nativeMetricMonitor,
            SerializedCompositeKeyBuilder<K> sharedRocksKeyBuilder,
            PriorityQueueSetFactory priorityQueueFactory,
            ForStTtlCompactFiltersManager ttlCompactFiltersManager,
            InternalKeyContext<K> keyContext,
            @Nullable CompletableFuture<Void> asyncCompactFuture) {

        super(
                kvStateRegistry,
                keySerializer,
                userCodeClassLoader,
                executionConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig,
                cancelStreamRegistry,
                keyGroupCompressionDecorator,
                keyContext);

        this.ttlCompactFiltersManager = ttlCompactFiltersManager;

        // ensure that we use the right merge operator, because other code relies on this
        this.columnFamilyOptionsFactory = Preconditions.checkNotNull(columnFamilyOptionsFactory);

        this.optionsContainer = Preconditions.checkNotNull(optionsContainer);

        this.instanceBasePath = Preconditions.checkNotNull(instanceBasePath);

        this.keyGroupPrefixBytes = keyGroupPrefixBytes;
        this.kvStateInformation = kvStateInformation;
        this.createdKVStates = new HashMap<>();

        this.readOptions = optionsContainer.getReadOptions();
        this.db = db;
        this.forStResourceGuard = forStResourceGuard;
        this.writeBatchWrapper = writeBatchWrapper;
        this.defaultColumnFamily = defaultColumnFamilyHandle;
        this.nativeMetricMonitor = nativeMetricMonitor;
        this.sharedRocksKeyBuilder = sharedRocksKeyBuilder;
        this.priorityQueueFactory = priorityQueueFactory;
        if (priorityQueueFactory instanceof HeapPriorityQueueSetFactory) {
            this.heapPriorityQueuesManager =
                    new HeapPriorityQueuesManager(
                            registeredPQStates,
                            (HeapPriorityQueueSetFactory) priorityQueueFactory,
                            keyContext.getKeyGroupRange(),
                            keyContext.getNumberOfKeyGroups());
        } else {
            this.heapPriorityQueuesManager = null;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <N> Stream<K> getKeys(String state, N namespace) {
        ForStKvStateInfo columnInfo = kvStateInformation.get(state);
        if (columnInfo == null
                || !(columnInfo.metaInfo instanceof RegisteredKeyValueStateBackendMetaInfo)) {
            return Stream.empty();
        }

        RegisteredKeyValueStateBackendMetaInfo<N, ?> registeredKeyValueStateBackendMetaInfo =
                (RegisteredKeyValueStateBackendMetaInfo<N, ?>) columnInfo.metaInfo;

        final TypeSerializer<N> namespaceSerializer =
                registeredKeyValueStateBackendMetaInfo.getNamespaceSerializer();
        final DataOutputSerializer namespaceOutputView = new DataOutputSerializer(8);
        boolean ambiguousKeyPossible =
                CompositeKeySerializationUtils.isAmbiguousKeyPossible(
                        getKeySerializer(), namespaceSerializer);
        final byte[] nameSpaceBytes;
        try {
            CompositeKeySerializationUtils.writeNameSpace(
                    namespace, namespaceSerializer, namespaceOutputView, ambiguousKeyPossible);
            nameSpaceBytes = namespaceOutputView.getCopyOfBuffer();
        } catch (IOException ex) {
            throw new FlinkRuntimeException("Failed to get keys from ForSt state backend.", ex);
        }

        ForStIteratorWrapper iterator =
                ForStOperationUtils.getForStIterator(
                        db, columnInfo.columnFamilyHandle, readOptions);
        iterator.seekToFirst();

        final ForStStateKeysIterator<K> iteratorWrapper =
                new ForStStateKeysIterator<>(
                        iterator,
                        state,
                        getKeySerializer(),
                        keyGroupPrefixBytes,
                        ambiguousKeyPossible,
                        nameSpaceBytes);

        Stream<K> targetStream =
                StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(iteratorWrapper, Spliterator.ORDERED),
                        false);
        return targetStream.onClose(iteratorWrapper::close);
    }

    @Override
    public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String state) {
        ForStKvStateInfo columnInfo = kvStateInformation.get(state);
        if (columnInfo == null
                || !(columnInfo.metaInfo instanceof RegisteredKeyValueStateBackendMetaInfo)) {
            return Stream.empty();
        }

        RegisteredKeyValueStateBackendMetaInfo<N, ?> registeredKeyValueStateBackendMetaInfo =
                (RegisteredKeyValueStateBackendMetaInfo<N, ?>) columnInfo.metaInfo;

        final TypeSerializer<N> namespaceSerializer =
                registeredKeyValueStateBackendMetaInfo.getNamespaceSerializer();
        boolean ambiguousKeyPossible =
                CompositeKeySerializationUtils.isAmbiguousKeyPossible(
                        getKeySerializer(), namespaceSerializer);

        ForStIteratorWrapper iterator =
                ForStOperationUtils.getForStIterator(
                        db, columnInfo.columnFamilyHandle, readOptions);
        iterator.seekToFirst();

        final ForStStateKeysAndNamespaceIterator<K, N> iteratorWrapper =
                new ForStStateKeysAndNamespaceIterator<>(
                        iterator,
                        state,
                        getKeySerializer(),
                        namespaceSerializer,
                        keyGroupPrefixBytes,
                        ambiguousKeyPossible);

        Stream<Tuple2<K, N>> targetStream =
                StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(iteratorWrapper, Spliterator.ORDERED),
                        false);
        return targetStream.onClose(iteratorWrapper::close);
    }

    @Override
    public void setCurrentKey(K newKey) {
        super.setCurrentKey(newKey);
        sharedRocksKeyBuilder.setKeyAndKeyGroup(getCurrentKey(), getCurrentKeyGroupIndex());
    }

    /** Should only be called by one thread, and only after all accesses to the DB happened. */
    @Override
    public void dispose() {
        if (this.disposed) {
            return;
        }
        super.dispose();

        // This call will block until all clients that still acquire access to the ForSt instance
        // have released it,
        // so that we cannot release the native resources while clients are still working with it in
        // parallel.
        forStResourceGuard.close();

        // IMPORTANT: null reference to signal potential async checkpoint workers that the db was
        // disposed, as
        // working on the disposed object results in SEGFAULTS.
        if (db != null) {

            IOUtils.closeQuietly(writeBatchWrapper);

            // Metric collection occurs on a background thread. When this method returns
            // it is guaranteed that thr ForSt reference has been invalidated
            // and no more metric collection will be attempted against the database.
            if (nativeMetricMonitor != null) {
                nativeMetricMonitor.close();
            }

            List<ColumnFamilyOptions> columnFamilyOptions =
                    new ArrayList<>(kvStateInformation.values().size());

            // ForSt's native memory management requires that *all* CFs (including default) are
            // closed before the
            // DB is closed. See:
            // https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#opening-a-database-with-column-families
            // Start with default CF ...
            ForStOperationUtils.addColumnFamilyOptionsToCloseLater(
                    columnFamilyOptions, defaultColumnFamily);
            IOUtils.closeQuietly(defaultColumnFamily);

            // ... continue with the ones created by Flink...
            for (ForStKvStateInfo kvStateInfo : kvStateInformation.values()) {
                ForStOperationUtils.addColumnFamilyOptionsToCloseLater(
                        columnFamilyOptions, kvStateInfo.columnFamilyHandle);
                IOUtils.closeQuietly(kvStateInfo.columnFamilyHandle);
            }

            // ... and finally close the DB instance ...
            IOUtils.closeQuietly(db);

            columnFamilyOptions.forEach(IOUtils::closeQuietly);

            IOUtils.closeQuietly(optionsContainer);

            ttlCompactFiltersManager.disposeAndClearRegisteredCompactionFactories();

            kvStateInformation.clear();

            cleanInstanceBasePath();
        }
        this.disposed = true;
    }

    @Nonnull
    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
        return create(stateName, byteOrderedElementSerializer, false);
    }

    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer,
                    boolean allowFutureMetadataUpdates) {
        if (this.heapPriorityQueuesManager != null) {
            return this.heapPriorityQueuesManager.createOrUpdate(
                    stateName, byteOrderedElementSerializer, allowFutureMetadataUpdates);
        } else {
            return priorityQueueFactory.create(
                    stateName, byteOrderedElementSerializer, allowFutureMetadataUpdates);
        }
    }

    private void cleanInstanceBasePath() {
        LOG.info(
                "Closed ForSt State Backend. Cleaning up ForSt working directory {}.",
                instanceBasePath);

        try {
            FileUtils.deleteDirectory(instanceBasePath);
        } catch (IOException ex) {
            LOG.warn("Could not delete ForSt working directory: {}", instanceBasePath, ex);
        }
    }

    // ------------------------------------------------------------------------
    //  Getters and Setters
    // ------------------------------------------------------------------------

    @VisibleForTesting
    PriorityQueueSetFactory getPriorityQueueFactory() {
        return priorityQueueFactory;
    }

    /**
     * Triggers an asynchronous snapshot of the keyed state backend from ForSt. This snapshot can be
     * canceled and is also stopped when the backend is closed through {@link #dispose()}. For each
     * backend, this method must always be called by the same thread.
     *
     * @param checkpointId The Id of the checkpoint.
     * @param timestamp The timestamp of the checkpoint.
     * @param streamFactory The factory that we can use for writing our state to streams.
     * @param checkpointOptions Options for how to perform this checkpoint.
     * @return Future to the state handle of the snapshot data.
     * @throws Exception indicating a problem in the synchronous part of the checkpoint.
     */
    @Nonnull
    @Override
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
            final long checkpointId,
            final long timestamp,
            @Nonnull final CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions) {

        // TODO: support snapshot
        throw new UnsupportedOperationException(
                "Not support snapshot yet for " + this.getClass().getName());
    }

    @Nonnull
    @Override
    public SavepointResources<K> savepoint() {

        // TODO: support savepoint
        throw new UnsupportedOperationException(
                "Not support savepoint yet for " + this.getClass().getName());
    }

    @Override
    public void notifyCheckpointComplete(long completedCheckpointId) {
        // do nothing since checkpoint is not supported currently.
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        // do nothing since checkpoint is not supported currently.
    }

    /**
     * Registers a k/v state information, which includes its state id, type, ForSt column family
     * handle, and serializers.
     *
     * <p>When restoring from a snapshot, we don’t restore the individual k/v states, just the
     * global ForSt database and the list of k/v state information. When a k/v state is first
     * requested we check here whether we already have a registered entry for that and return it
     * (after some necessary state compatibility checks) or create a new one if it does not exist.
     */
    private <N, S extends State, SV, SEV>
            Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
                    tryRegisterKvStateInformation(
                            StateDescriptor<S, SV> stateDesc,
                            TypeSerializer<N> namespaceSerializer,
                            @Nonnull StateSnapshotTransformFactory<SEV> snapshotTransformFactory,
                            boolean allowFutureMetadataUpdates)
                            throws Exception {

        ForStKvStateInfo oldStateInfo = kvStateInformation.get(stateDesc.getName());

        TypeSerializer<SV> stateSerializer = stateDesc.getSerializer();

        ForStKvStateInfo newRocksStateInfo;
        RegisteredKeyValueStateBackendMetaInfo<N, SV> newMetaInfo;
        if (oldStateInfo != null) {
            @SuppressWarnings("unchecked")
            RegisteredKeyValueStateBackendMetaInfo<N, SV> castedMetaInfo =
                    (RegisteredKeyValueStateBackendMetaInfo<N, SV>) oldStateInfo.metaInfo;

            newMetaInfo =
                    updateRestoredStateMetaInfo(
                            Tuple2.of(oldStateInfo.columnFamilyHandle, castedMetaInfo),
                            stateDesc,
                            namespaceSerializer,
                            stateSerializer);

            newMetaInfo =
                    allowFutureMetadataUpdates
                            ? newMetaInfo.withSerializerUpgradesAllowed()
                            : newMetaInfo;

            newRocksStateInfo = new ForStKvStateInfo(oldStateInfo.columnFamilyHandle, newMetaInfo);
            kvStateInformation.put(stateDesc.getName(), newRocksStateInfo);
        } else {
            newMetaInfo =
                    new RegisteredKeyValueStateBackendMetaInfo<>(
                            stateDesc.getType(),
                            stateDesc.getName(),
                            namespaceSerializer,
                            stateSerializer,
                            StateSnapshotTransformFactory.noTransform());

            newMetaInfo =
                    allowFutureMetadataUpdates
                            ? newMetaInfo.withSerializerUpgradesAllowed()
                            : newMetaInfo;

            newRocksStateInfo =
                    ForStOperationUtils.createStateInfo(
                            newMetaInfo,
                            db,
                            columnFamilyOptionsFactory,
                            ttlCompactFiltersManager,
                            optionsContainer.getWriteBufferManagerCapacity());
            ForStOperationUtils.registerKvStateInformation(
                    this.kvStateInformation,
                    this.nativeMetricMonitor,
                    stateDesc.getName(),
                    newRocksStateInfo);
        }

        return Tuple2.of(newRocksStateInfo.columnFamilyHandle, newMetaInfo);
    }

    private <N, S extends State, SV>
            RegisteredKeyValueStateBackendMetaInfo<N, SV> updateRestoredStateMetaInfo(
                    Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
                            oldStateInfo,
                    StateDescriptor<S, SV> stateDesc,
                    TypeSerializer<N> namespaceSerializer,
                    TypeSerializer<SV> stateSerializer)
                    throws Exception {

        RegisteredKeyValueStateBackendMetaInfo<N, SV> restoredKvStateMetaInfo = oldStateInfo.f1;

        // fetch current serializer now because if it is incompatible, we can't access
        // it anymore to improve the error message
        TypeSerializer<N> previousNamespaceSerializer =
                restoredKvStateMetaInfo.getNamespaceSerializer();

        TypeSerializerSchemaCompatibility<N> s =
                restoredKvStateMetaInfo.updateNamespaceSerializer(namespaceSerializer);
        if (s.isCompatibleAfterMigration() || s.isIncompatible()) {
            throw new StateMigrationException(
                    "The new namespace serializer ("
                            + namespaceSerializer
                            + ") must be compatible with the old namespace serializer ("
                            + previousNamespaceSerializer
                            + ").");
        }

        restoredKvStateMetaInfo.checkStateMetaInfo(stateDesc);

        // fetch current serializer now because if it is incompatible, we can't access
        // it anymore to improve the error message
        TypeSerializer<SV> previousStateSerializer = restoredKvStateMetaInfo.getStateSerializer();

        TypeSerializerSchemaCompatibility<SV> newStateSerializerCompatibility =
                restoredKvStateMetaInfo.updateStateSerializer(stateSerializer);
        if (newStateSerializerCompatibility.isCompatibleAfterMigration()
                || newStateSerializerCompatibility.isIncompatible()) {
            // TODO: support state migration
            throw new StateMigrationException(
                    "The new state serializer ("
                            + stateSerializer
                            + ") must not be incompatible with the old state serializer ("
                            + previousStateSerializer
                            + ").");
        }

        return restoredKvStateMetaInfo;
    }

    @Override
    @Nonnull
    public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc,
            @Nonnull StateSnapshotTransformFactory<SEV> snapshotTransformFactory)
            throws Exception {
        return createOrUpdateInternalState(
                namespaceSerializer, stateDesc, snapshotTransformFactory, false);
    }

    @Nonnull
    @Override
    public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc,
            @Nonnull StateSnapshotTransformFactory<SEV> snapshotTransformFactory,
            boolean allowFutureMetadataUpdates)
            throws Exception {
        Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>> registerResult =
                tryRegisterKvStateInformation(
                        stateDesc,
                        namespaceSerializer,
                        snapshotTransformFactory,
                        allowFutureMetadataUpdates);
        if (!allowFutureMetadataUpdates) {
            // Config compact filter only when no future metadata updates
            ttlCompactFiltersManager.configCompactFilter(
                    stateDesc, registerResult.f1.getStateSerializer());
        }

        return createState(stateDesc, registerResult);
    }

    private <N, SV, S extends State, IS extends S> IS createState(
            StateDescriptor<S, SV> stateDesc,
            Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
                    registerResult)
            throws Exception {
        @SuppressWarnings("unchecked")
        IS createdState = (IS) createdKVStates.get(stateDesc.getName());
        if (createdState == null) {
            StateCreateFactory stateCreateFactory = STATE_CREATE_FACTORIES.get(stateDesc.getType());
            if (stateCreateFactory == null) {
                throw new FlinkRuntimeException(stateNotSupportedMessage(stateDesc));
            }
            createdState =
                    stateCreateFactory.createState(
                            stateDesc, registerResult, ForStKeyedStateBackend.this);
        } else {
            StateUpdateFactory stateUpdateFactory = STATE_UPDATE_FACTORIES.get(stateDesc.getType());
            if (stateUpdateFactory == null) {
                throw new FlinkRuntimeException(stateNotSupportedMessage(stateDesc));
            }
            createdState = stateUpdateFactory.updateState(stateDesc, registerResult, createdState);
        }

        createdKVStates.put(stateDesc.getName(), createdState);
        return createdState;
    }

    private <S extends State, SV> String stateNotSupportedMessage(
            StateDescriptor<S, SV> stateDesc) {
        return String.format(
                "State %s is not supported by %s", stateDesc.getClass(), this.getClass());
    }

    /** Only visible for testing, DO NOT USE. */
    File getInstanceBasePath() {
        return instanceBasePath;
    }

    @VisibleForTesting
    @Override
    public int numKeyValueStateEntries() {
        int count = 0;

        for (ForStKvStateInfo metaInfo : kvStateInformation.values()) {
            // TODO maybe filterOrTransform only for k/v states
            try (ForStIteratorWrapper rocksIterator =
                    ForStOperationUtils.getForStIterator(
                            db, metaInfo.columnFamilyHandle, readOptions)) {
                rocksIterator.seekToFirst();

                while (rocksIterator.isValid()) {
                    count++;
                    rocksIterator.next();
                }
            }
        }

        return count;
    }

    @Override
    public boolean requiresLegacySynchronousTimerSnapshots(SnapshotType checkpointType) {
        return priorityQueueFactory instanceof HeapPriorityQueueSetFactory
                && !checkpointType.isSavepoint();
    }

    @Override
    public boolean isSafeToReuseKVState() {
        return true;
    }

    /** ForSt DB specific information about the k/v states. */
    public static class ForStKvStateInfo implements AutoCloseable {
        public final ColumnFamilyHandle columnFamilyHandle;
        public final RegisteredStateMetaInfoBase metaInfo;

        public ForStKvStateInfo(
                ColumnFamilyHandle columnFamilyHandle, RegisteredStateMetaInfoBase metaInfo) {
            this.columnFamilyHandle = columnFamilyHandle;
            this.metaInfo = metaInfo;
        }

        @Override
        public void close() throws Exception {
            this.columnFamilyHandle.close();
        }
    }
}
