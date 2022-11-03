package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.test.checkpointing.ChangelogRecoveryITCaseBase.ArtificialFailure;
import org.apache.flink.test.checkpointing.ChangelogRecoveryITCaseBase.ControlledSource;
import org.apache.flink.test.checkpointing.ChangelogRecoveryITCaseBase.CountFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.testutils.junit.SharedObjects;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.apache.flink.runtime.testutils.CommonTestUtils.getLatestCompletedCheckpointPath;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS;
import static org.apache.flink.test.checkpointing.ChangelogRecoveryITCaseBase.getAllStateHandleId;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class ConcurrentCheckpointITCase extends TestLogger {

    protected MiniClusterWithClientResource cluster;

    @ClassRule
    public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    @Rule
    public final SharedObjects sharedObjects = SharedObjects.create();

    @Before
    public void setup() throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 1000);
        configuration.setInteger(MAX_CONCURRENT_CHECKPOINTS, 10);
        // reduce file threshold to reproduce FLINK-28843
        configuration.set(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, MemorySize.parse("20b"));
        cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(configuration)
                                .setNumberTaskManagers(1)
                                .setNumberSlotsPerTaskManager(4)
                                .build());
        cluster.before();
    }

    @Test
    public void testConcurrentCheckpoint() throws Exception {
        File firstCheckpointFolder = TEMPORARY_FOLDER.newFolder();
        MiniCluster miniCluster = cluster.getMiniCluster();
        EmbeddedRocksDBStateBackend rocksDBStateBackend = new EmbeddedRocksDBStateBackend(true);
        StreamExecutionEnvironment env1 =
                getEnv(rocksDBStateBackend, firstCheckpointFolder, 10);
        SharedReference<MiniCluster> miniClusterRef = sharedObjects.add(miniCluster);

        JobGraph firstJobGraph =
                buildJobGraph(env1, 2000, 2500, miniClusterRef);

        try {
            miniCluster.submitJob(firstJobGraph).get();
            miniCluster.requestJobResult(firstJobGraph.getJobID()).get();
        } catch (Exception ex) {
            Preconditions.checkState(
                    ExceptionUtils.findThrowable(ex, ArtificialFailure.class).isPresent());
        }

        String firstRestorePath = getLatestCompletedCheckpointPath(firstJobGraph.getJobID(), miniCluster).get();
        long lastCp = miniCluster.getExecutionGraph(firstJobGraph.getJobID()).get()
                .getCheckpointStatsSnapshot().getHistory().getLatestCompletedCheckpoint().getCheckpointId();
        int earliestCp = 1;
        while (earliestCp < lastCp) {
            File earliestPath = new File(firstCheckpointFolder.getPath() + "/" + firstJobGraph.getJobID() + "/chk-" + earliestCp);
            if (earliestPath.exists()) {
                firstRestorePath = earliestPath.getAbsolutePath();
                break;
            }
            earliestCp += 1;
        }
        if (earliestCp == lastCp) {
            fail("can't find earliest checkpoint, please re-run");
        }
        // restore from broken chk
        File secondCheckpointFolder = TEMPORARY_FOLDER.newFolder();
        StreamExecutionEnvironment env2 =
                getEnv(rocksDBStateBackend, secondCheckpointFolder, 100);
        JobGraph secondJobGraph =
                buildJobGraph(env2, 10000, 10001, miniClusterRef);
        secondJobGraph.setSavepointRestoreSettings(
                SavepointRestoreSettings.forPath(firstRestorePath, false, RestoreMode.CLAIM));
        miniCluster.submitJob(secondJobGraph).get();
        JobResult jobResult = miniCluster.requestJobResult(secondJobGraph.getJobID()).get();
        assertFalse(jobResult.isSuccess());
        Preconditions.checkState(jobResult.getSerializedThrowable().get().getFullStringifiedStackTrace().contains("FileNotFound"));
    }

    private JobGraph buildJobGraph(
            StreamExecutionEnvironment env,
            int waitingOnIndex,
            int failIndex,
            SharedReference<MiniCluster> miniCluster) {
        SharedReference<JobID> jobID = sharedObjects.add(new JobID());
        ControlledSource controlledSource = new ControlledSource() {
            @Override
            protected void beforeElement(SourceContext<Integer> ctx) throws Exception {
                if (currentIndex == waitingOnIndex) {
                    waitWhile(
                            () ->
                                    getAllStateHandleId(jobID.get(), miniCluster.get())
                                            .isEmpty());
                } else if (currentIndex > failIndex) {
                    throwArtificialFailure();
                }
            }
        };
        KeyedStream<Integer, Integer> keyedStream =
                env.addSource(controlledSource).keyBy(element -> element);
        keyedStream.process(new CountFunction()).addSink(new DiscardingSink<>()).setParallelism(1);
        return env.getStreamGraph().getJobGraph(env.getClass().getClassLoader(), jobID.get());
    }

    private StreamExecutionEnvironment getEnv(
            StateBackend stateBackend,
            File checkpointFile,
            long checkpointInterval) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(checkpointInterval);
        env.getCheckpointConfig().enableUnalignedCheckpoints(false);
        env.enableChangelogStateBackend(false);
        env.setStateBackend(stateBackend)
                .setRestartStrategy(RestartStrategies.fixedDelayRestart(0, 0));
        env.getCheckpointConfig().setCheckpointStorage(checkpointFile.toURI());
        env.getCheckpointConfig()
                .setExternalizedCheckpointCleanup(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(10);
        return env;
    }
}
