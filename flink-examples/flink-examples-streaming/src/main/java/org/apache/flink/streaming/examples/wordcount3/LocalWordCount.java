package org.apache.flink.streaming.examples.wordcount3;

import static org.apache.flink.streaming.examples.wordcount2.JobConfig.CHECKPOINT_PATH;
import static org.apache.flink.streaming.examples.wordcount2.JobConfig.SHARING_GROUP;
import static org.apache.flink.streaming.examples.wordcount2.JobConfig.STATE_BACKEND;

public class LocalWordCount {
    public static void main(String[] args) throws Exception {
        String[] parameters = new String[] {
                "-" + STATE_BACKEND.key(),
                "forst",
                "-" + CHECKPOINT_PATH.key(),
                "file:///tmp",
                "-" + SHARING_GROUP.key(),
                "true"
        };
        WordCount.main(parameters);
    }
}
