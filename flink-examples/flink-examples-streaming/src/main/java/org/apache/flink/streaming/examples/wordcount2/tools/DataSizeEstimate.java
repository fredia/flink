package org.apache.flink.streaming.examples.wordcount2.tools;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.ParameterTool;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Estimate size of serialized key/value.
 * NOTE: the way to generate string will influence the size. Please refer {@link StringSerializer}.
 */
public class DataSizeEstimate {

	private static char[] fatArray;

	private static char[] zeroArray;

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);

		int wordNumber = params.getInt("wordNumber", 20000000);

		int wordLength = params.getInt("wordLength", 16);

		String targetFile = params.get("targetFile", "estimate.dat");

		boolean zeroPadding = params.getBoolean("zeroPadding", false);

		fatArray = new char[wordLength];
		Random random = new Random(System.currentTimeMillis());
		for (int i = 0; i < fatArray.length; i++) {
			fatArray[i] = (char) random.nextInt();
		}

		zeroArray = new char[wordLength];

		Path path = new Path(targetFile);
		FSDataOutputStream fsDataOutputStream = path.getFileSystem().create(path, FileSystem.WriteMode.OVERWRITE);
		ByteArrayOutputStreamWithPos byteArrayOutputStreamWithPos = new ByteArrayOutputStreamWithPos();
		DataOutputView dataOutputView = new DataOutputViewStreamWrapper(byteArrayOutputStreamWithPos);

		Map<Integer, Integer> diffKeyLen = new HashMap<>();
		Map<Integer, Integer> diffValueLen = new HashMap<>();
		for (int i = 0; i < wordNumber; i++) {
			String key = zeroPadding ? covertToStringPaddingZero(i, wordLength) : covertToString(i, wordLength);
			StringSerializer.INSTANCE.serialize(key, dataOutputView);
			int keyLen = byteArrayOutputStreamWithPos.getPosition();
			IntSerializer.INSTANCE.serialize(1, dataOutputView);
			int valueLen = byteArrayOutputStreamWithPos.getPosition() - keyLen;
			fsDataOutputStream.write(byteArrayOutputStreamWithPos.getBuf(), 0, byteArrayOutputStreamWithPos.getPosition());
			byteArrayOutputStreamWithPos.reset();
			diffKeyLen.compute(keyLen, (len, count) -> count == null ? 1 : count + 1);
			diffValueLen.compute(valueLen, (len, count) -> count == null ? 1 : count + 1);
		}
		long totalLen = fsDataOutputStream.getPos();
		fsDataOutputStream.close();

		System.out.println("wordNumber: " + wordNumber + ", wordLength: " + wordLength + ", totalSize: " + totalLen);

		long totalKeySize = 0;
		for (Map.Entry<Integer, Integer> entry : diffKeyLen.entrySet()) {
			totalKeySize += entry.getKey() * (long) entry.getValue();
		}
		System.out.println("totalKeySize: " + totalKeySize + ", keySize: " + diffKeyLen);

		long totalValueSize = 0;
		for (Map.Entry<Integer, Integer> entry : diffValueLen.entrySet()) {
			totalValueSize += entry.getKey() * (long) entry.getValue();
		}
		System.out.println("totalValueSize: " + totalValueSize + ", valueSize: " + diffValueLen);
	}

	private static String covertToString(int number, int wordLen) {
		String a = String.valueOf(number);
		StringBuilder builder = new StringBuilder(wordLen);
		builder.append(a);
		builder.append(fatArray, 0, wordLen - a.length());
		return builder.toString();
	}

	private static String covertToStringPaddingZero(int number, int wordLen) {
		String a = String.valueOf(number);
		StringBuilder builder = new StringBuilder(wordLen);
		builder.append(a);
		builder.append(zeroArray, 0, wordLen - a.length());
		return builder.toString();
	}
}
