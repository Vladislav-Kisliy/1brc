/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class CalculateAverage_vlad {

    private static final String FILE = "./measurements";
    public static final int INITIAL_CAPACITY = 1024;

    private static record Measurement(String station, double value) {
        private Measurement(String[] parts) {
            this(parts[0], Double.parseDouble(parts[1]));
        }
    }

    private static record ResultRow(double min, double mean, double max) {

        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    private static class MeasurementAggregator {
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        double sum = 0.0;
        long count = 0;

        public void sample(double temp) {
            min = Math.min(min, temp);
            max = Math.max(max, temp);
            sum += temp;
            count++;
        }

        public MeasurementAggregator merge(MeasurementAggregator m2) {
            min = Math.min(min, m2.min);
            max = Math.max(max, m2.max);
            sum += m2.sum;
            count += m2.count;
            return this;
        }

        @Override
        public String toString() {
            return round(min) + "/" + round(sum / count) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    // Inner class representing a key for measurements.
    static class Key {
        final byte[] data = new byte[32];
        int hash = 0, length = 0;

        // Override of hashCode using the hash field.
        @Override
        public int hashCode() {
            return hash;
        }

        // Custom equals method to compare two Key objects.
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Key k) {
                if (length != k.length || hash != k.hash) {
                    return false;
                }
                return Arrays.equals(data, 0, length, k.data, 0, length);
            }
            return false;
        }

        // Converts the key's data to a String.
        @Override
        public String toString() {
            return new String(data, 0, length, StandardCharsets.UTF_8);
        }

        // Appends a byte to the key and updates its hash.
        public void append(byte b) {
            data[length++] = b;
            hash = hash * 10191 + b;
        }

        // Resets the key to its initial state.
        public void clear() {
            length = hash = 0;
        }
    }

    static class FileChunk {
        final long i;
        final long chunk;
        final long length;
        final RandomAccessFile raf;

        public FileChunk(long i, long chunk, long length, RandomAccessFile raf) {
            this.i = i;
            this.chunk = chunk;
            this.length = length;
            this.raf = raf;
        }

        @Override
        public String toString() {
            return "FileChunk{" +
                    "i=" + i +
                    ", chunk=" + chunk +
                    ", length=" + length +
                    '}';
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        var start = Instant.now();
        File file = new File(FILE + ".txt");
        long length = file.length();
        long chunk = 1 << 27; // Size of the chunk to be processed.
        RandomAccessFile raf = new RandomAccessFile(file, "r");

        // ExecutorService pool = Executors.newFixedThreadPool(processors + 1);
        ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();

        int size = ((int) (length / chunk)) + 1;
        Future<Map<Key, MeasurementAggregator>>[] results = new Future[size];
        for (int i = 0; i < size; i++) {
            var fileChunk = new FileChunk(i, chunk, length, raf);
            // System.out.println("chunk =" + fileChunk);
            results[i] = pool.submit(new FileProcessor(fileChunk));
        }

        System.out.println("Threads were created. Wait results");

        Map<Key, MeasurementAggregator> aggregates = new HashMap<>(INITIAL_CAPACITY);
        for (int i = 0; i < size; i++) {
            Map<Key, MeasurementAggregator> localAggregates = results[i].get();
            for (Map.Entry<Key, MeasurementAggregator> entry : localAggregates.entrySet()) {
                Key key = entry.getKey();
                MeasurementAggregator aggregator = aggregates.get(key);
                MeasurementAggregator value = entry.getValue();
                if (aggregator == null) {
                    aggregates.put(key, value);
                } else {
                    aggregates.put(key, aggregator.merge(value));
                }
            }
        }

        pool.shutdown();
        // Output result
        // System.out.println(aggregates);
        Map<String, ResultRow> measurements = new TreeMap<>();
        for (Map.Entry<Key, MeasurementAggregator> entry : aggregates.entrySet()) {
            Key key = entry.getKey();
            MeasurementAggregator agg = entry.getValue();
            var row = new ResultRow(agg.min, (Math.round(agg.sum * 10.0) / 10.0) / agg.count, agg.max);
            measurements.put(key.toString(), row);
        }
        System.out.println(report(measurements));

        if (!pool.awaitTermination(1, TimeUnit.MILLISECONDS)) {
            System.err.println("Pool didn't stop");
            pool.shutdownNow();
        }
        System.out.println("All lines were read. Duration =" + Duration.between(start, Instant.now()));
        // Files.writeString(Paths.get(FILE + ".out"), measurements.toString());
    }

    public static class FileProcessor implements Callable<Map<Key, MeasurementAggregator>> {

        public static final int PADDING = 64 * 1024;
        private final FileChunk fc;
        private final Map<Key, MeasurementAggregator> localAggregates;

        public FileProcessor(FileChunk fc) {
            this.fc = fc;
            this.localAggregates = new HashMap<>(INITIAL_CAPACITY);
        }

        @Override
        public Map<Key, MeasurementAggregator> call() throws Exception {
            long start = fc.i * fc.chunk;
            long size = Math.min(fc.length, start + fc.chunk + PADDING) - start;
            try {
                MappedByteBuffer mbb = fc.raf.getChannel().map(FileChannel.MapMode.READ_ONLY, start, size);
                if (fc.i > 0) {
                    skipToFirstLine(mbb);
                }
                Key key = new Key();
                while (mbb.remaining() > 0 && mbb.position() <= fc.chunk) {
                    key.clear();
                    readKey(mbb, key);
                    int temp = readTemperatureFromBuffer(mbb);
                    var m = localAggregates.computeIfAbsent(key, n -> new MeasurementAggregator());
                    if (m.count == 0) {
                        key = new Key();
                    }
                    m.sample(temp / 10.0);
                }

            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
            // System.out.println("Quit. " + Thread.currentThread().getName());
            return localAggregates;
        }

        // Skips to the first line in the buffer, used for chunk processing.
        private static void skipToFirstLine(MappedByteBuffer mbb) {
            while ((mbb.get() & 0xFF) >= ' ') {
                // Skip bytes until reaching the start of a line.
            }
        }

        private static void readKey(MappedByteBuffer mbb, Key key) {
            while (mbb.remaining() > 0) {
                byte b = mbb.get();
                if (b == ';' || b == 0x0a || b == 0x0d) {
                    break;
                }
                key.append(b);
            }
        }

        // Reads a temperature value from the buffer.
        private static int readTemperatureFromBuffer(MappedByteBuffer mbb) {
            int temp = 0;
            boolean negative = false;
            outer:
            while (mbb.remaining() > 0) {
                byte b = mbb.get();
                switch (b) {
                    case '-':
                        negative = true;
                        break;
                    case '.':
                        break;
                    case '\r':
                    case '\n':
                        break outer;
                    default:
                        temp = 10 * temp + (b - '0');
                        break;
                }
            }
            if (mbb.remaining() > 0) {
                byte b = mbb.get(mbb.position());
                if (b == '\n') {
                    mbb.get();
                }
            }
            if (negative) {
                temp = -temp;
            }
            return temp;
        }
    }

    private static String report(Map<String, ResultRow> measurements) {
        StringBuilder sb = new StringBuilder(16384);
        sb.append("{");
        int counter = 0;
        for (Map.Entry<String, ResultRow> entry : measurements.entrySet()) {
            if (counter != 0) {
                sb.append(", ");
            }
            sb.append(entry.getKey());
            sb.append("=");
            sb.append(entry.getValue());
            counter++;
        }
        sb.append("}");
        return sb.toString();
    }
}
