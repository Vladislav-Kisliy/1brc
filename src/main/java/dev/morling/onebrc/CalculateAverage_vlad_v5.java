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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class CalculateAverage_vlad_v5 {

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

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        int processors = Math.max(Runtime.getRuntime().availableProcessors() - 2, 2);
        BlockingDeque<List<String>> tasks = new LinkedBlockingDeque<>();
        // ExecutorService pool = Executors.newFixedThreadPool(processors + 1);
        ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
        Future<?> producerFuture = pool.submit(new StringProducer(tasks));

        Future<Map<String, MeasurementAggregator>>[] results = new Future[processors];
        for (int i = 0; i < processors; i++) {
            results[i] = pool.submit(new StringConsumer(tasks));
        }
        System.out.println("Threads were created. Wait results");
        producerFuture.get();

        Map<String, MeasurementAggregator> aggregates = new HashMap<>(INITIAL_CAPACITY);
        for (int i = 0; i < processors; i++) {
            Map<String, MeasurementAggregator> localAggregates = results[i].get();
            System.out.println("localAggr =" + localAggregates);
            for (Map.Entry<String, MeasurementAggregator> entry : localAggregates.entrySet()) {
                String key = entry.getKey();
                MeasurementAggregator aggregator = aggregates.get(key);
                MeasurementAggregator value = entry.getValue();
                if (aggregator == null) {
                    aggregates.put(key, value);
                }
                else {
                    aggregates.put(key, aggregator.merge(value));
                }
            }
        }
        pool.shutdown();
        // Output result
        // System.out.println(aggregates);
        Map<String, ResultRow> measurements = new TreeMap<>();
        for (Map.Entry<String, MeasurementAggregator> entry : aggregates.entrySet()) {
            String key = entry.getKey();
            MeasurementAggregator agg = entry.getValue();
            var row = new ResultRow(agg.min, (Math.round(agg.sum * 10.0) / 10.0) / agg.count, agg.max);
            measurements.put(key, row);
        }
        System.out.println(measurements);

        if (!pool.awaitTermination(1, TimeUnit.MILLISECONDS)) {
            System.err.println("Pool didn't stop");
            pool.shutdownNow();
        }
        Files.writeString(Paths.get(FILE + ".out"), measurements.toString());
        CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> "Result");
        future.thenAfpply()

    }

    public static class StringProducer implements Runnable {

        private static final int MAX_ELEMENTS = 10_000;
        private final BlockingDeque<List<String>> tasks;

        public StringProducer(BlockingDeque<List<String>> tasks) {
            this.tasks = tasks;
        }

        @Override
        public void run() {
            var start = Instant.now();
            // StringBuilder builder = new StringBuilder();
            List<String> pack = new ArrayList<>(MAX_ELEMENTS);
            int counter = 0;
            try (BufferedReader br = new BufferedReader(new FileReader(FILE + ".txt", StandardCharsets.UTF_8))) {
                String readLine;
                while ((readLine = br.readLine()) != null) {
                    // System.out.println("["+readLine+"]");
                    if (counter < MAX_ELEMENTS) {
                        pack.add(readLine);
                        counter++;
                    }
                    else {
                        if (!tasks.offer(pack)) {
                            System.out.println("Couldn't put a message to the queue 1s. Skip it");
                        }
                        pack = new ArrayList(MAX_ELEMENTS);
                        counter = 0;
                    }
                }
                tasks.offer(pack);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            System.out.println("All lines were read. Duration =" + Duration.between(start, Instant.now()));
            // System.out.println("builder =" + pack);
        }
    }

    public static class StringConsumer implements Callable<Map<String, MeasurementAggregator>> {

        private final BlockingDeque<List<String>> tasks;
        private final Map<String, MeasurementAggregator> localAggregates;

        public StringConsumer(BlockingDeque<List<String>> tasks) {
            this.tasks = tasks;
            this.localAggregates = new HashMap<>(INITIAL_CAPACITY);
        }

        @Override
        public Map<String, MeasurementAggregator> call() throws Exception {
            List<String> pack;
            while ((pack = tasks.poll(60, TimeUnit.MILLISECONDS)) != null) {
                // System.out.println("got pack ="+pack.size());
                for (String readLine : pack) {
                    String[] strings = readLine.split(";");
                    String key = strings[0];
                    var t = Double.parseDouble(strings[1]);
                    MeasurementAggregator item = localAggregates.get(key);
                    if (item == null) {
                        var m = new MeasurementAggregator();
                        m.sample(t);
                        localAggregates.put(key, m);
                    }
                    else {
                        item.sample(t);
                    }
                }
            }
            // System.out.println("Quit. " + Thread.currentThread().getName());
            return localAggregates;
        }
    }
}
