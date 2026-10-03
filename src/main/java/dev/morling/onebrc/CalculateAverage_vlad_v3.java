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
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class CalculateAverage_vlad_v3 {

    private static final String FILE = "./measurements";

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

    ;

    private static class MeasurementAggregator {
        private double min;
        private double max;
        private double sum;
        private long count = 1l;

        public MeasurementAggregator(double start) {
            this.min = start;
            this.max = start;
            this.sum = start;
        }

        public void setMin(double min) {
            this.min = Math.min(this.min, min);
        }

        public void setMax(double max) {
            this.max = Math.max(this.max, max);
        }

        public void addSum(double sum) {
            this.sum += sum;
        }

        public void incrementCount() {
            this.count++;
        }

        @Override
        public String toString() {
            return "MeasurementAggregator{" +
                    "min=" + min +
                    ", max=" + max +
                    ", sum=" + sum +
                    ", count=" + count +
                    '}';
        }
    }

    public static void main(String[] args) throws IOException {
        // Collector<Measurement, MeasurementAggregator, ResultRow> collector = Collector.of(
        // MeasurementAggregator::new,
        // (a, m) -> {
        // a.min = Math.min(a.min, m.value);
        // a.max = Math.max(a.max, m.value);
        // a.sum += m.value;
        // a.count++;
        // },
        // (agg1, agg2) -> {
        // var res = new MeasurementAggregator();
        // res.min = Math.min(agg1.min, agg2.min);
        // res.max = Math.max(agg1.max, agg2.max);
        // res.sum = agg1.sum + agg2.sum;
        // res.count = agg1.count + agg2.count;
        //
        // return res;
        // },
        // agg -> {
        // return new ResultRow(agg.min, (Math.round(agg.sum * 10.0) / 10.0) / agg.count, agg.max);
        // });
        // Read data

        Map<String, MeasurementAggregator> aggregates = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(FILE + ".txt", StandardCharsets.UTF_8), 8192 * 2)) {
            String key;
            String readLine;
            while ((readLine = br.readLine()) != null) {
                String[] strings = readLine.split(";");
                key = strings[0];
                var t = Double.parseDouble(strings[1]);
                MeasurementAggregator item = aggregates.get(key);
                if (item == null) {
                    var m = new MeasurementAggregator(t);
                    aggregates.put(key, m);
                }
                else {
                    item.setMin(t);
                    item.setMax(t);
                    item.addSum(t);
                    item.incrementCount();
                }
            }
        }

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

        Files.writeString(Paths.get(FILE + ".out"), measurements.toString());
    }

}
