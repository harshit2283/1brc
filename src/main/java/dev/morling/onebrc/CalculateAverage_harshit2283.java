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
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.DoubleStream;
import java.util.DoubleSummaryStatistics;

public class CalculateAverage_harshit2283 {

    public static void main(String[] args) throws IOException, InterruptedException {
        String filename = "./measurements.txt"; // Replace with actual filename

        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            Map<String, double[]> stationData = processFileInParallel(reader);
            printResults(stationData);
        }
    }

    private static Map<String, double[]> processFileInParallel(BufferedReader reader) throws InterruptedException { // Add throws declaration
        var stationData = new ConcurrentHashMap<String, double[]>();
        var lineCounter = new AtomicInteger();

        int numThreads = Math.min(Runtime.getRuntime().availableProcessors(), 4);

        Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                String line;
                while ((line = reader.lines().skip(lineCounter.getAndIncrement()).findFirst().orElse(null)) != null) {
                    processLine(line, stationData);
                }
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        return stationData;
    }

    private static void processLine(String line, Map<String, double[]> stationData) {
        String[] parts = line.split(";");
        String stationName = parts[0];
        double temperature = Double.parseDouble(parts[1]);

        stationData.computeIfAbsent(stationName, k -> new double[1])[0] += temperature;
    }

    private static void printResults(Map<String, double[]> stationData) {
        TreeMap<String, double[]> sortedData = new TreeMap<>(stationData);
        for (Map.Entry<String, double[]> entry : sortedData.entrySet()) {
            double[] temperatures = entry.getValue();
            DoubleSummaryStatistics stats = DoubleStream.of(temperatures).summaryStatistics();
            System.out.printf("%s=%.1f/%.1f/%.1f%n", entry.getKey(), stats.getMin(), stats.getAverage(), stats.getMax());
        }
    }
}
