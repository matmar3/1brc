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

import sun.misc.Unsafe;

import java.lang.foreign.Arena;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.FormatProcessor.FMT;

public class CalculateAverage_matm {

    private static final Path MEASUREMENT_FILES = Path.of("./measurements.txt");
    private static final short SEMICOLON = 59;
    private static final short NEW_LINE = 10;

    private static final Unsafe UNSAFE = initUnsafe();

    private static Unsafe initUnsafe() {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            return (Unsafe) theUnsafe.get(Unsafe.class);
        }
        catch (Exception ex) {
            throw new RuntimeException();
        }
    }

    public static void main(String[] args) throws Exception {
        compute(MEASUREMENT_FILES);
    }

    public static void compute(Path path) throws Exception {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            var numberOfThreads = Runtime.getRuntime().availableProcessors();

            long startAddress = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), Arena.global()).address();
            long endAddress = startAddress + channel.size();

            List<Segment> segments = prepareSegments(startAddress, endAddress, numberOfThreads);

            var result = segments
                    .parallelStream()
                    .map(s -> processChunk(s.start(), s.end()))
                    .reduce(
                            new ConcurrentHashMap<Index, Temperature>(),
                            (mergedMap, map) -> {
                                map.forEach((key, value) -> mergedMap.merge(key, value, (t1, t2) -> new Temperature(
                                        t1.name,
                                        Math.min(t1.min, t2.min),
                                        Math.max(t1.max, t2.max),
                                        t1.count + t2.count,
                                        t1.sum + t2.sum)));
                                return mergedMap;
                            }, (m1, m2) -> {
                                m1.putAll(m2);
                                return m1;
                            });

            TreeMap<String, Temperature> treeResults = new TreeMap<>();
            for (var t : result.values()) {
                treeResults.put(t.getName(), t);
            }

            System.out.println(treeResults);
        }
    }

    private static List<Segment> prepareSegments(long start, long end, int numberOfThreads) {
        List<Segment> segments = new ArrayList<>(numberOfThreads);

        long chunkSize = (end - start) / numberOfThreads;
        if (chunkSize < 1000)
            chunkSize = end - start;

        long filePos, lastPosition;
        filePos = lastPosition = start;

        // seek the closest line end fop each thread
        for (int i = 0; i < numberOfThreads - 1; i++) {
            filePos += chunkSize;

            for (long position = 0; position < 64; position++) {
                if (filePos + position > end) {
                    break;
                }

                if (UNSAFE.getByte(filePos + position) == NEW_LINE) {
                    segments.add(new Segment(lastPosition, filePos + position));
                    lastPosition = filePos + position;
                    break;
                }
            }
        }
        segments.add(new Segment(lastPosition, end));

        return segments;
    }

    public static HashMap<Index, Temperature> processChunk(long startLocation, long endLocation) {
        HashMap<Index, Temperature> weathers = HashMap.newHashMap(500);

        long position = startLocation;
        byte currentByte;

        while (position < endLocation) {
            // process name
            int nameHash = 0;
            long cityStartPosition = position;
            while ((currentByte = UNSAFE.getByte(position)) != SEMICOLON) {
                nameHash = 31 * nameHash + currentByte;
                ++position;
            }
            var cityNameBytes = new byte[(int) (position - cityStartPosition)];
            UNSAFE.copyMemory(null, cityStartPosition, cityNameBytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, cityNameBytes.length);

            // process number
            long numberStartPosition = position + 1;
            while (UNSAFE.getByte(position) != NEW_LINE) {
                ++position;
            }
            var temperatureBytes = new byte[5];
            var temperatureOffset = 5 - (position - numberStartPosition);
            UNSAFE.copyMemory(null, numberStartPosition, temperatureBytes, Unsafe.ARRAY_BYTE_BASE_OFFSET + temperatureOffset, temperatureBytes.length);

            // save process values
            var cityIdx = new Index(nameHash);
            var actualValue = weathers.get(cityIdx);
            weathers.put(cityIdx, computeIfAbsent(actualValue, cityNameBytes, bytesToInt(temperatureBytes)));

            ++position;
        }

        return weathers;
    }

    public static Temperature computeIfAbsent(Temperature actualValue, byte[] cityName, int temperature) {
        if (actualValue == null) {
            var temp = new Temperature(cityName);
            temp.compute(temperature);
            return temp;
        }
        else {
            actualValue.compute(temperature);
            return actualValue;
        }
    }

    record Segment(long start, long end) {
    }

    public static class Index {
        private final int hash;

        public Index(int hash) {
            this.hash = hash;
        }

        @Override
        public boolean equals(final Object o) {
            return true;
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    public static int bytesToInt(byte[] bytes) {
        // for positive x.x numbers
        // 01234
        // 1.1
        if (bytes[0] == 0 && bytes[1] == 0) {
            return 10 * (bytes[2] - '0') + (bytes[4] - '0');
        }
        // for negative x.x numbers
        // 01234
        // 1.1
        else if (bytes[0] == 0 && bytes[1] == 45) {
            return -(10 * (bytes[2] - '0') + (bytes[4] - '0'));
        }
        // for positive xx.x numbers
        // 01234
        // 31.1
        else if (bytes[0] == 0) {
            return 100 * (bytes[1] - '0') + 10 * (bytes[2] - '0') + (bytes[4] - '0');

        }
        // for negative xx.x numbers
        // 01234
        // -31.1
        else {
            return -(100 * (bytes[1] - '0') + 10 * (bytes[2] - '0') + (bytes[4] - '0'));
        }
    }

    public static class Temperature {
        byte[] name;
        int min;
        int max;
        long count;
        long sum;

        public Temperature(byte[] name, int min, int max, long count, long sum) {
            this.name = name;
            this.min = min;
            this.max = max;
            this.count = count;
            this.sum = sum;
        }

        public Temperature(byte[] name) {
            this.name = name;
            this.max = Integer.MIN_VALUE;
            this.min = Integer.MAX_VALUE;
            this.sum = 0L;
            this.count = 0L;
        }

        public void compute(int temperature) {
            this.min = Math.min(this.min, temperature);
            this.max = Math.max(this.max, temperature);
            this.sum += temperature;
            this.count += 1;
        }

        public String getName() {
            return new String(name, StandardCharsets.UTF_8).trim();
        }

        @Override
        public String toString() {
            return FMT."%.1f\{((double) min) / 10.0}/%.1f\{(((double) sum) / 10.0 / count)}/%.1f\{((double) max) / 10.0}";
        }
    }

}
