/*
 * Copyright 2025 Brinqa, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.brinqa.neo4j.tool.index;

import com.brinqa.neo4j.tool.dto.Bucket;
import com.brinqa.neo4j.tool.dto.Bucket.Size;
import com.brinqa.neo4j.tool.dto.IndexData;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;

public class BucketBuilder {

  /**
   * Create a stream of {@link Bucket} by partitioning the list. If the index supports a LARGE then
   * there can only be 4 indexes in a LARGE bucket.
   *
   * @param pairs a list of index to size of node label.
   * @return stream of buckets partition with S/M/L.
   */
  public static Stream<Bucket> build(List<Pair<IndexData, Long>> pairs) {
    final Map<Size, ArrayList<IndexData>> map = new HashMap<>();
    for (Pair<IndexData, Long> pair : pairs) {
      final var size = toSize(pair.getValue());
      final var l = map.computeIfAbsent(size, na -> new ArrayList<>());
      l.add(pair.getKey());
    }

    // break up into buckets
    return map.entrySet().stream().flatMap(e -> toBuckets(e.getKey(), e.getValue()));
  }

  /**
   * Create a stream of {@link Bucket} by partitioning the list. If the index supports a LARGE then
   * there can only be 4 indexes in a LARGE bucket.
   *
   * @param size bucket size S/M/L
   * @param values list of indexes
   * @return stream of buckets partition with S/M/L.
   */
  static Stream<Bucket> toBuckets(Bucket.Size size, List<IndexData> values) {
    return Lists.partition(values, toBatchSize(size)).stream()
        .map(b -> Bucket.builder().size(size).indexes(values).build());
  }

  static int toBatchSize(Size size) {
    switch (size) {
      case SMALL:
        return 200;
      case MEDIUM:
        return 50;
      case LARGE:
        return 4;
    }
    throw new IllegalArgumentException("Unsupported batch size: " + size);
  }

  static Size toSize(long total) {
    return (total < 50_000) ? Size.SMALL : (total < 1_000_000) ? Size.MEDIUM : Size.LARGE;
  }
}
