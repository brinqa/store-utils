/*
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
import io.vavr.Tuple;
import java.util.List;
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
    return io.vavr.collection.List.ofAll(pairs)
        .groupBy(p -> toSize(p.getValue()))
        .map(t -> Tuple.of(t._1, t._2.map(Pair::getKey)))
        .toJavaStream()
        .flatMap(
            t -> {
              var sz = t._1;
              var l = t._2.toJavaList();
              var batchSize = toBatchSize(sz);
              return Lists.partition(l, batchSize).stream()
                  .map(b -> Bucket.builder().size(sz).indexes(b).build());
            });
  }

  /** Number of indexes of SMALL in a SMALL bucket for instance. */
  static int toBatchSize(Size size) {
    return switch (size) {
      case SMALL -> 1000;
      case MEDIUM -> 100;
      case LARGE -> largeBatchSize();
    };
  }

  /**
   * Number of indexes in a LARGE bucket. This is the number of processors divided by 4. This avoids
   * over powering the system in general.
   */
  static int largeBatchSize() {
    int ret = Runtime.getRuntime().availableProcessors() / 4;
    return Math.max(ret, 4);
  }

  static Size toSize(long total) {
    return (total < 50_000) ? Size.SMALL : (total < 1_000_000) ? Size.MEDIUM : Size.LARGE;
  }
}
