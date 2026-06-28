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
package com.brinqa.neo4j.tool.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class StagingStoreTest {

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void nodeRoundTripAcrossPartitions() throws Exception {
    final File root = tmp.newFolder("dump");
    final var store = new StagingStore(root);
    store.init();

    final var n1 = new NodeRecord("k1", List.of("Person", "Admin"), props("name", "alice"));
    final var n2 = new NodeRecord("k2", List.of("Person"), props("name", "bob"));
    final var n3 = new NodeRecord("k3", List.of(), props("name", "ghost"));

    try (var writer = store.newNodeWriter()) {
      writer.add(n1);
      writer.add(n2);
      writer.add(n3);
    }

    final var read = new ArrayList<NodeRecord>();
    store.forEachNode(read::add);

    assertEquals(3, read.size());
    // order is per-partition, so compare as a set keyed by node key
    final var byKey = new LinkedHashMap<String, NodeRecord>();
    read.forEach(r -> byKey.put(r.getKey(), r));
    assertEquals(new NodeRecord("k1", List.of("Admin", "Person"), n1.getProps()), byKey.get("k1"));
    assertEquals(n2, byKey.get("k2"));
    assertEquals(n3, byKey.get("k3"));
  }

  @Test
  public void relRoundTrip() throws Exception {
    final File root = tmp.newFolder("dump");
    final var store = new StagingStore(root);
    store.init();

    final var r1 = new RelRecord("k1", "k2", "KNOWS", props("since", 2020L));
    final var r2 = new RelRecord("k2", "k1", "LIKES", Map.of());

    try (var writer = store.newRelWriter()) {
      writer.add(r1);
      writer.add(r2);
    }

    final var read = new ArrayList<RelRecord>();
    store.forEachRel(read::add);
    assertEquals(2, read.size());
  }

  @Test
  public void partitionNaming() {
    assertEquals(StagingStore.NO_LABEL_PARTITION, StagingStore.partitionFor(List.of()));
    // label order does not matter
    assertEquals(
        StagingStore.partitionFor(List.of("A", "B")), StagingStore.partitionFor(List.of("B", "A")));
    // different label sets land in different partitions
    assertNotEquals(
        StagingStore.partitionFor(List.of("A")), StagingStore.partitionFor(List.of("A", "B")));
  }

  @Test
  public void nodeLabelsUseCanonicalOrderForBatching() throws Exception {
    final File root = tmp.newFolder("dump");
    final var store = new StagingStore(root);
    store.init();

    try (var writer = store.newNodeWriter()) {
      writer.add(new NodeRecord("k1", List.of("B", "A"), Map.of()));
      writer.add(new NodeRecord("k2", List.of("A", "B"), Map.of()));
    }

    final var read = new ArrayList<NodeRecord>();
    store.forEachNode(read::add);

    assertEquals(List.of("A", "B"), read.get(0).getLabels());
    assertEquals(List.of("A", "B"), read.get(1).getLabels());
  }

  private static Map<String, Object> props(String k, Object v) {
    final var m = new LinkedHashMap<String, Object>();
    m.put(k, v);
    return m;
  }
}
