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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ManifestTest {

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void writeThenRead() throws Exception {
    final File dir = tmp.newFolder("dump");
    final var manifest =
        Manifest.builder()
            .toolVersion("1.2.3")
            .neo4jVersion("5.26.0")
            .startedAt("2020-01-01T00:00:00Z")
            .finishedAt("2020-01-01T01:00:00Z")
            .status("COMPLETE")
            .labels(List.of("A", "B"))
            .relationshipTypes(List.of("KNOWS"))
            .nodeCount(10)
            .relationshipCount(5)
            .batchSize(1000)
            .formatVersion(Manifest.FORMAT_VERSION)
            .idPropertyPresent(true)
            .importIdProperty(Manifest.IMPORT_ID)
            .importLabel(Manifest.IMPORT_LABEL)
            .build();

    manifest.write(dir);
    assertTrue(new File(dir, "manifest.json").isFile());

    final var read = Manifest.read(dir);
    assertEquals(manifest, read);
    assertEquals("COMPLETE", read.getStatus());
    assertEquals(10, read.getNodeCount());
    assertEquals(List.of("A", "B"), read.getLabels());
  }
}
