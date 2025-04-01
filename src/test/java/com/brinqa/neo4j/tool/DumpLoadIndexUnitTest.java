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
package com.brinqa.neo4j.tool;

import static com.brinqa.neo4j.tool.util.Print.println;

import com.brinqa.neo4j.tool.dto.IndexData;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.util.List;
import org.junit.Test;

/** Attempt to recreate all the indexes */
public class DumpLoadIndexUnitTest {

  @Test
  public void indexLoad() throws Exception {
    final var mapper = new ObjectMapper();
    final var f = new File("dump-index.json");
    final var typedef = new TypeReference<List<IndexData>>() {};
    final var indexes = mapper.readValue(f, typedef);

    for (final var index : indexes) {
      if (index.getOwningConstraint() != null) {
        println("index: " + index.getOwningConstraint());
      }
    }
  }
}
