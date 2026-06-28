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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/** Description of an export, written to {@code manifest.json}. */
@Value
@Jacksonized
@Builder(toBuilder = true)
public class Manifest {

  /** Bumped when the on-disk record/serialization format changes incompatibly. */
  public static final int FORMAT_VERSION = 1;

  /** Property used to carry export-local node identity during import; removed afterwards. */
  public static final String IMPORT_ID = "__import_id";

  /** Temporary label that lets relationship import look up endpoints by import id. */
  public static final String IMPORT_LABEL = "__Imported";

  String toolVersion;
  String neo4jVersion;
  String startedAt;
  String finishedAt;
  String status; // IN_PROGRESS | COMPLETE
  List<String> labels;
  List<String> relationshipTypes;
  long nodeCount;
  long relationshipCount;
  int batchSize;
  int formatVersion;
  boolean idPropertyPresent;
  String importIdProperty;
  String importLabel;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public void write(File dir) {
    try {
      MAPPER.writerWithDefaultPrettyPrinter().writeValue(new File(dir, "manifest.json"), this);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static Manifest read(File dir) {
    try {
      return MAPPER.readValue(new File(dir, "manifest.json"), Manifest.class);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
