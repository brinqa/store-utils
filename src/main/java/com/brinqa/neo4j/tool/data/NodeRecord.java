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

import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * A single exported node. {@code key} is the export-local identity (the source element id, only
 * meaningful within one export) used to wire up relationships on import. {@code props} are stored
 * in normalized form (see {@link PropertyCodec}).
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class NodeRecord {
  private String key;
  private List<String> labels;
  private Map<String, Object> props;
}
