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
package com.brinqa.neo4j.tool.util;

import java.util.List;
import java.util.stream.Collectors;

/** Safe quoting of Cypher identifiers (labels, relationship types, index/property names). */
public final class CypherNames {

  private CypherNames() {}

  /**
   * Backtick-quote a single identifier, doubling any embedded backticks. e.g. {@code a`b} becomes
   * {@code `a``b`}.
   */
  public static String quote(String identifier) {
    return "`" + identifier.replace("`", "``") + "`";
  }

  /** Render a list of labels as a quoted Cypher label chain, e.g. {@code :`A`:`B`}. */
  public static String labels(List<String> labels) {
    return labels.stream().map(l -> ":" + quote(l)).collect(Collectors.joining());
  }
}
