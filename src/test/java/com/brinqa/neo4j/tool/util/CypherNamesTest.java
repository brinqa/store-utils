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

import static org.junit.Assert.assertEquals;

import java.util.List;
import org.junit.Test;

public class CypherNamesTest {

  @Test
  public void quotesSimpleNames() {
    assertEquals("`Person`", CypherNames.quote("Person"));
  }

  @Test
  public void escapesEmbeddedBackticks() {
    assertEquals("`a``b`", CypherNames.quote("a`b"));
  }

  @Test
  public void quotesInjectionAttempt() {
    // A label that tries to break out stays inside one quoted identifier: the embedded backtick is
    // doubled so it cannot terminate the quote early.
    assertEquals("`A``) DELETE n //`", CypherNames.quote("A`) DELETE n //"));
  }

  @Test
  public void buildsLabelChain() {
    assertEquals(":`A`:`B`", CypherNames.labels(List.of("A", "B")));
  }

  @Test
  public void emptyLabelChain() {
    assertEquals("", CypherNames.labels(List.of()));
  }
}
