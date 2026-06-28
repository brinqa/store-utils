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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;

public class PropertyCodecTest {

  private static Object roundTrip(Object in) {
    return PropertyCodec.denormalize(PropertyCodec.normalize(in));
  }

  @Test
  public void scalars() {
    assertNull(roundTrip(null));
    assertEquals("hello", roundTrip("hello"));
    assertEquals(Boolean.TRUE, roundTrip(true));
    assertEquals(42L, roundTrip(42L));
    assertEquals(3.14, roundTrip(3.14));
  }

  @Test
  public void integersWidenToLong() {
    assertEquals(7L, roundTrip(7));
    assertEquals(2.5, roundTrip(2.5f));
  }

  @Test
  public void byteArray() {
    final var bytes = new byte[] {1, 2, 3, 4};
    assertArrayEquals(bytes, (byte[]) roundTrip(bytes));
  }

  @Test
  public void lists() {
    assertEquals(List.of(1L, 2L, 3L), roundTrip(List.of(1L, 2L, 3L)));
    assertEquals(List.of("a", "b"), roundTrip(List.of("a", "b")));
  }

  @Test
  public void nestedMap() {
    final var m = new LinkedHashMap<String, Object>();
    m.put("a", 1L);
    m.put("b", List.of("x"));
    assertEquals(m, roundTrip(m));
  }

  @Test
  public void temporal() {
    final var date = LocalDate.of(2020, 1, 2);
    assertEquals(date, roundTrip(date));

    final var dt = ZonedDateTime.of(2020, 1, 2, 3, 4, 5, 0, ZoneOffset.UTC);
    assertEquals(dt, roundTrip(dt));
  }

  @Test
  public void duration() {
    final var input = Values.isoDuration(1, 2, 3, 4).asIsoDuration();
    final var out = ((Value) roundTrip(input)).asIsoDuration();
    assertEquals(1, out.months());
    assertEquals(2, out.days());
    assertEquals(3, out.seconds());
    assertEquals(4, out.nanoseconds());
  }

  @Test
  public void point() {
    final var input = Values.point(7203, 1.5, 2.5).asPoint();
    final var out = ((Value) roundTrip(input)).asPoint();
    assertEquals(7203, out.srid());
    assertEquals(1.5, out.x(), 0.0);
    assertEquals(2.5, out.y(), 0.0);
  }

  @Test
  public void normalizeOnlyProducesPlainTypes() {
    // Temporal values become tagged plain maps so Kryo never reflects into java.base.
    final Object n = PropertyCodec.normalize(LocalDate.of(2020, 1, 2));
    assertEquals("date", ((Map<?, ?>) n).get(PropertyCodec.TAG));
  }
}
