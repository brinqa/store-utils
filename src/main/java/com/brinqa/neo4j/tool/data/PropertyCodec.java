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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.neo4j.driver.Values;
import org.neo4j.driver.types.IsoDuration;
import org.neo4j.driver.types.Point;

/**
 * Converts property values returned by the Neo4j Java Driver into a small, Kryo-friendly canonical
 * form made only of String/Boolean/Long/Double/byte[]/List/Map, and back again into values the
 * driver accepts as query parameters.
 *
 * <p>Temporal values, durations and points cannot be reflected over safely on JDK 17 (java.base is
 * not open) so they are represented as tagged maps ({@code {"__t__": "<tag>", ...}}) instead of
 * being serialized field-by-field. {@code denormalize} rebuilds the matching driver type so the
 * property keeps its original Neo4j type on import.
 */
final class PropertyCodec {

  /** Marker key used to tag temporal/spatial values inside a plain map. */
  static final String TAG = "__t__";

  private PropertyCodec() {}

  /** Convert a driver-side value into a serialization-safe value. */
  static Object normalize(Object v) {
    if (v == null || v instanceof String || v instanceof Boolean || v instanceof byte[]) {
      return v;
    }
    if (v instanceof Long || v instanceof Double) {
      return v;
    }
    if (v instanceof Integer || v instanceof Short || v instanceof Byte) {
      return ((Number) v).longValue();
    }
    if (v instanceof Float) {
      return ((Number) v).doubleValue();
    }
    if (v instanceof List<?> list) {
      final var out = new ArrayList<>(list.size());
      for (Object e : list) {
        out.add(normalize(e));
      }
      return out;
    }
    if (v instanceof Map<?, ?> map) {
      final var out = new LinkedHashMap<String, Object>();
      for (Map.Entry<?, ?> e : map.entrySet()) {
        out.put(String.valueOf(e.getKey()), normalize(e.getValue()));
      }
      return out;
    }
    if (v instanceof LocalDate d) {
      return tagged("date", d.toString());
    }
    if (v instanceof LocalTime t) {
      return tagged("localtime", t.toString());
    }
    if (v instanceof OffsetTime t) {
      return tagged("time", t.toString());
    }
    if (v instanceof LocalDateTime dt) {
      return tagged("localdatetime", dt.toString());
    }
    if (v instanceof ZonedDateTime dt) {
      return tagged("datetime", dt.toString());
    }
    if (v instanceof OffsetDateTime dt) {
      return tagged("datetime", dt.toString());
    }
    if (v instanceof IsoDuration d) {
      final var m = new LinkedHashMap<String, Object>();
      m.put(TAG, "duration");
      m.put("months", d.months());
      m.put("days", d.days());
      m.put("seconds", d.seconds());
      m.put("nanoseconds", (long) d.nanoseconds());
      return m;
    }
    if (v instanceof Point p) {
      final var m = new LinkedHashMap<String, Object>();
      m.put(TAG, "point");
      m.put("srid", (long) p.srid());
      m.put("x", p.x());
      m.put("y", p.y());
      if (!Double.isNaN(p.z())) {
        m.put("z", p.z());
      }
      return m;
    }
    throw new IllegalArgumentException("Unsupported property type: " + v.getClass().getName());
  }

  /** Convert a normalized value back into something the driver accepts as a parameter. */
  static Object denormalize(Object v) {
    if (v instanceof List<?> list) {
      final var out = new ArrayList<>(list.size());
      for (Object e : list) {
        out.add(denormalize(e));
      }
      return out;
    }
    if (v instanceof Map<?, ?> map) {
      final Object tag = map.get(TAG);
      if (tag == null) {
        final var out = new LinkedHashMap<String, Object>();
        for (Map.Entry<?, ?> e : map.entrySet()) {
          out.put(String.valueOf(e.getKey()), denormalize(e.getValue()));
        }
        return out;
      }
      return rebuild(String.valueOf(tag), map);
    }
    return v;
  }

  private static Object rebuild(String tag, Map<?, ?> m) {
    return switch (tag) {
      case "date" -> LocalDate.parse((String) m.get("v"));
      case "localtime" -> LocalTime.parse((String) m.get("v"));
      case "time" -> OffsetTime.parse((String) m.get("v"));
      case "localdatetime" -> LocalDateTime.parse((String) m.get("v"));
      case "datetime" -> ZonedDateTime.parse((String) m.get("v"));
      case "duration" ->
          Values.isoDuration(
              asLong(m.get("months")),
              asLong(m.get("days")),
              asLong(m.get("seconds")),
              (int) asLong(m.get("nanoseconds")));
      case "point" -> {
        final int srid = (int) asLong(m.get("srid"));
        final double x = (double) m.get("x");
        final double y = (double) m.get("y");
        yield m.containsKey("z")
            ? Values.point(srid, x, y, (double) m.get("z"))
            : Values.point(srid, x, y);
      }
      default -> throw new IllegalStateException("Unknown tagged value: " + tag);
    };
  }

  private static long asLong(Object o) {
    return ((Number) o).longValue();
  }

  private static Map<String, Object> tagged(String tag, String value) {
    final var m = new LinkedHashMap<String, Object>();
    m.put(TAG, tag);
    m.put("v", value);
    return m;
  }
}
