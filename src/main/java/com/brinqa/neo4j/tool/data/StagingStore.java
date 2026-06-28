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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Disk-backed staging for an export. Nodes and relationships are written as length-prefixed Kryo
 * records into one file per partition (node files are partitioned by label set, relationship files
 * by type). Writing keeps only one buffered stream open per partition and reading streams records
 * back one at a time, so memory stays bounded regardless of graph size.
 *
 * <p>Layout:
 *
 * <pre>
 *   &lt;root&gt;/nodes/&lt;safe-label-set&gt;.kryo
 *   &lt;root&gt;/relationships/&lt;safe-type&gt;.kryo
 * </pre>
 *
 * <p>A flat append log is used rather than an embedded key/value store (RocksDB/speedb): export
 * then import is a pure sequential write-then-scan, so the random-access and compaction features of
 * a KV store add native-dependency risk for no benefit here. ponytail: flat log, swap in RocksDB
 * only if random lookups during staging are ever needed.
 */
public final class StagingStore {

  static final String NO_LABEL_PARTITION = "_NO_LABEL";

  private final File nodesDir;
  private final File relsDir;

  public StagingStore(File root) {
    this.nodesDir = new File(root, "nodes");
    this.relsDir = new File(root, "relationships");
  }

  public void init() {
    mkdirs(nodesDir);
    mkdirs(relsDir);
  }

  public NodeWriter newNodeWriter() {
    return new NodeWriter();
  }

  public RelWriter newRelWriter() {
    return new RelWriter();
  }

  public void forEachNode(Consumer<NodeRecord> consumer) {
    final var codec = new KryoCodec();
    for (File f : listKryo(nodesDir)) {
      readRecords(f, bytes -> consumer.accept(codec.decodeNode(bytes)));
    }
  }

  public void forEachRel(Consumer<RelRecord> consumer) {
    final var codec = new KryoCodec();
    for (File f : listKryo(relsDir)) {
      readRecords(f, bytes -> consumer.accept(codec.decodeRel(bytes)));
    }
  }

  public List<File> nodePartitions() {
    return listKryo(nodesDir);
  }

  public List<File> relPartitions() {
    return listKryo(relsDir);
  }

  public void forEachNode(File partition, Consumer<NodeRecord> consumer) {
    final var codec = new KryoCodec();
    readRecords(partition, bytes -> consumer.accept(codec.decodeNode(bytes)));
  }

  public void forEachRel(File partition, Consumer<RelRecord> consumer) {
    final var codec = new KryoCodec();
    readRecords(partition, bytes -> consumer.accept(codec.decodeRel(bytes)));
  }

  /** Append node records, partitioned by their (sorted) label set. */
  public final class NodeWriter implements Closeable {
    private final KryoCodec codec = new KryoCodec();
    private final Map<String, DataOutputStream> streams = new HashMap<>();

    public void add(NodeRecord record) {
      final var labels = sortedLabels(record.getLabels());
      write(
          stream(nodesDir, streams, partitionFor(labels)),
          codec.encodeNode(new NodeRecord(record.getKey(), labels, record.getProps())));
    }

    @Override
    public void close() {
      closeAll(streams);
    }
  }

  /** Append relationship records, partitioned by type. */
  public final class RelWriter implements Closeable {
    private final KryoCodec codec = new KryoCodec();
    private final Map<String, DataOutputStream> streams = new HashMap<>();

    public void add(RelRecord record) {
      write(stream(relsDir, streams, safe(record.getType())), codec.encodeRel(record));
    }

    @Override
    public void close() {
      closeAll(streams);
    }
  }

  /** Partition name for a label set: sorted labels joined, sanitized, hash-suffixed. */
  static String partitionFor(List<String> labels) {
    if (labels == null || labels.isEmpty()) {
      return NO_LABEL_PARTITION;
    }
    final var sorted = new ArrayList<>(labels);
    sorted.sort(String::compareTo);
    return safe(String.join("__", sorted));
  }

  private static List<String> sortedLabels(List<String> labels) {
    if (labels == null || labels.isEmpty()) {
      return List.of();
    }
    final var sorted = new ArrayList<>(labels);
    sorted.sort(String::compareTo);
    return sorted;
  }

  /** Make a string safe to use as a file name while staying collision-resistant. */
  static String safe(String raw) {
    final var sanitized = raw.replaceAll("[^A-Za-z0-9_-]", "_");
    final var clipped = sanitized.length() > 100 ? sanitized.substring(0, 100) : sanitized;
    return clipped + "_" + Integer.toHexString(raw.hashCode());
  }

  private static DataOutputStream stream(
      File dir, Map<String, DataOutputStream> streams, String p) {
    return streams.computeIfAbsent(
        p,
        name -> {
          try {
            final var f = new File(dir, name + ".kryo");
            return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(f)));
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        });
  }

  private static void write(DataOutputStream out, byte[] bytes) {
    try {
      out.writeInt(bytes.length);
      out.write(bytes);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void closeAll(Map<String, DataOutputStream> streams) {
    for (DataOutputStream out : streams.values()) {
      try {
        out.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  private static void readRecords(File f, Consumer<byte[]> consumer) {
    try (var in = new DataInputStream(new BufferedInputStream(new FileInputStream(f)))) {
      while (true) {
        final int len;
        try {
          len = in.readInt();
        } catch (EOFException eof) {
          return;
        }
        final var bytes = new byte[len];
        in.readFully(bytes);
        consumer.accept(bytes);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static List<File> listKryo(File dir) {
    final var files = dir.listFiles((d, n) -> n.endsWith(".kryo"));
    if (files == null) {
      return List.of();
    }
    final var sorted = new ArrayList<>(List.of(files));
    sorted.sort(Comparator.comparing(File::getName));
    return sorted;
  }

  private static void mkdirs(File dir) {
    if (!dir.isDirectory() && !dir.mkdirs()) {
      throw new UncheckedIOException(new IOException("Unable to create directory: " + dir));
    }
  }
}
