package org.neo4j.tool;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.BloomFilter;
import org.rocksdb.BlockBasedTableConfig;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Disk-backed long-to-long map using RocksDB.
 * Replaces in-memory maps that cannot handle stores with ID spaces > Integer.MAX_VALUE.
 */
public class RocksDBIdMap implements Closeable {

    private final RocksDB db;
    private final Options options;
    private final File dbDir;

    static {
        RocksDB.loadLibrary();
    }

    public RocksDBIdMap(File parentDir) throws RocksDBException {
        dbDir = new File(parentDir, ".id-map-" + System.currentTimeMillis());
        if (!dbDir.mkdirs()) {
            throw new RocksDBException("Unable to create RocksDB directory: " + dbDir.getAbsolutePath());
        }

        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        tableConfig.setFilterPolicy(new BloomFilter(10, false));
        tableConfig.setBlockSize(16 * 1024);
        tableConfig.setBlockCacheSize(128 * 1024 * 1024);

        options = new Options();
        options.setCreateIfMissing(true);
        options.setWriteBufferSize(64 * 1024 * 1024);
        options.setMaxWriteBufferNumber(3);
        options.setTableFormatConfig(tableConfig);

        db = RocksDB.open(options, dbDir.getAbsolutePath());
    }

    public void put(long key, long value) {
        try {
            db.put(longToBytes(key), longToBytes(value));
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to put key=" + key + " value=" + value, e);
        }
    }

    public long getOrDefault(long key, long defaultValue) {
        try {
            byte[] val = db.get(longToBytes(key));
            return val == null ? defaultValue : bytesToLong(val);
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to get key=" + key, e);
        }
    }

    private static byte[] longToBytes(long value) {
        byte[] bytes = new byte[8];
        ByteBuffer.wrap(bytes).putLong(value);
        return bytes;
    }

    private static long bytesToLong(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getLong();
    }

    @Override
    public void close() throws IOException {
        db.close();
        options.close();
        deleteRecursively(dbDir);
    }

    private static void deleteRecursively(File file) {
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    deleteRecursively(child);
                }
            }
        }
        file.delete();
    }
}
