package com.conveyal.r5.analyst;

import com.conveyal.file.FileStorageFormat;
import com.conveyal.file.FileUtils;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;

/**
 * A local in-memory cache for PointSets, which are loaded from persistent storage on S3.
 * It will load either gridded or freeform pointsets, depending on the file extension of the S3 key.
 * Each instance of PointSetCache reads from a single S3 bucket specified at construction.
 */
public class PointSetCache {
    private static final Logger LOG = LoggerFactory.getLogger(PointSetCache.class);

    /** How large the cache should be. Should be large enough to fit all field of a project */
    private static final int CACHE_SIZE = 200;

    private final Function<String, File> getFile;

    private LoadingCache<String, PointSet> cache = CacheBuilder.newBuilder()
                .maximumSize(CACHE_SIZE)
                // Lambda functions cannot be used here because CacheLoader has multiple overrideable methods.
                .build(new CacheLoader<String, PointSet>() {
                    @Override
                    public PointSet load(String s) throws Exception {
                        return loadPointSet(s);
                    }
                });

    public PointSetCache(Function<String, File> getFile) {
        this.getFile = getFile;
    }

    private PointSet loadPointSet(String key) throws IOException {
        File file = getFile.apply(key);
        // If the object does not exist on S3, getObject will throw an exception which will be caught in the
        // PointSetCache.get method. Grids are gzipped on S3.
        InputStream is = new GZIPInputStream(FileUtils.getInputStream(file));
        if (key.endsWith(FileStorageFormat.GRID.extension)) {
            return Grid.read(is);
        } else if (key.endsWith(FileStorageFormat.FREEFORM.extension)) {
            return new FreeFormPointSet(is);
        } else {
            throw new RuntimeException("Unrecognized file extension in object key: " + key);
        }
    }

    public PointSet get (String key) {
        try {
            return cache.get(key);
        } catch (ExecutionException e) {
            LOG.error("Error retrieving destinationPointSetId {}", key, e);
            throw new RuntimeException(e);
        }
    }
}
