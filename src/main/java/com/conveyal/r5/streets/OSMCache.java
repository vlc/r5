package com.conveyal.r5.streets;

import com.conveyal.osmlib.OSM;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.io.File;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

/**
 * TODO this should be moved out of osm-lib and R5 into Analysis, and better integrated with TransportNetworkCache
 */
public class OSMCache {
    private final Function<String, File> getFile;

    public interface Config {
        String bundleBucket ();
    }

    /**
     * Construct a new OSMCache.
     */
    public OSMCache (Function<String, File> getFile) {
        this.getFile = getFile;
    }

    private Cache<String, OSM> osmCache = CacheBuilder.newBuilder()
            .maximumSize(10)
            .build();

    public String cleanId(String id) {
        return id.replaceAll("[^A-Za-z0-9]", "-");
    }

    public String getKey (String id) {
        return cleanId(id) + ".pbf";
    }

    public OSM get (String id) {
        try {
            return osmCache.get(id, () -> {
                File osmFile = getFile.apply(getKey(id));
                OSM ret = new OSM(null);
                ret.intersectionDetection = true;
                ret.readFromFile(osmFile.getAbsolutePath());
                return ret;
            });
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
