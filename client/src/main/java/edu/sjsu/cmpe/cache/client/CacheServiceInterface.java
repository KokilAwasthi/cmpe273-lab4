package edu.sjsu.cmpe.cache.client;

import java.util.Map;

/**
 * Cache Service Interface
 * 
 */
public interface CacheServiceInterface {
    public void get(long key);

    public void put(long key, String value);
    
    public void delete(long key);
    
    public Map<Long, Boolean> getWriteSuccessMap();
    public Map<Long, String> getReadSuccessMap();
}
