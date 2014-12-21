package edu.sjsu.cmpe.cache.client;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mashape.unirest.http.async.Callback;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

/**
 * Distributed cache service
 * 
 */
public class DistributedCacheService implements CacheServiceInterface {
    private final String cacheServerUrl;
    private CRDTClient client;
    private Map<Long, Boolean> writeSuccess = new ConcurrentHashMap<Long, Boolean>();
    private Map<Long, String> readSuccess = new ConcurrentHashMap<Long, String>();

    public DistributedCacheService(String serverUrl, CRDTClient client) {
        this.cacheServerUrl = serverUrl;
        this.client = client;
    }

    //Async GET
    /**
     * @see edu.sjsu.cmpe.cache.client.CacheServiceInterface#get(long)
     */
    @Override
    public void get(final long key) {
         Future<HttpResponse<JsonNode>> future = 
            Unirest.get(this.cacheServerUrl + "/cache/{key}")
                    .header("accept", "application/json")
                    .routeParam("key", Long.toString(key))
                    .asJsonAsync(new Callback<JsonNode>() {

            public void failed(UnirestException e) {
                System.out.println("The request has failed");
            }

            public void completed(HttpResponse<JsonNode> response) {
                 int code = response.getCode();
                 System.out.println("The request completed in ["+DistributedCacheService.this.cacheServerUrl+"] , response code is :" +code);
                 if(code == 200) {
                     String val = response.getBody().getObject().getString("value");
                     DistributedCacheService.this.readSuccess.put(key, val);
                     System.out.println("get for key["+key+"] = "+ val);
                 }
                 client.readRepair(key);
                //Map<String, String> headers = response.getHeaders();
                // JsonNode body = response.getBody();
                // InputStream rawBody = response.getRawBody();
            }

            public void cancelled() {
                System.out.println("The request has been cancelled");
            }
        });

    }
    
    // Synchronous GET
    /*
    @Override
    public String get(long key) {
        HttpResponse<JsonNode> response = null;
        try {
            response = Unirest.get(this.cacheServerUrl + "/cache/{key}")
                    .header("accept", "application/json")
                    .routeParam("key", Long.toString(key)).asJson();
        } catch (UnirestException e) {
            System.err.println(e);
        }

        return (response.getCode() == 204)? 
                null:response.getBody().getObject().getString("value");
    }
    */
   
    //Async PUT :
    @Override
     public void put(final long key, String value) {
        
         Future<HttpResponse<JsonNode>> response = 
                 Unirest.put(this.cacheServerUrl + "/cache/{key}/{value}")
                    .header("accept", "application/json")
                    .routeParam("key", Long.toString(key))
                    .routeParam("value", value)
                    .asJsonAsync(new Callback<JsonNode>() {

            public void failed(UnirestException e) {
                System.out.println("The request has failed");
            }

            public void completed(HttpResponse<JsonNode> response) {
                 int code = response.getCode();
                 System.out.println("The request completed in ["+DistributedCacheService.this.cacheServerUrl+"] , response code is :" +code);
                 writeSuccess.put(key, (code==200));
                 client.writeRepair(key);
                //Map<String, String> headers = response.getHeaders();
                // JsonNode body = response.getBody();
                // InputStream rawBody = response.getRawBody();
            }

            public void cancelled() {
                System.out.println("The request has been cancelled");
            }
        });            
    }

    @Override
    public void delete(long key) {
        HttpResponse<JsonNode> response = null;
        try {
            response = Unirest.delete(this.cacheServerUrl + "/cache/{key}")
                    .header("accept", "application/json")
                    .routeParam("key", Long.toString(key)).asJson();
        } catch (UnirestException e) {
            System.err.println(e);
        }
        if (response.getCode() != 200) {
            System.out.println("Failed to delete from the cache.");
        }
    }

    @Override
    public Map<Long, Boolean> getWriteSuccessMap() {
        return this.writeSuccess;
    }

    @Override
    public Map<Long, String> getReadSuccessMap() {
        return this.readSuccess;
    }
}
