/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.sjsu.cmpe.cache.client;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Kokil Awasthi
 */
public class CRDTClient {

    ArrayList<CacheServiceInterface> servers = new ArrayList();
    Map<Long, Integer> writeKeyServerMap = new ConcurrentHashMap<Long, Integer>();
    Map<Long, Integer> readKeyServerMap = new ConcurrentHashMap<Long, Integer>();
    
    CRDTClient() {
        servers.add(new DistributedCacheService("http://localhost:3000", this));
        servers.add(new DistributedCacheService("http://localhost:3001", this));
        servers.add(new DistributedCacheService("http://localhost:3002", this));
    }
    
    public void get(long ln) {
        for (int i = 0; i < servers.size(); i++) {
            servers.get(i).get(ln);
        }
    }
    
    public void put(long ln, String val) {
        for (int i = 0; i < servers.size(); i++) {
            servers.get(i).put(ln,val);
        }
        System.out.println("put("+ln+" => "+val);
    }
    
    public synchronized void writeRepair(long key) {
        if(writeKeyServerMap.get(key)==null) {
            writeKeyServerMap.put(key,1);
        } else if(writeKeyServerMap.get(key).intValue()<2) {
            writeKeyServerMap.put(key, writeKeyServerMap.get(key).intValue()+1);
        } else { // response from all servers received
            CacheServiceInterface oneSuccess = null;
            for (int i = 0; i < servers.size(); i++) {
                if(servers.get(i).getWriteSuccessMap().get(key)) {// check success response
                    if(oneSuccess!=null) {// this means atleast two success
                        // re-initialize oneSuccess and break for loop as the write is success
                        oneSuccess = null;
                        break;
                    } else {
                        oneSuccess = servers.get(i);
                    }
                }
            }
            if (oneSuccess != null) { // only one success and two failed
                oneSuccess.delete(key);
                System.out.println("Repaired write for key="+key);
            }
            System.out.println("*** Cleaning all write maps for key="+key);
            cleanWriteMaps(key);
        }
        
    }

    public synchronized void readRepair(long key) {
        if(readKeyServerMap.get(key)==null) {
            readKeyServerMap.put(key,1);
        } else if(readKeyServerMap.get(key).intValue()<2) {
            readKeyServerMap.put(key, readKeyServerMap.get(key).intValue()+1);
        } else { // response from all servers received
            CacheServiceInterface oneFailed = null;
            String val1 = servers.get(0).getReadSuccessMap().get(key);
            String val2 = servers.get(1).getReadSuccessMap().get(key);
            String val3 = servers.get(2).getReadSuccessMap().get(key);
            
            // two nulls and one non-null. 
            if (val1 != null && val2 == null && val3== null) {
                servers.get(0).put(key, val2);
            } else if (val1 == null && val2 != null && val3== null) {
                servers.get(1).put(key, val3);
            }  else if (val1 == null && val2 == null && val3!= null) {
                servers.get(2).put(key, val2);
            } // one null and two equal non-null values
            else if (val1 == null && val2 != null && val3!= null && val2.equals(val3)) {
                servers.get(0).put(key, val3);
            } else if (val1 != null && val2 == null && val3!= null && val1.equals(val3)) {
                servers.get(1).put(key, val3);
            }  else if (val1 != null && val2 != null && val3== null && val1.equals(val2)) {
                servers.get(2).put(key, val2);
            } // no null and two equal values
            else if (!val1.equals(val2) && val2.equals(val3)) {
                servers.get(0).put(key, val3);
            } else if (!val1.equals(val2) && val1.equals(val3)) {
                servers.get(1).put(key, val3);
            }  else if (!val1.equals(val3) && val1.equals(val2)) {
                servers.get(2).put(key, val2);
            }

            System.out.println("*** Cleaning all read maps for key="+key);
            cleanReadMaps(key);
        }
    }

    private void cleanWriteMaps(long key) {
        writeKeyServerMap.remove(key);
        for (int i = 0; i < servers.size(); i++) {
            servers.get(i).getWriteSuccessMap().remove(key);
        }
    }

    private void cleanReadMaps(long key) {
        readKeyServerMap.remove(key);
        for (int i = 0; i < servers.size(); i++) {
            servers.get(i).getReadSuccessMap().remove(key);
        }
    }
}
