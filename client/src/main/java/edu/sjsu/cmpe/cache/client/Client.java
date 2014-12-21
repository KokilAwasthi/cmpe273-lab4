package edu.sjsu.cmpe.cache.client;
import java.util.*;

public class Client {

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Cache Client...");
        CRDTClient crdtClient = new CRDTClient();
        crdtClient.put(1, "a");
        System.out.println("Finished step 1");
        Thread.sleep(30000);

        crdtClient.put(1, "b");
        System.out.println("Finished step 2");
        Thread.sleep(30000);
        
        crdtClient.get(1);
        System.out.println("Finished step 3");
    }

    
}
