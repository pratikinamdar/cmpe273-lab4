package edu.sjsu.cmpe.cache.client;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.mashape.unirest.http.Headers;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mashape.unirest.request.HttpRequestWithBody;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * @author Pratik Inamdar
 *
 */
public class DistributedCacheService implements CacheServiceInterface {
    private  String cacheServerUrl;
    private  String[] serverUrls;
    private AtomicInteger successReadCount;
    private AtomicInteger successWriteCount;
    int numOfServer;
    public DistributedCacheService(String serverUrl) {
        this.cacheServerUrl = serverUrl;
    }

    // Constructor
    public DistributedCacheService(String...serverUrls){
        this.serverUrls=serverUrls;
        this.numOfServer = serverUrls.length;
    }
    /**
     * @see edu.sjsu.cmpe.cache.client.CacheServiceInterface#get(long)
     */



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
        String value = response.getBody().getObject().getString("value");

        return value;
    }


    //Asynchronous read
    @Override
    public String getAsynch(long key) {

        final CountDownLatch counter = new CountDownLatch(numOfServer);
        HttpResponse<JsonNode> response = null;
      successReadCount = new AtomicInteger(0);
        final ListMultimap<String,String> successfulServers = ArrayListMultimap.create();

        try {
            for (int i = 0; i < numOfServer; i++) {

                final String currentServerURL = this.serverUrls[i];

                Future<HttpResponse<JsonNode>> future = Unirest.get(currentServerURL + "/cache/{key}")
                        .header("accept", "application/json")
                        .routeParam("key", Long.toString(key)).asJsonAsync(new Callback<JsonNode>() {
                            String currentUrl = currentServerURL;

                            public void failed(UnirestException e) {
                                System.out.println("The request not completed");
                            }

                            public void completed(HttpResponse<JsonNode> response) {
                                int code = response.getCode();
                                Headers headers = response.getHeaders();
                                JsonNode body = response.getBody();
                                InputStream rawBody = response.getRawBody();
System.out.println(response.getBody());
                                String value;
                                if (response.getBody()!=null)
                                    value = response.getBody().getObject().getString("value");
                                    else
                                        value="fault";
                                    successReadCount.incrementAndGet();
                                    successfulServers.put(value, currentUrl);
                                    counter.countDown();
                                    ;


                            }

                            public void cancelled() {
                                System.out.println("The request has been cancelled");
                            }

                        });


            }
            counter.await();
        }
        catch (InterruptedException e){
            e.printStackTrace();
        }



        List<String> sameValueServers = new ArrayList<String>();
        String successValue=null;

        for (String value : successfulServers.keySet()) {

            List<String> receivedServerUrls = successfulServers.get(value);
            if(receivedServerUrls!=null) {
                if (receivedServerUrls.size() > sameValueServers.size()) {
                    sameValueServers = receivedServerUrls;
                    successValue = value;
                }
            }
}

       // String value = response.getBody().getObject().getString("value");
        //Sending correct values to other

        List<String> faultServers = new ArrayList<String>();
        for(String srvUrl:serverUrls){
            int i=0;
            for(;i<sameValueServers.size();i++){
                if(srvUrl.equalsIgnoreCase(sameValueServers.get(i)))
                    break;
            }
            if(i>=sameValueServers.size()){
                faultServers.add(srvUrl);

            }
        }
System.out.println("Successfully received from servers...");
        for (String srvr:sameValueServers){
            System.out.println(srvr);
        }
        System.out.println("Values repaired on servers...");
        for (String srvr:faultServers){
            System.out.println(srvr);
            this.cacheServerUrl=srvr;
            put(key,successValue);
        }
     //   return value;
        return successValue;
    }

    /**
     * @see edu.sjsu.cmpe.cache.client.CacheServiceInterface#put(long,
     *      java.lang.String)
     */

    // Synchronous write start
    @Override
    public void put(long key, String value) {
        HttpResponse<JsonNode> response = null;
        try {
            response = Unirest
                    .put(this.cacheServerUrl + "/cache/{key}/{value}")
                    .header("accept", "application/json")
                    .routeParam("key", Long.toString(key))
                    .routeParam("value", value).asJson();
        } catch (UnirestException e) {
            System.err.println(e);
        }

        if (response.getCode() != 200) {
            System.out.println("Loading to caching failed.");
        }
    }
// Synchronous write end

    // ASynchronous write start
    @Override
    public void putAsynch(long key, String value) {

        try {
            final List<String> successfulServers = new ArrayList<String>();
            successWriteCount = new AtomicInteger(0);
            final CountDownLatch counter = new CountDownLatch(numOfServer);
            for (int i = 0; i < numOfServer; i++) {
                final String currentServerURL = this.serverUrls[i];
                Future<HttpResponse<JsonNode>> future = Unirest
                        .put(currentServerURL + "/cache/{key}/{value}")
                        .header("accept", "application/json")
                        .routeParam("key", Long.toString(key))
                        .routeParam("value", value).asJsonAsync(new Callback<JsonNode>() {
                            String currentUrl = currentServerURL;
                            public void failed(UnirestException e) {
                                System.out.println("The request has failed");
                                counter.countDown();
                            }

                            public void completed(HttpResponse<JsonNode> response) {
                                int code = response.getCode();
                                Headers headers = response.getHeaders();
                                JsonNode body = response.getBody();
                                InputStream rawBody = response.getRawBody();
                                successWriteCount.incrementAndGet();
                                successfulServers.add(currentUrl);
                                counter.countDown();
                            }

                            public void cancelled() {
                                System.out.println("The request has been cancelled");
                            }

                        });

            }
            counter.await();

            if(numOfServer%2==0){
                if(successWriteCount.intValue()>=(numOfServer/2)){
                    System.out.println("Successful Put on servers...");
                    for(String successfulServer:successfulServers){
                        System.out.println(successfulServer);
                    }
                }
                else{
                    System.out.println("Deleting values from Server(s)...");
                    for (int i = 0; i < successfulServers.size(); i++) {
                        System.out.println(successfulServers.get(i));
                        HttpRequestWithBody response = Unirest.delete(successfulServers.get(i)+"/cache/{key}");
                        System.out.println(response);
                    }
                }


            }
            else{
                if(successWriteCount.intValue()>=((numOfServer/2)+1)){
                    System.out.println("Successful Put on servers...");
                    for(String successfulServer:successfulServers){
                        System.out.println(successfulServer);
                    }
                }
                else{
                    System.out.println("Deleting values from Server(s)...");
                    for (int i = 0; i < successfulServers.size(); i++) {
                        System.out.println(successfulServers.get(i));
                        HttpRequestWithBody response = Unirest.delete(successfulServers.get(i)+"/cache/{key}");
                        System.out.println(response);
                    }
                }


            }
        }
        catch(InterruptedException e){
            e.printStackTrace();
        }

    }



}
