/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package redisindexing;

import java.util.*;
import redis.clients.jedis.Jedis;

/**
 *
 * @author marin
 */
public class RedisIndexing {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        Jedis j = new Jedis("localhost");
        Set<String> keys = j.keys("*");
        
//        // Clear keys
//        for (String key : keys) {
//            System.out.println(key);
//            j.del(key);
//        
//        }
        
        // Sample Log Entry
        List<LogEntry> lel = prepareTests();
        
        
        // Index Definition
        // ID : 1
        // Keys : MSISDN, Service, Date
        // Result : flowID
        
        // Sorted Set - score - the time of the day
        // index:1:359878210682:getCustomerInvoices:2012-09-13
        // index:1:359878210682:openOrder:2012-09-13
        // index:1:359878210682:cancelSubscription:2012-09-13
        
        // index:1:359878210683:getCustomerInvoices:2012-09-13
        // index:1:359878210683:openOrder:2012-09-13
        // index:1:359878210683:cancelSubscription:2012-09-13
                
        // Index 2 
        // Keys : MSISDN, Date
        // List/Set - msisdn blocks containing the msisdn
        // index:2:359878210682:2012-09-13
       
        
        // Index 3 
        // Keys : Service, Date
        // List/Set - operations blocks containing the msisdn
        // index:3:getCustomerInvoices:2012-09-13
        //   -- index:1:359878210683:getCustomerInvoices:2012-09-13
        //   -- index:1:359878210682:getCustomerInvoices:2012-09-13
        
        
        // Index 4
        // Target Country + MSISDN + Operation + Date
        // 
        
        
        IndexDescriptor idx1 = new IndexDescriptor(1,j);
        idx1.addIndexColumn("MSISDN");
        idx1.addIndexColumn("service");

        for (LogEntry le : lel) {
            idx1.index(le);
        }
        
        IndexDescriptor idx2 = new IndexDescriptor(2,j);
        idx2.addIndexColumn("MSISDN");
        idx2.addIndexColumn("package");

        for (LogEntry le : lel) {
            j.set("entry:" + le.getFlowID(), le.toString());
            idx2.index(le);
        }
        
        
        HashMap<String, String> searchCriteria = new HashMap<>();
        searchCriteria.put("MSISDN", "359878210045");
        searchCriteria.put("service", "initiateOrder");
        searchCriteria.put("package", "pkg0001");
        
        List<IndexDescriptor> ld = new ArrayList<>();
        ld.add(idx1);
        ld.add(idx2);
        
        Set<String> search;
        
//        // ---------------------------------------------------------------------
//        System.out.println("==== First search - some keys");
//        search = SearchIndex.search(searchCriteria, ld, new Date(), new Date(2012, 9, 18, 18, 8, 00), j);
//        
//        for (String searchResult : search)
//            System.out.println("Search Result : " + searchResult);
//        // ---------------------------------------------------------------------
        
        
        
        // ---------------------------------------------------------------------
        System.out.println("==== Second search - composite keys");
        Calendar c = Calendar.getInstance();
        c.set(2012, 8, 19, 0, 0, 0);
        Date fromDate = c.getTime();
        search = SearchIndex.search(searchCriteria, ld, fromDate, null, j);
        
        for (String searchResult : search) {
            System.out.println("= Search Result : " + searchResult);
            System.out.println("== Entry : " + j.get("entry:" + searchResult));
        }
        // ---------------------------------------------------------------------     
        
        
        // ---------------------------------------------------------------------
        System.out.println("==== Second(2) search - composite keys with date");
        c = Calendar.getInstance();
        c.set(2012, 8, 19, 15, 59, 0);
        fromDate = c.getTime();
        c.set(2012, 8, 19, 16, 22, 0);
        Date toDate = c.getTime();
        
        search = SearchIndex.search(searchCriteria, ld, fromDate, toDate, j);
        
        for (String searchResult : search) {
            System.out.println("= Search Result : " + searchResult);
            System.out.println("== Entry : " + j.get("entry:" + searchResult));
        }
        // ---------------------------------------------------------------------        
    }
    
    
    
    
    private static List<LogEntry> prepareTests() {
        
        LogEntry le;
        String MSISDNbase = "35987821";
        List<LogEntry> lel = new ArrayList<>();
        
        for (int i = 0; i < 100; i++) {
            le= new LogEntry();
            le.setFlowID(UUID.randomUUID().toString());
            le.add("auditCountryCode","BG");
            String MSISDN = MSISDNbase + String.format("%04d", i);
            le.add("MSISDN",MSISDN);
            
            // Random
            Random r = new Random();
            int operationID = r.nextInt(4);
            switch(operationID) {
             
                case 1  :  le.add("service", "getCustomerInvoices");break;
                case 2  :  le.add("service", "initiateOrder");break;
                case 3  :  le.add("service", "cancelOrder");break;
                default :  le.add("service", "terminateContract");break;
                
            }
            
            int packageID = r.nextInt(4);
            switch(packageID) {
             
                case 1  :  le.add("package", "pkg0001");break;
                case 2  :  le.add("package", "pkg0002");break;
                case 3  :  le.add("package", "pkg0003");break;
                default :  le.add("package", "pkg0004");break;
                
            }
            
            
            le.add("individualID", "22212");
            le.add("targetCountry", "DE");
            le.setLogTime(new Date());
            lel.add(le);
            
            System.out.println(le);
        }
        
        return lel;
        
    }
    
}
