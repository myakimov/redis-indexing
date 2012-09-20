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
public class SearchIndex {
    
    public static Set<String> search(Map<String, String> searchCriteria, 
                                      List<IndexDescriptor> indexes ,
                                      Date fromDate,
                                      Date toDate,
                                      Jedis j) {
        
        System.out.println("FromDate : " + fromDate + "; ToDate : " + toDate);
        
        
        Set<String> results = new LinkedHashSet<>();
        List<String> indexKeys = new ArrayList<>();
        
        for (IndexDescriptor id : indexes) {
            List<String> coveredKeys = id.indexing(searchCriteria);
            
            System.out.println("Index " + id.indexID + " covered " + coveredKeys.size() + " out of " + searchCriteria.size());
            
            indexKeys.addAll(id.getIndexKeys(searchCriteria, fromDate, toDate));
        }
    
        List<Set<String>> resultsFromIndex = new ArrayList<>();
        
        for(String indexKey : indexKeys) {
            
            double startScore = 0D;
            double endScore = 0D;
            
            if (toDate == null) 
                endScore = Double.MAX_VALUE;
            else {
                Calendar c = Calendar.getInstance();
                c.setTime(toDate);
                long time = c.getTimeInMillis();
                Calendar cDate = Calendar.getInstance();
                cDate.set(c.get(Calendar.YEAR), c.get(Calendar.MONTH), c.get(Calendar.DAY_OF_MONTH),0,0,0);
                long dateTime = cDate.getTimeInMillis();
                endScore = (double)(time - dateTime);
            }
            
            if (fromDate == null) 
                startScore = 0D;
            else {
                Calendar c = Calendar.getInstance();
                c.setTime(fromDate);
                long time = c.getTimeInMillis();
                Calendar cDate = Calendar.getInstance();
                cDate.set(c.get(Calendar.YEAR), c.get(Calendar.MONTH), c.get(Calendar.DAY_OF_MONTH),0,0,0);
                long dateTime = cDate.getTimeInMillis();
                startScore = (double)(time - dateTime);
            }
            
            System.out.println("Start Score : " + startScore + "; EndScore : " + endScore);
            Set<String> resultsSet = j.zrangeByScore(indexKey, startScore, endScore, 1, 100);
            
//            System.out.println("Search Result for key : " + indexKey);
//            for (String result : resultsSet) {
//                System.out.println("Result : " + result);
//            }
            
            resultsFromIndex.add(resultsSet);
       
        }
   
        if (resultsFromIndex.size() > 1) {
            
            Set<String> set2 = resultsFromIndex.get(1);
            
            for (String result1 : resultsFromIndex.get(0)) {
                if (set2.contains(result1))
                    results.add(result1);
            }
        }
        
        return results;
    }
}
