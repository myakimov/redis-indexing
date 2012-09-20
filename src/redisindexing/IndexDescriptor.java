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
enum INDEX_TYPE {
    BASE_INDEX,
    CHILD_INDEX
}


public class IndexDescriptor {
    
    private List<String> indexKeys = new ArrayList<>();
    
    int indexID;
    private Jedis jedisConn;
    
    public IndexDescriptor(int indexID, Jedis j) {
        this.indexID = indexID;
        jedisConn = j;
    }
    
    public void addIndexColumn(String indexColumn) {
        indexKeys.add(indexColumn);
    }
    
    public List<String> indexing(Map<String, String> searchCriteria) {
        
        List<String> coveredKeys = new ArrayList<>();
        
        for (String key : searchCriteria.keySet()) {
            if (indexKeys.contains(key))
                coveredKeys.add(key);
        }
        
        return coveredKeys;
    }
    
    
    public List<String> getIndexKeys(Map<String, String> searchCriteria, Date fromDate, Date toDate) {
       
       String indexKey = "index:" + indexID; 
      
       Calendar c = Calendar.getInstance();
       c.setTime(fromDate);
       String dateSuffix = ":" + c.get(Calendar.YEAR) + "-" + c.get(Calendar.MONTH) + "-" + c.get(Calendar.DAY_OF_MONTH);

       System.out.println("Number of search criteria : " + searchCriteria.size());
       
       // Only one key - search is atomic + dates
       if (searchCriteria.size() == 1) {
           String atomicKey = searchCriteria.keySet().iterator().next();
           if (indexKeys.contains(atomicKey)) 
               indexKey = "index:atomic:" + atomicKey + ":" + searchCriteria.values().iterator().next() + dateSuffix;
           
       } else {
           for (String key : indexKeys) {

               if (searchCriteria.keySet().contains(key)) {
                   indexKey += ":" + searchCriteria.get(key);
               }
           }

           indexKey += dateSuffix;
       }
       
       System.out.println("Search IndexKey : " + indexKey);
       
       ArrayList<String> ik = new ArrayList<>();
       ik.add(indexKey);
       
       return ik;
        
    }
    
    
    public boolean index(LogEntry le) {
        
        String indexKey = "index:" + indexID;
        List<String> subIndexKeys = new ArrayList<>();
        
        // Calculating the date
        Calendar c = Calendar.getInstance();
        c.setTime(le.getLogTime());
        long time = c.getTimeInMillis();
        Calendar cDate = Calendar.getInstance();
        cDate.set(c.get(Calendar.YEAR), c.get(Calendar.MONTH), c.get(Calendar.DAY_OF_MONTH),0,0,0);
        long dateTime = cDate.getTimeInMillis();
        double timeInDay = (double)(time - dateTime);
        String dateSuffix = ":" + c.get(Calendar.YEAR) + "-" + c.get(Calendar.MONTH) + "-" + c.get(Calendar.DAY_OF_MONTH);
        
        for (String key : indexKeys) {
            
            String value = le.getParameters().get(key);
            // A value cannot be found in the entry, ignore indexing
            if (value == null)
                return false;
            
            String subIndexKey = "index:atomic:" + key + ":" + value + dateSuffix;           
            System.out.println("SubIndexKey : " + subIndexKey);
            subIndexKeys.add(subIndexKey);
            indexKey += ":" + value;
        }
        
        indexKey += dateSuffix;
        
        System.out.println("IndexKey : " + indexKey);
        Map<Double,String> indexEntry = new HashMap<>();
        indexEntry.put(timeInDay, le.getFlowID());
        
        jedisConn.zadd(indexKey, indexEntry);
        
        for (String subIndexKey : subIndexKeys) {
            indexEntry = new HashMap<>();
            indexEntry.put(timeInDay, le.getFlowID());
            jedisConn.zadd(subIndexKey, indexEntry);
        }
       
        return true;
    }
    
}
