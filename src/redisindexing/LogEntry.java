/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package redisindexing;

import java.util.Date;
import java.util.HashMap;

/**
 *
 * @author marin
 */
public class LogEntry {
    
    private String flowID;
    private Date logTime; 
    private HashMap<String, String> parameters = new HashMap<>();

    /**
     * @return the flowID
     */
    public String getFlowID() {
        return flowID;
    }

    /**
     * @param flowID the flowID to set
     */
    public void setFlowID(String flowID) {
        this.flowID = flowID;
    }

    /**
     * @return the logTime
     */
    public Date getLogTime() {
        return logTime;
    }

    /**
     * @param logTime the logTime to set
     */
    public void setLogTime(Date logTime) {
        this.logTime = logTime;
    }

    /**
     * @return the parameters
     */
    public HashMap<String, String> getParameters() {
        return parameters;
    }

    /**
     * @param parameters the parameters to set
     */
    public void add(String key, String value) {
        parameters.put(key, value);
    }
    
    @Override
    public String toString() {
        return "LogEntry | " + 
                " FlowID:" + flowID + ";" +
                " LogTime:" + logTime + ";" +
                " Service:" + parameters.get("service") + ";" +
                " MSISDN:" + parameters.get("MSISDN") + ";" +
                " Package:" + parameters.get("package") + ";";
    } 

}
