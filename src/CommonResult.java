
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import org.voltdb.client.*;

import org.json.JSONArray;
import org.json.JSONException;  
import org.json.JSONObject;
import org.voltdb.client.ClientResponse;  


public class CommonResult implements JsonToVoltdb{
	JSONArray iids = null; 
	//boolean bIid;
	VoltdbBolt bolt;
	
	String[] keys = new String[]{
		"uuid", "req_id", "cid","gid", "user_id", "session_id", "p_bid", "abtest", 
		"ab_tactics", "ab_name", "ab_bypass", "creation_time", "insert_time", 
		"session2year", "session6moth", "session2hour", "spam", "t_cid", "result", "fmt"
	};
		
	HashMap<String, Object> kvs = new HashMap<String, Object>();
	
	public CommonResult(VoltdbBolt bolt){
		this.bolt = bolt;
	}
	
	public void Cleanup(){	
		
		for(int i = 0; i < keys.length; ++i)
			kvs.put(keys[i], null);
	}
	
	public boolean WriteToVoltdb(Client client){
		
		   try { 
			   if( iids != null ){										
					int i = 0;
					for (; i < iids.length(); ++i){
				   		ClientResponse resp = client.callProcedure("RESULT_IID.insert", 
				   				iids.getString(i),kvs.get("req_id"),kvs.get("gid"),kvs.get("user_id"),kvs.get("session_id"),
								kvs.get("p_bid"),kvs.get("ab_tactics"),kvs.get("creation_time"),kvs.get("insert_time")
			   						);
						byte stat = resp.getStatus();
						if (stat != ClientResponse.SUCCESS){
							if (stat == ClientResponse.CONNECTION_TIMEOUT) {
							   	System.out.println("A procedure invocation has timed out.");
							}
							if (stat == ClientResponse.CONNECTION_LOST) {
								System.out.println("Connection lost before procedure response.");
							}
						}
					}
			   }
			   else{
			   		ClientResponse resp = client.callProcedure("RESULT.insert", 
					   kvs.get(keys[0]),kvs.get(keys[1]),kvs.get(keys[2]),kvs.get(keys[3]),kvs.get(keys[4]),
					   kvs.get(keys[5]),kvs.get(keys[6]),kvs.get(keys[7]),kvs.get(keys[8]),kvs.get(keys[9]),
					   kvs.get(keys[10]),kvs.get(keys[11]),kvs.get(keys[12]),kvs.get(keys[13]),kvs.get(keys[14]),
					   kvs.get(keys[15]),kvs.get(keys[16]),kvs.get(keys[17]),kvs.get(keys[18]),kvs.get(keys[19])
   						);
					byte stat = resp.getStatus();
					if (stat != ClientResponse.SUCCESS){
						if (stat == ClientResponse.CONNECTION_TIMEOUT) {
						   	System.out.println("A procedure invocation has timed out.");
						}
						if (stat == ClientResponse.CONNECTION_LOST) {
							System.out.println("Connection lost before procedure response.");
						}
					}
			   }
			   
		   } catch (Exception e) { 
			   e.printStackTrace();
			   return false;
		   }
		return true;
	}
	
	
	public boolean LoadFromJson(JSONObject kfk2sql, JSONObject json) throws JSONException {
		Cleanup();
		ConvertJsonToMap.Convert(kvs, kfk2sql, "Result", json);
		if (kvs.get("creation_time") == null)
			return false;
		
		String cid = json.optString("cid");
		
		//if( cid.compareTo("Czhuishusq") == 0) {	// enter result_iid table	
		if(bolt.iidTableCids.contains(cid)) {	
			try {			
				iids = json.getJSONArray("rec_item_ids");
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//bIid = true;
		}
		else {
			//bIid = false;
			iids = null;
			if (kvs.get("session_id") == null)
				kvs.put("session_id", kvs.get("uuid"));
		}
		
		kvs.put("insert_time", new Date().getTime() * 1000L + 8000000L*3600);
				   
		return true;
	}
	
}

