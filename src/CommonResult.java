
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import org.voltdb.client.*;

import org.json.JSONArray;
import org.json.JSONException;  
import org.json.JSONObject;
import org.voltdb.client.ClientResponse;  


public class CommonResult implements JsonToVoltdb{

	String[] keys = new String[]{
		"uuid", "req_id", "gid", "user_id", "session_id", "p_bid", "abtest", 
		"ab_tactics", "ab_name", "ab_bypass", "creation_time", "insert_time", 
		"session2year", "session6moth", "session2hour", "spam", "t_cid",  "result","fmt"
	};
	
	HashMap<String, Object> kvs = new HashMap<String, Object>();
	
	
	public void Cleanup(){	

		for(int i = 0; i < keys.length; ++i)
			kvs.put(keys[i], null);
	}
	
	public boolean WriteToVoltdb(Client client){
		
		   try { 
			   		ClientResponse resp = client.callProcedure("RESULT.insert", 
					   kvs.get(keys[0]),kvs.get(keys[1]),
					   kvs.get(keys[5]),kvs.get(keys[6]),kvs.get(keys[7]),kvs.get(keys[8]),kvs.get(keys[9]),
					   kvs.get(keys[10]),kvs.get(keys[11]),kvs.get(keys[12]),kvs.get(keys[13]),kvs.get(keys[14]),
					   kvs.get(keys[15]),kvs.get(keys[16]),kvs.get(keys[17]),kvs.get(keys[18])
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
					
			   		resp = client.callProcedure("RESULT_IDENTIFY.insert", 
					   kvs.get("uuid"),kvs.get("gid"),kvs.get("user_id"),kvs.get("session_id"));
					stat = resp.getStatus();
					if (stat != ClientResponse.SUCCESS){
						if (stat == ClientResponse.CONNECTION_TIMEOUT) {
						   	System.out.println("A procedure invocation has timed out.");
						}
						if (stat == ClientResponse.CONNECTION_LOST) {
							System.out.println("Connection lost before procedure response.");
						}
					}
					
			   
		   } catch (Exception e) { 
			   e.printStackTrace();
			   return false;
		   }
		return true;
	}
	
	public boolean WriteResultItem(Client client, String iid){
		
		   try { 
			   /*
				   ClientResponse resp = client.callProcedure("RESULT_WRITE", 
						   uuid, req_id, gid, user_id, session_id, p_bid, abtest, 
						   ab_tactics, ab_name, ab_bypass, creation_time, insert_time, 
						   session2year, session6moth, session2hour, spam, t_cid, result, fmt
	   						);
	   						*/
			   		ClientResponse resp = client.callProcedure("RESULT_ITEM.insert", 
					    iid, kvs.get("uuid"));
					byte stat = resp.getStatus();
					if (stat != ClientResponse.SUCCESS){
						if (stat == ClientResponse.CONNECTION_TIMEOUT) {
						   	System.out.println("A procedure invocation has timed out.");
						}
						if (stat == ClientResponse.CONNECTION_LOST) {
							System.out.println("Connection lost before procedure response.");
						}
					}
			   
		   } catch (Exception e) { 
			   e.printStackTrace();
			   return false;
		   }
		return true;
	}
	
	public boolean LoadFromJson(JSONObject kfk2sql, JSONObject json) {
		Cleanup();
		ConvertJsonToMap.Convert(kvs, kfk2sql, "Result", json);
		if (kvs.get("uuid") == null || kvs.get("creation_time") == null)
			return false;
		kvs.put("insert_time", new Date().getTime() * 1000L + 8000000L*3600);
		
		return true;
	}
	
	public boolean LoadFromJson(JSONObject inputJSONObject) {
		/*
		Cleanup();
		try { 
			if (!inputJSONObject.has("timestamp")  
					|| inputJSONObject.has("uuid")){       						
				return false;
			}
			
			uuid = inputJSONObject.getString("uuid");
		
			if (inputJSONObject.has("req_id")){
				req_id = inputJSONObject.getString("req_id");
			}
			
			
			if (inputJSONObject.has("gid")){
				gid = (inputJSONObject.getString("gid"));
			}
						
			if (inputJSONObject.has("uid") && inputJSONObject.get("uid") instanceof String){
				user_id = inputJSONObject.getString("uid");
			}
			
			if (inputJSONObject.has("sid")){
				session_id = inputJSONObject.getString("sid");
			}
			
			if (inputJSONObject.has("p_bid") && inputJSONObject.get("p_bid") instanceof String){
				p_bid = (inputJSONObject.getString("p_bid"));
			}
			
			if (inputJSONObject.has("__abtest__") && inputJSONObject.get("__abtest__") instanceof JSONArray){
				abtest = "";
				JSONArray abJSONObject = inputJSONObject.getJSONArray("__abtest__");								
				int i = 0;
				for (; i < abJSONObject.length(); ++i){
					abtest += (abJSONObject.getString(i));
				}
			}
						
			if (inputJSONObject.has("ab_tactics") && inputJSONObject.get("ab_tactics") instanceof String){									
				ab_tactics = (inputJSONObject.getString("ab_tactics"));
			}
			
			if (inputJSONObject.has("ab_name") && inputJSONObject.get("ab_name") instanceof String){
				ab_name = (inputJSONObject.getString("ab_name"));
			}
			
			if (inputJSONObject.has("ab_bypass") && inputJSONObject.get("ab_bypass") instanceof String){
				ab_bypass = (inputJSONObject.getString("ab_bypass"));
			}
			
			creation_time = (Math.round(1000000*inputJSONObject.getDouble("timestamp")) + 8000000*3600);
			insert_time = new Date().getTime() * 1000 + 8000000*3600;
			
			if (inputJSONObject.has("tma")){
				session2year = (inputJSONObject.getString("tma"));
			}
			
			if (inputJSONObject.has("tmd")){
				session6moth = (inputJSONObject.getString("tmd"));
			}
			
			if (inputJSONObject.has("tmc")){
				session2hour = (inputJSONObject.getString("tmc"));
			}	
			
			if (inputJSONObject.has("spam")){
				spam = (inputJSONObject.getString("spam"));
			}
			
			if (inputJSONObject.has("t_cid")){
				t_cid = (inputJSONObject.getString("t_cid"));
			}
			
			if (inputJSONObject.has("rec_item_ids")){
				result = "";
				JSONArray lstJSONObject = inputJSONObject.getJSONArray("rec_item_ids");								
				int i = 0;
				for (; i < lstJSONObject.length(); ++i){
					result += (lstJSONObject.getString(i));
				}
			}
			
			if (inputJSONObject.has("__fmt__")){
				fmt = (inputJSONObject.getString("__fmt__"));
			}
			
			
		} catch (Exception e) { 
		   e.printStackTrace();
		   return false;
		}
		*/
		return true;
	}
}

