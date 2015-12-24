import java.util.ArrayList;
import java.util.Date;

import org.voltdb.client.*;

import org.json.JSONArray;
import org.json.JSONException;  
import org.json.JSONObject;
 


public class ZhuiShuResult implements JsonToVoltdb{
	public ArrayList<String> item_id = new ArrayList<String>(); 
	public String req_id;
	public String uuid;
	public String gid;
	public String uid;
	public String sid;
	public String p_bid;
	public String ab_tactics;
	public String ab_name;
	public String ab_bypass;
	public Long timestamp;
	public String tma;
	public String tmd;
	public String tmc;
	
	public void Cleanup(){		
		item_id.clear(); 
		req_id = null;
		uuid = null;
		gid = null;
		uid = null;
		sid = null;
		p_bid = null;
		ab_tactics = null;
		ab_name = null;
		ab_bypass = null;
		timestamp = null;
		tma = null;
		tmd = null;
		tmc = null;
	}
	
	public boolean WriteToVoltdb(Client client){
		
		   try { 		
			   for (int i = 0; i < item_id.size(); ++i){
				   String iid = item_id.get(i);
				   ClientResponse resp = client.callProcedure("Czhuishusq_WRITE", 
	   						iid, 		req_id, 	uuid, 		gid, 		uid,
	   						sid, 		p_bid, 		ab_tactics,	ab_name,	ab_bypass,
	   						timestamp,	tma,		tmd,		tmc
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
	
	public boolean LoadFromJson(JSONObject kfk2sql, JSONObject json) {
		/*
		ConvertJsonToMap.Convert(kvs, kfk2sql, "Item", json);
		if (kvs.get("item_id") == null || kvs.get("item_name") == null ||
			kvs.get("item_link") == null || kvs.get("creation_time") == null)
			return false;
		kvs.put("insert_time", new Date().getTime() * 1000 + 8000000*3600);
		kvs.put("is_active", 1);
		*/
		return true;
	}
	
	public boolean LoadFromJson(JSONObject inputJSONObject) {
		Cleanup();
		try { 
			//JSONObject inputJSONObject = new JSONObject(json);
			if (!inputJSONObject.has("timestamp") || !inputJSONObject.has("rec_item_ids") 
					|| !inputJSONObject.has("uuid")){       						
				return false;
			}
			
			JSONArray lstJSONObject = inputJSONObject.getJSONArray("rec_item_ids");
			
			if (0 == lstJSONObject.length())
				return false;
			
			int i = 0;
			for (; i < lstJSONObject.length(); ++i){
				item_id.add(lstJSONObject.getString(i));
			}
		
			if (inputJSONObject.has("req_id")){
				req_id = inputJSONObject.getString("req_id");
			}
			
			//if (inputJSONObject.has("uuid")){
				uuid = inputJSONObject.getString("uuid");
			//}
			
			if (inputJSONObject.has("gid")){
				gid = (inputJSONObject.getString("gid"));
			}
						
			if (inputJSONObject.has("uid") && inputJSONObject.get("uid") instanceof String){
				uid = inputJSONObject.getString("uid");
			}
			
			if (inputJSONObject.has("sid")){
				sid = inputJSONObject.getString("sid");
			}
			
			if (inputJSONObject.has("p_bid") && inputJSONObject.get("p_bid") instanceof String){
				p_bid = (inputJSONObject.getString("p_bid"));
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
			
			timestamp = (Math.round(1000000*inputJSONObject.getDouble("timestamp")));
			
			if (inputJSONObject.has("tma")){
				tma = (inputJSONObject.getString("tma"));
			}
			
			if (inputJSONObject.has("tmd")){
				tmd = (inputJSONObject.getString("tmd"));
			}
			
			if (inputJSONObject.has("tmc")){
				tmc = (inputJSONObject.getString("tmc"));
			}			
		} catch (Exception e) { 
		   e.printStackTrace();
		   return false;
		}
		
		return true;
	}
}
