import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;

import java.util.Date;
import java.util.HashMap;

public class Request implements JsonToVoltdb {
	
	private VoltdbBolt bolt;
	
	String[] keys = new String[]{
		"item_id", "cid", "gid", "user_id", "session_id", "uuid", "req_id", "request_name", 
		"category", "category_id", "brand", "brand_id", "days", "ratio", "filter", 
		"ip", "ref_page", "creation_time", "insert_time", "session2year", 
		"session6moth", "session2hour", "ignore_domain", "rule", "store", 
		"item_target_type", "callback", "format", "include_base", "page_type", 
		"banner_id", "order_num", "num", "type", "base_num", "et", "t_cid", "spam"
	};

	HashMap<String, Object> kvs = new HashMap<String, Object>();
	
	public Request(VoltdbBolt bolt){
		this.bolt = bolt;
	}

	public void Cleanup() {
		// TODO Auto-generated method stub
		
		for(int i = 0; i < keys.length; ++i)
			kvs.put(keys[i], null);
	}

	public boolean WriteToVoltdb(Client client) {
		// TODO Auto-generated method stub
		   try { 	
			  
			   	ClientResponse resp = client.callProcedure("REQUEST.insert", 
					   kvs.get(keys[0]),kvs.get(keys[1]),kvs.get(keys[2]),kvs.get(keys[3]),kvs.get(keys[4]),
					   kvs.get(keys[5]),kvs.get(keys[6]),kvs.get(keys[7]),kvs.get(keys[8]),kvs.get(keys[9]),
					   kvs.get(keys[10]),kvs.get(keys[11]),kvs.get(keys[12]),kvs.get(keys[13]),kvs.get(keys[14]),
					   kvs.get(keys[15]),kvs.get(keys[16]),kvs.get(keys[17]),kvs.get(keys[18]),kvs.get(keys[19]),
					   kvs.get(keys[20]),kvs.get(keys[21]),kvs.get(keys[22]),kvs.get(keys[23]),kvs.get(keys[24]),
					   kvs.get(keys[25]),kvs.get(keys[26]),kvs.get(keys[27]),kvs.get(keys[28]),kvs.get(keys[29]),
					   kvs.get(keys[30]),kvs.get(keys[31]),kvs.get(keys[32]),kvs.get(keys[33]),kvs.get(keys[34]),
					   kvs.get(keys[35]),kvs.get(keys[36]),kvs.get(keys[37])
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
		   
		   } catch (Exception e) { 
			   e.printStackTrace();
			   return false;
		   }
		   return true;
	}
	
	public boolean LoadFromJson(JSONObject kfk2sql, JSONObject json) throws JSONException {
		Cleanup();
		ConvertJsonToMap.Convert(kvs, kfk2sql, "Request", json);
		if (kvs.get("uuid") == null || kvs.get("creation_time") == null)
			return false;
		if (kvs.get("item_id") == null)
			kvs.put("item_id", kvs.get("uuid"));
		kvs.put("insert_time", new Date().getTime() * 1000L + 8000000L*3600);
		
		return true;
	}

	public boolean LoadFromJson(JSONObject inputJSONObject) {
		// TODO Auto-generated method stub
		Cleanup();
		/*
		
    	try{
        	
			if (!inputJSONObject.has("timestamp") || !inputJSONObject.has("uuid")){       						
				return false;
			}
			
			uuid = inputJSONObject.getString("uuid");
				   
			if (inputJSONObject.has("iid")){
				item_id = inputJSONObject.getString("iid");
			}
			else{ // if not item id, we set uuid as it
				item_id = uuid;
			}
			
			if (inputJSONObject.has("sid")){
				session_id = (inputJSONObject.getString("sid"));
			}
			
			if (inputJSONObject.has("uid") && inputJSONObject.get("uid") instanceof String){
				user_id = inputJSONObject.getString("uid");
			}
			
			if (inputJSONObject.has("gid")){
				gid = (inputJSONObject.getString("gid"));
			}
			
			if (inputJSONObject.has("req_id")){
				req_id = (inputJSONObject.getString("req_id"));
			}
			   
			if (inputJSONObject.has("cat")){
				category = (inputJSONObject.getString("cat"));
			}
			
			if (inputJSONObject.has("ip")){
				ip = (inputJSONObject.getString("ip"));//.split(",")[0];
			}
			
			if (inputJSONObject.has("ref_page") && inputJSONObject.get("ref_page") instanceof String){
				ref_page = inputJSONObject.getString("ref_page");//.replace("\\", "\\\\");
				if (ref_page.length() > 1000)
					ref_page = ref_page.substring(0,999);
			}
			
			creation_time = (Math.round(1000000*inputJSONObject.getDouble("timestamp")))+ 8000000*3600;
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
			
			
			if (inputJSONObject.has("p_t")){
				page_type = (inputJSONObject.getString("p_t"));
			}
			
			if (inputJSONObject.has("p_bid")){
				banner_id = (inputJSONObject.getString("p_bid"));
			}
			
			if (inputJSONObject.has("et")){
				et = (inputJSONObject.getString("et"));
			}
			
			if (inputJSONObject.has("method")){
				request_name = (inputJSONObject.getString("method"));
			}
			
			if (inputJSONObject.has("cat_id")){
				category_id = (inputJSONObject.getString("cat_id"));
			}
			
			if (inputJSONObject.has("brd")){
				brand = (inputJSONObject.getString("brd"));
			}
			
			if (inputJSONObject.has("brd_id")){
				brand_id = (inputJSONObject.getString("brd_id"));
			}
			
			if (inputJSONObject.has("dat")){
				days = (inputJSONObject.getLong("dat"));
			}
			
			if (inputJSONObject.has("rat")){
				ratio = (inputJSONObject.getDouble("rat"));
			}
			
			if (inputJSONObject.has("flt")){
				filter = (inputJSONObject.getString("flt"));
			}

			if (inputJSONObject.has("ignore_domain")){
				ignore_domain = (inputJSONObject.getInt("ignore_domain"));
			}
			
			if (inputJSONObject.has("rule")){
				rule = (inputJSONObject.getString("rule"));
			}
			
			if (inputJSONObject.has("store")){
				store = (inputJSONObject.getInt("store"));
			}
			
			if (inputJSONObject.has("itt")){
				item_target_type = (inputJSONObject.getString("itt"));
			}
			
			if (inputJSONObject.has("callback")){
				callback = (inputJSONObject.getString("callback"));
			}
						
			
			if (inputJSONObject.has("fmt")){
				format = (inputJSONObject.getString("fmt"));
			}
			
			if (inputJSONObject.has("inb")){
				include_base = (inputJSONObject.getInt("inb"));
			}
			
			if (inputJSONObject.has("p_bn")){
				order_num = (inputJSONObject.getLong("p_bn"));
			}
			
			if (inputJSONObject.has("num")){
				num = (inputJSONObject.getLong("num"));
			}
			
			if (inputJSONObject.has("typ")){
				type = (inputJSONObject.getString("typ"));
			}
			
			if (inputJSONObject.has("bn")){
				base_num = (inputJSONObject.getLong("bn"));
			}
			
			if (inputJSONObject.has("t_cid")){
				t_cid = (inputJSONObject.getString("t_cid"));
			}
			
			if (inputJSONObject.has("spam")){
				spam = (inputJSONObject.getString("spam"));
			}
			
					  
        } catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}   
		*/	
    	return true;		

	}

}
