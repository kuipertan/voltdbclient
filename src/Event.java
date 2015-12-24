import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;

import java.util.Date;
import java.util.HashMap;

import org.json.JSONArray;
import org.json.JSONException; 
import org.json.JSONObject;

public class Event implements JsonToVoltdb {
	
	String[] keys = new String[]{
		"item_id", "session_id", "uuid", "user_id", "gid", "cid", "event_name", 
		"price", "quantity", "order_id", "ip_address", "ref_page", "creation_time", 
		"insert_time", "session2year", "session6moth", "session2hour", "entry_page", 
		"link", "link_keyword", "code_type", "page_style", "signature", "parent", 
		"browser_type", "user_agent", "os_type", "os_code", "resolution", "username", 
		"flash_version", "java_support tinyint", "color_bit", "loadtime_leftpix", 
		"score", "content", "total", "currency", "address", "express", "pay", 
		"mobile", "name", "express_price", "query_str", "empty ", "rid", "app", 
		"s_iid", "cat", "reviews", "source", "banner_id", "order_num", "page_num", 
		"attr", "source_cid", "target_cid", "tag_id", "tag_value", "dom_path", 
		"top_pix", "percent", "printlist", "click_num", "total_time", "valid_time", 
		"spam", "appversion", "channel", "network", "model", "os", "osversion", 
		"operator", "action", "event_id", "duration", "lat", "lng", "page_id", 
		"installedApps", "appkey", "platform", "d_s", "iids", "ctime", "s_addr"
	};

	HashMap<String, Object> kvs = new HashMap<String, Object>();
	
	private String cid;
	public void SetCid(String cid){
		this.cid = cid;
	}
	public void Cleanup() {
		// TODO Auto-generated method stub
		for(int i = 0; i < keys.length; ++i)
			kvs.put(keys[i], null);
	}

	public boolean WriteToVoltdb(Client client) {
		// TODO Auto-generated method stub
		   try { 		
			   ClientResponse resp = client.callProcedure("EVENT.insert", 
					   kvs.get(keys[0]),kvs.get(keys[1]),kvs.get(keys[2]),kvs.get(keys[3]),kvs.get(keys[4]),
					   kvs.get(keys[5]),kvs.get(keys[6]),kvs.get(keys[7]),kvs.get(keys[8]),kvs.get(keys[9]),
					   kvs.get(keys[10]),kvs.get(keys[11]),kvs.get(keys[12]),kvs.get(keys[13]),kvs.get(keys[14]),
					   kvs.get(keys[15]),kvs.get(keys[16]),kvs.get(keys[17]),kvs.get(keys[18]),kvs.get(keys[19]),
					   kvs.get(keys[20]),kvs.get(keys[21]),kvs.get(keys[22]),kvs.get(keys[23]),kvs.get(keys[24]),
					   kvs.get(keys[25]),kvs.get(keys[26]),kvs.get(keys[27]),kvs.get(keys[28]),kvs.get(keys[29]),
					   kvs.get(keys[30]),kvs.get(keys[31]),kvs.get(keys[32]),kvs.get(keys[33]),kvs.get(keys[34]),
					   kvs.get(keys[35]),kvs.get(keys[36]),kvs.get(keys[37]),kvs.get(keys[38]),kvs.get(keys[39]),
					   kvs.get(keys[40]),kvs.get(keys[41]),kvs.get(keys[42]),kvs.get(keys[43]),kvs.get(keys[44]),
					   kvs.get(keys[45]),kvs.get(keys[46]),kvs.get(keys[47]),kvs.get(keys[48]),kvs.get(keys[49]),
					   kvs.get(keys[50]),kvs.get(keys[51]),kvs.get(keys[52]),kvs.get(keys[53]),kvs.get(keys[54]),
					   kvs.get(keys[55]),kvs.get(keys[56]),kvs.get(keys[57]),kvs.get(keys[58]),kvs.get(keys[59]),
					   kvs.get(keys[60]),kvs.get(keys[61]),kvs.get(keys[62]),kvs.get(keys[63]),kvs.get(keys[64]),
					   kvs.get(keys[65]),kvs.get(keys[66]),kvs.get(keys[67]),kvs.get(keys[68]),kvs.get(keys[69]),
					   kvs.get(keys[70]),kvs.get(keys[71]),kvs.get(keys[72]),kvs.get(keys[73]),kvs.get(keys[74]),
					   kvs.get(keys[75]),kvs.get(keys[76]),kvs.get(keys[77]),kvs.get(keys[78]),kvs.get(keys[79]),
					   kvs.get(keys[80]),kvs.get(keys[81]),kvs.get(keys[82]),kvs.get(keys[83]),kvs.get(keys[84]),
					   kvs.get(keys[85]),kvs.get(keys[86]),kvs.get(keys[87])
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
	
	public boolean WriteToVoltdb(Client client, String iid, Long quantity, Double price) {
		kvs.put("item_id", iid);
		kvs.put("quantity", quantity);
		kvs.put("price", price);
		// TODO Auto-generated method stub
		   try { 		
			   ClientResponse resp = client.callProcedure("EVENT.insert", 
					   kvs.get(keys[0]),kvs.get(keys[1]),kvs.get(keys[2]),kvs.get(keys[3]),kvs.get(keys[4]),
					   kvs.get(keys[5]),kvs.get(keys[6]),kvs.get(keys[7]),kvs.get(keys[8]),kvs.get(keys[9]),
					   kvs.get(keys[10]),kvs.get(keys[11]),kvs.get(keys[12]),kvs.get(keys[13]),kvs.get(keys[14]),
					   kvs.get(keys[15]),kvs.get(keys[16]),kvs.get(keys[17]),kvs.get(keys[18]),kvs.get(keys[19]),
					   kvs.get(keys[20]),kvs.get(keys[21]),kvs.get(keys[22]),kvs.get(keys[23]),kvs.get(keys[24]),
					   kvs.get(keys[25]),kvs.get(keys[26]),kvs.get(keys[27]),kvs.get(keys[28]),kvs.get(keys[29]),
					   kvs.get(keys[30]),kvs.get(keys[31]),kvs.get(keys[32]),kvs.get(keys[33]),kvs.get(keys[34]),
					   kvs.get(keys[35]),kvs.get(keys[36]),kvs.get(keys[37]),kvs.get(keys[38]),kvs.get(keys[39]),
					   kvs.get(keys[40]),kvs.get(keys[41]),kvs.get(keys[42]),kvs.get(keys[43]),kvs.get(keys[44]),
					   kvs.get(keys[45]),kvs.get(keys[46]),kvs.get(keys[47]),kvs.get(keys[48]),kvs.get(keys[49]),
					   kvs.get(keys[50]),kvs.get(keys[51]),kvs.get(keys[52]),kvs.get(keys[53]),kvs.get(keys[54]),
					   kvs.get(keys[55]),kvs.get(keys[56]),kvs.get(keys[57]),kvs.get(keys[58]),kvs.get(keys[59]),
					   kvs.get(keys[60]),kvs.get(keys[61]),kvs.get(keys[62]),kvs.get(keys[63]),kvs.get(keys[64]),
					   kvs.get(keys[65]),kvs.get(keys[66]),kvs.get(keys[67]),kvs.get(keys[68]),kvs.get(keys[69]),
					   kvs.get(keys[70]),kvs.get(keys[71]),kvs.get(keys[72]),kvs.get(keys[73]),kvs.get(keys[74]),
					   kvs.get(keys[75]),kvs.get(keys[76]),kvs.get(keys[77]),kvs.get(keys[78]),kvs.get(keys[79]),
					   kvs.get(keys[80]),kvs.get(keys[81]),kvs.get(keys[82]),kvs.get(keys[83]),kvs.get(keys[84]),
					   kvs.get(keys[85]),kvs.get(keys[86]),kvs.get(keys[87])
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

	public boolean LoadFromJson(JSONObject kfk2sql, JSONObject json) {
		Cleanup();
		ConvertJsonToMap.Convert(kvs, kfk2sql, "Event", json);
		if ((kvs.get("item_id") == null && kvs.get("uuid") == null && !json.has("lst"))|| 
				kvs.get("creation_time") == null)
			return false;
		if (kvs.get("item_id") == null)
			kvs.put("item_id", kvs.get("uuid"));
		
		kvs.put("cid", cid);
		kvs.put("insert_time", new Date().getTime() * 1000L + 8000000L*3600);
		
		return true;
	}
	
	public boolean LoadFromJson(JSONObject root) {
		// TODO Auto-generated method stub
		Cleanup();
		/*
		
    	try{
        	JSONObject inputJSONObject = new JSONObject(json);
			if (!inputJSONObject.has("timestamp") || !inputJSONObject.has("uuid")){       						
				return false;
			}
			
			uuid = inputJSONObject.getString("uuid");
			
			if (inputJSONObject.has("iid")){
				iid = inputJSONObject.getString("iid");
			}
			else if(inputJSONObject.has("lst")){				
				JSONArray lstJSONObject = inputJSONObject.getJSONArray("lst");
				if (lstJSONObject.length() > 0){
					JSONObject c =  lstJSONObject.getJSONObject(0);
					if (c.has("i"))
						iid = c.getString("i");
					else
						iid = uuid;
				}
				else
					iid = uuid;
			}
			else{ // if not item id, we set uuid as it
				iid = uuid;
			}
			
			if (inputJSONObject.has("sid")){
				sid = (inputJSONObject.getString("sid"));
			}
			
			if (inputJSONObject.has("uid") && inputJSONObject.get("uid") instanceof String){
				uid = inputJSONObject.getString("uid");
			}
			
			if (inputJSONObject.has("gid")){
				gid = (inputJSONObject.getString("gid"));
			}
			
			if (inputJSONObject.has("cid")){
				cid = (inputJSONObject.getString("cid"));
			}
			
			if (inputJSONObject.has("method")){
				method = (inputJSONObject.getString("method"));
			}

			
			if (inputJSONObject.has("price")){
				price = (inputJSONObject.getString("price"));
			}
			
			if (inputJSONObject.has("ip")){
				ip = (inputJSONObject.getString("ip")).split(",")[0];
			}
			
			if (inputJSONObject.has("ref_page") && inputJSONObject.get("ref_page") instanceof String){
				ref_page = inputJSONObject.getString("ref_page").replace("\\", "\\\\");
				if (ref_page.length() > 1000)
					ref_page = ref_page.substring(0,999);
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
			
			if (inputJSONObject.has("p_t")){
				p_t = (inputJSONObject.getString("p_t"));
			}
			
			if (inputJSONObject.has("user_agent")){
				user_agent = (inputJSONObject.getString("user_agent"));
				if (user_agent.length() > 255)
					user_agent = user_agent.substring(0, 254);
			}
			
			if (inputJSONObject.has("ja")){
				if (inputJSONObject.getBoolean("ja") )					
					ja = 1;
				else
					ja = 0;
			}		

			if (inputJSONObject.has("qstr")){
				qstr = (inputJSONObject.getString("qstr"));
			}
			
			if (inputJSONObject.has("emp")){
				if (inputJSONObject.getBoolean("emp") )
					emp = 1;
				else
					emp = 0;
			}
			
			if (inputJSONObject.has("p_bid")){
				p_bid = (inputJSONObject.getString("p_bid"));
			}
					  
        } catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}  */ 	
    	return true;		

	}

}
