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
	private boolean bIid;
	
	private VoltdbBolt bolt;
	
	public Event(VoltdbBolt bolt){
		this.bolt = bolt;
	}
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
		String proc = "EVENT.insert";
		if (bIid)
			proc = "EVENT_IID.insert";
	   try { 		
		   ClientResponse resp = client.callProcedure(proc, 
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
		String proc = null;
		if (bIid)
			proc = "EVENT_IID.insert";
		else
			proc = "EVENT.insert";
		//kvs.put("item_id", iid);
		//kvs.put("quantity", quantity);
		kvs.put("price", price);
		// TODO Auto-generated method stub
		   try { 		
			   ClientResponse resp = client.callProcedure(proc, 
					   /*kvs.get(keys[0])*/iid,kvs.get(keys[1]),kvs.get(keys[2]),kvs.get(keys[3]),kvs.get(keys[4]),
					   kvs.get(keys[5]),kvs.get(keys[6]),/*kvs.get(keys[7])*/price,/*kvs.get(keys[8])*/quantity,kvs.get(keys[9]),
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

	public boolean LoadFromJson(JSONObject kfk2sql, JSONObject json) throws JSONException {
		Cleanup();
		ConvertJsonToMap.Convert(kvs, kfk2sql, "Event", json);
		String cid = json.optString("cid");
		
		/*
		if (kvs.get("cid") == null)
			kvs.put("cid", cid);
		*/
		if (kvs.get("creation_time") == null)
			return false;
		
		//if( cid.compareTo("Czhuishusq") == 0) {	// enter event_iid table	
		if(bolt.iidTableCids.contains(cid)) {	
			if (kvs.get("item_id") == null ){
				if (kvs.get("uuid") == null)
					return false;
				kvs.put("item_id", kvs.get("uuid"));
			}
			bIid = true;
		}
		else {
			bIid = false;
			if (kvs.get("session_id") == null)
				kvs.put("session_id", kvs.get("uuid"));
		}
	
		kvs.put("insert_time", new Date().getTime() * 1000L + 8000000L*3600);
		
		return true;
	}
	
}
