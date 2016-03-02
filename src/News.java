import java.util.Date;
import java.util.HashMap;

import org.json.JSONException;
import org.json.JSONObject;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;

public class News implements JsonToVoltdb {

	String[] keys = new String[]{
			"item_id", "cid", "keywords", "location", "title", "type", "url", "author", 
			"abstract", "post_time", "uid", "category", "tag", "pic", "view_count", 
			"comment_count", "attr", "expire_time", "is_active", "creation_time", 
			"insert_time", "address", "worktype", "minsalary", "maxsalary", 
			"workyear", "diploma", "jobdescription", "desired_skills", 
			"industry", "companysize", "companyname", "companyurl", "companystage", 
			"companytype", "Language", "Management", "applicantsnum", "headcount", 
			"jobstemptation", "end_time", "founder", "showsalary", "pagetype"
		};

	HashMap<String, Object> kvs = new HashMap<String, Object>();
		
	public void Cleanup() {
		// TODO Auto-generated method stub
		for(int i = 0; i < keys.length; ++i)
			kvs.put(keys[i], null);
	}

	public boolean WriteToVoltdb(Client client) {
		// TODO Auto-generated method stub
		 try { 	
			   ClientResponse resp = client.callProcedure("News.upsert", 
					   kvs.get(keys[0]),kvs.get(keys[1]),kvs.get(keys[2]),kvs.get(keys[3]),kvs.get(keys[4]),
					   kvs.get(keys[5]),kvs.get(keys[6]),kvs.get(keys[7]),kvs.get(keys[8]),kvs.get(keys[9]),
					   kvs.get(keys[10]),kvs.get(keys[11]),kvs.get(keys[12]),kvs.get(keys[13]),kvs.get(keys[14]),
					   kvs.get(keys[15]),kvs.get(keys[16]),kvs.get(keys[17]),kvs.get(keys[18]),kvs.get(keys[19]),
					   kvs.get(keys[20]),kvs.get(keys[21]),kvs.get(keys[22]),kvs.get(keys[23]),kvs.get(keys[24]),
					   kvs.get(keys[25]),kvs.get(keys[26]),kvs.get(keys[27]),kvs.get(keys[28]),kvs.get(keys[29]),
					   kvs.get(keys[30]),kvs.get(keys[31]),kvs.get(keys[32]),kvs.get(keys[33]),kvs.get(keys[34]),
					   kvs.get(keys[35]),kvs.get(keys[36]),kvs.get(keys[37]),kvs.get(keys[38]),kvs.get(keys[39]),
					   kvs.get(keys[40]),kvs.get(keys[41]),kvs.get(keys[42]),kvs.get(keys[43])
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

	public boolean Update_active(Client client, String iid) {
		// TODO Auto-generated method stub
		   try { 	
			   ClientResponse resp = client.callProcedure("NEWS_non_active", iid);			   
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
		// TODO Auto-generated method stub
		Cleanup();
		ConvertJsonToMap.Convert(kvs, kfk2sql, "News", json);
		if (kvs.get("item_id") == null || kvs.get("creation_time") == null)
			return false;
		kvs.put("insert_time", new Date().getTime() * 1000L + 8000000L*3600);
		return true;
	}

}
