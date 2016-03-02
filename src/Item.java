import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import org.json.JSONException;
import org.json.JSONObject;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;

public class Item implements JsonToVoltdb {
	
	String[] keys = new String[]{
			"item_id", "item_name", "item_link", "cid", "image_link", 
			"simage_link", "price", "price_str", "market_price", 
			"member_price", "vip_price", "discount", "start_time", 
			"end_time", "expire_time", "category", "category_id", 
			"brand", "brand_id", "color", "size", "stock", "gender", 
			"description", "campaign", "material", "style", 
			"production_place", " weight", "group_id", "type", 
			"location", "business_circle", "tag", "attr", "author", 
			"address", "is_active", "goods", "creation_time", "insert_time", 
			"stock_count", "unit_price", "housing_type", "direction", 
			"area", "decoration", "floor", "total_floor", "building_time", 
			"owner", "phone_number", "residential_area", "housing_features", 
			"area_features", "post_time", "property_type", "source", "sale_time", 
			"checkin_time", "housing_types", "property_time", "property_company", 
			"property_fee", "building_no", "housing_configuration", "building_type", 
			"rent_type", "school_type", "school_level", "school_prop", "adjacent_school", 
			"view_count", "rise_rate", "sale_count", "rent_count", "subtitle", "goods_type", 
			"rating_count", "rating", "buy_count", "seller_name", "seller_no", "seller_link", 
			"iusage", "coord", "days", "travelers", "departure", "destination", "sights", 
			"star", "traffic", "season", "place", "style_1", "pattern"
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
			   
			   ClientResponse resp = client.callProcedure("ITEM.upsert", 
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
					   kvs.get(keys[85]),kvs.get(keys[86]),kvs.get(keys[87]),kvs.get(keys[88]),kvs.get(keys[89]),
					   kvs.get(keys[90]),kvs.get(keys[91]),kvs.get(keys[92]),kvs.get(keys[93]),kvs.get(keys[94]),
					   kvs.get(keys[95]),kvs.get(keys[96])
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
			   ClientResponse resp = client.callProcedure("ITEM_non_active", iid);			   
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
		ConvertJsonToMap.Convert(kvs, kfk2sql, "Item", json);
		if (kvs.get("item_id") == null || kvs.get("item_name") == null ||
			kvs.get("item_link") == null || kvs.get("creation_time") == null)
			return false;
		kvs.put("insert_time", new Date().getTime() * 1000L + 8000000L*3600);
		kvs.put("is_active", 1);
		
		return true;
	}

}
