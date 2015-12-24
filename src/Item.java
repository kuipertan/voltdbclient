import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import org.json.JSONException;
import org.json.JSONObject;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;

public class Item implements JsonToVoltdb {
	/*
	public String item_id;
	public String item_name;
	public String item_link;
	public String image_link;
	public String simage_link;
	public Double price;
	public String price_str;
	public Double market_price;
	public Double member_price;
	public Double vip_price;
	public Double discount;
	public Long start_time;
	public Long end_time;
	public Long expire_time;
	public String category;
	public String category_id;
	public String brand;
	public String brand_id;
	public String color;
	public String size;
	public String stock;
	public String gender;
	public String description;
	public String campaign;
	public String material;
	public String style;
	public String production_place;
	public String weight;
	public String group_id;
	public String type;
	public String location;
	public String business_circle;
	public String tag;
	public String attr;
	public String author;
	public String address;
	public Integer is_active;
	public String goods;
	public Long creation_time;
	public Long insert_time;
	public Long stock_count;
	public Double unit_price;
	public String housing_type;
	public String direction;
	public Double area;
	public String decoration;
	public Long floor;
	public Long total_floor;
	public String building_time;
	public String owner;
	public String phone_number;
	public String residential_area;
	public String housing_features;
	public String area_features;
	public String post_time;
	public String property_type;
	public String source;
	public String sale_time;
	public String checkin_time;
	public String housing_types;
	public Long property_time;
	public String property_company;
	public Double property_fee;
	public String building_no;
	public String housing_configuration;
	public String building_type;
	public String rent_type;
	public String school_type;
	public String school_level;
	public String school_prop;
	public String adjacent_school;
	public Long view_count;
	public Double rise_rate;
	public Long sale_count;
	public Long rent_count;
	public String subtitle;
	public String goods_type;
	public Long rating_count;
	public Double rating;
	public Long buy_count;
	public String seller_name;
	public String seller_no;
	public String seller_link;
	public String iusage;
	public String coord;
	public Long days;
	public Long travelers;
	public String departure;
	public String destination;
	public String sights;
	public Integer star ;
	public String traffic;
	public String season;
	public String place;
	public String style_1;
	public String pattern;
*/
	String[] keys = new String[]{
			"item_id", "item_name", "item_link", "image_link", 
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
		/*
		item_id = null;
		item_name = null;
		item_link = null;
		image_link = null;
		simage_link = null;
		price = null;
		price_str = null;
		market_price = null;
		member_price = null;
		vip_price = null;
		discount = null;
		start_time = null;
		end_time = null;
		expire_time = null;
		category = null;
		category_id = null;
		brand = null;
		brand_id = null;
		color = null;
		size = null;
		stock = null;
		gender = null;
		description = null;
		campaign = null;
		material = null;
		style = null;
		production_place = null;
		weight = null;
		group_id = null;
		type = null;
		location = null;
		business_circle = null;
		tag = null;
		attr = null;
		author = null;
		address = null;
		is_active = null;
		goods = null;
		creation_time = null;
		insert_time = null;
		stock_count = null;
		unit_price = null;
		housing_type = null;
		direction = null;
		area = null;
		decoration = null;
		floor = null;
		total_floor = null;
		building_time = null;
		owner = null;
		phone_number = null;
		residential_area = null;
		housing_features = null;
		area_features = null;
		post_time = null;
		property_type = null;
		source = null;
		sale_time = null;
		checkin_time = null;
		housing_types = null;
		property_time = null;
		property_company = null;
		property_fee = null;
		building_no = null;
		housing_configuration = null;
		building_type = null;
		rent_type = null;
		school_type = null;
		school_level = null;
		school_prop = null;
		adjacent_school = null;
		view_count = null;
		rise_rate = null;
		sale_count = null;
		rent_count = null;
		subtitle = null;
		goods_type = null;
		rating_count = null;
		rating = null;
		buy_count = null;
		seller_name = null;
		seller_no = null;
		seller_link = null;
		iusage = null;
		coord = null;
		days = null;
		travelers = null;
		departure = null;
		destination = null;
		sights = null;
		star  = null;
		traffic = null;
		season = null;
		place = null;
		style_1 = null;
		pattern = null;
		*/
		
		for(int i = 0; i < keys.length; ++i)
			kvs.put(keys[i], null);
	}

	public boolean WriteToVoltdb(Client client) {
		// TODO Auto-generated method stub
		   try { 	
			   /*
			   ClientResponse resp = client.callProcedure("REQUEST_upsert", 
					   item_id, item_name, item_link, image_link, simage_link, 
					   price, price_str, market_price, member_price, vip_price, 
					   discount, start_time, end_time, expire_time, category, 
					   category_id, brand, brand_id, color, size, stock, gender, 
					   description, campaign, material, style, production_place, 
					   weight, group_id, type, location, business_circle, tag, 
					   attr, author, address, is_active, goods, creation_time, 
					   insert_time, stock_count, unit_price, housing_type, 
					   direction, area, decoration, floor, total_floor, building_time, 
					   owner, phone_number, residential_area, housing_features, 
					   area_features, post_time, property_type, source, sale_time, 
					   checkin_time, housing_types, property_time, property_company, 
					   property_fee, building_no, housing_configuration, building_type, 
					   rent_type, school_type, school_level, school_prop, adjacent_school, 
					   view_count, rise_rate, sale_count, rent_count, subtitle, goods_type, 
					   rating_count, rating, buy_count, seller_name, seller_no, seller_link, 
					   iusage, coord, days, travelers, departure, destination, sights, star, 
					   traffic, season, place, style_1, pattern
   						);
   						*/
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
					   kvs.get(keys[95])
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
	
	public boolean LoadFromJson(JSONObject kfk2sql, JSONObject json) {
		Cleanup();
		ConvertJsonToMap.Convert(kvs, kfk2sql, "Item", json);
		if (kvs.get("item_id") == null || kvs.get("item_name") == null ||
			kvs.get("item_link") == null || kvs.get("creation_time") == null)
			return false;
		kvs.put("insert_time", new Date().getTime() * 1000L + 8000000L*3600);
		kvs.put("is_active", 1);
		
		return true;
	}

	public boolean LoadFromJson(JSONObject inputJSONObject) {
		// TODO Auto-generated method stub
		Cleanup();
		
    	try{
    		    	
			if (!inputJSONObject.has("timestamp") || !inputJSONObject.has("iid")){       						
				return false;
			}
			
			inputJSONObject.getString("iid");
			/*	   			
			if (inputJSONObject.has("name")){
				item_name = (inputJSONObject.getString("name"));
			}
			
			if (inputJSONObject.has("url") ){
				item_link = inputJSONObject.getString("url").replace("\\", "\\\\");
			}
			
			if (inputJSONObject.has("img")){
				image_link = (inputJSONObject.getString("img"));
			}
			
			if (inputJSONObject.has("simg")){
				simage_link = (inputJSONObject.getString("simg"));
			}
			   
			if (inputJSONObject.has("prc")){
				price = (inputJSONObject.getDouble("prc"));
			}
			
			if (inputJSONObject.has("prc_s")){
				price_str = (inputJSONObject.getString("prc_s"));
			}
			
			if (inputJSONObject.has("mktp")){
				market_price = (inputJSONObject.getDouble("mktp"));
			}
			
			if (inputJSONObject.has("memp")){
				member_price = (inputJSONObject.getDouble("memp"));
			}
			
			if (inputJSONObject.has("vipp")){
				vip_price = (inputJSONObject.getDouble("vipp"));
			}
			
			if (inputJSONObject.has("dis")){
				discount = (inputJSONObject.getDouble("dis"));
			}
			
			if (inputJSONObject.has("dis")){
				start_time = (inputJSONObject.getDouble("dis"));
			}
			*/
			
					  
        } catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}   	
    	return true;		

	}

}
