import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Iterator;

import org.json.JSONArray;
import org.json.JSONException;  
import org.json.JSONObject;

public class ConvertJsonToMap {
	public static void Convert(HashMap<String, Object> out, JSONObject kfk2sql, String table, JSONObject json) throws JSONException{
		
		JSONObject columns;
		//try {			
			columns = kfk2sql.getJSONObject(table);
			Iterator it = columns.keys();  
	        while(it.hasNext()) {  
	            String key = (String)it.next(); 
	            JSONArray array = columns.getJSONArray(key);
	            String colName = array.getString(0);
	            String colType = array.getString(1);
	            int len = -1;
	            if (array.length() > 3)
	            	len = array.getInt(3);
	            if (json.has(key) && out.containsKey(colName)){
	            	if (colType.compareTo("INFOS") ==0 || colType.compareTo("INFOSD") == 0){ // string => varchar

            			String val = json.optString(key);
            			if (val.compareTo("null") != 0){
	            			if (len > 0 && val.length() > len)
	            				val = val.substring(0, len-1);
	            			out.put(colName, val);
            			}
	            		
	            		
	            	}
	            	else if (colType.compareTo("INFOJ") ==0){ // json array / obj? => varchar

	            		StringBuffer val = new StringBuffer();	            	
	            		if (json.get(key) instanceof JSONArray){
	            			if(len < 0)
	            				out.put(colName, json.getJSONArray(key).toString());
	            			else{
		            			JSONArray arr = json.getJSONArray(key);
		            			int curlen = 2;
		            			val.append("[");
		            			for(int i=0; i<arr.length(); ++i){		            				
		            				String v = arr.getString(i);
		            				if (len < curlen + 3 + v.length())
		            					break;
		            				if (i != 0){
		            					val.append(",");
		            				}
		            				val.append("\"");
		            				val.append(v);
		            				val.append("\"");
		            				curlen = curlen + 3 + v.length();
		            			}
		            			val.append("]");
		            			out.put(colName, val.toString());
	            			}
	            			
	            		}
	            		else {
	            			JSONObject o = json.optJSONObject(key);
	            			if (o != null){
	            			val.append(o.toString());
	            			if (len > 0 && val.length() > len)
		            			out.put(colName, val.substring(0, len-1));
	            			else
	            				out.put(colName, val.toString());
	            			}
	            		}
	            	}
	            	else if (colType.compareTo("INFOJI") ==0){ // json array [int,int] / obj? => varchar

	            		StringBuffer val = new StringBuffer();	            	
	            		if (json.get(key) instanceof JSONArray){
	            			if(len < 0)
	            				out.put(colName, json.getJSONArray(key).toString());
	            			else{
	            				val.append("[");
		            			JSONArray arr = json.getJSONArray(key);
		            			int curlen = 2;
		            			for(int i=0; i<arr.length(); ++i){		            				
		            				String v = Long.toString(arr.getLong(i));
		            				if (len < curlen + 1 + v.length())
		            					break;
		            				if (i != 0){
		            					val.append(",");
		            				}
		            				val.append(v);
		            				curlen = curlen +  v.length() + 1;
		            			}
		            			val.append("]");
		            			out.put(colName, val.toString());
	            			}
	            			
	            		}
	            		else {
	            			JSONObject o = json.optJSONObject(key);
	            			if (o != null){
	            			val.append(o.toString());
	            			if (len > 0 && val.length() > len)
		            			out.put(colName, val.substring(0, len-1));
	            			else
	            				out.put(colName, val.toString());
	            			}
	            		}
	            	}
	            	else if (colType.compareTo("INFOTFD") ==0 || colType.compareTo("INFOTF") ==0 ||
	            			colType.compareTo("INFOTD") ==0 || colType.compareTo("INFOT") ==0 ){ // double float  =>timestamp
	            			out.put(colName, Math.round(json.getDouble(key) * 1000000L) + 8000000L * 3600);
	            	}
					else if (colType.compareTo("INFOI") ==0 || colType.compareTo("INFOID") ==0 ){ // integer
							out.put(colName, json.getLong(key));
					}
					else if (colType.compareTo("INFOD") ==0 || colType.compareTo("INFODD") ==0){ // double
							out.put(colName, json.getDouble(key));
					}
					else if (colType.compareTo("INFOB") ==0 || colType.compareTo("INFOBD") ==0){ // boolean						
						out.put(colName, json.getInt(key));
						
					}
					else if (colType.compareTo("INFOSorJ") ==0){
						if (json.get(key) instanceof String)
	            			out.put(colName, json.getString(key));
						else if (json.get(key) instanceof JSONArray)
							out.put(colName, json.getJSONArray(key).toString());
					}
	            	
	            }
	        }
	        
		//} catch (JSONException e) {
			// TODO Auto-generated catch block
		//	e.printStackTrace();
		//}
	}
}
