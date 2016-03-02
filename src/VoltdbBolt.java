
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.voltdb.client.*;

import java.util.HashSet;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;

public class VoltdbBolt implements IRichBolt {
	
	public int taskId;
	private OutputCollector collector;
	private Client client = null;
	private CommonResult rs;
	private Event event;
	private Request rq;
	private Item item;
	private News news;
	
	private String servers;
	private String user;
	private String pass;
	private String mapcfg;
	private JSONObject kfk2sql;
	
	public HashSet<String> iidTableCids = new HashSet<String>();
	
	public void InitConfig(String svs, String user, String pass, String map_context, String iidCids){
		this.servers = svs;
		this.user = user;
		this.pass = pass;
		this.mapcfg = map_context;
		String [] cids = iidCids.split(",");
		
		for(int i=0; i<cids.length; ++i)
			iidTableCids.add(cids[i]);
	}
	
	public void cleanup() {
		// TODO Auto-generated method stub
			try {
				this.client.close();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

	}


	public void execute(Tuple arg0) {
		// TODO Auto-generated method stub
		String line = arg0.getString(0);  
		line = line.replace(":true,", ":1,");
		line = line.replace(":false,", ":0,");
		//System.err.println("execute task id:" + taskId);
		
		String json= line.substring(line.indexOf('}') + 1);
		JSONObject inputJSONObject = null;
		try {
			inputJSONObject = new JSONObject(json);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (line.startsWith("{DS.Input.")){
			String method = null;
			String item_type = null;
			try {
				if (inputJSONObject.has("method")){
					method = (inputJSONObject.getString("method"));				
				}
							
				if (inputJSONObject.has("ItemType")){
					item_type = (inputJSONObject.getString("ItemType"));
				}
				else{
					item_type = "ItemBase"; //default value
				}
				
				if (method.compareTo("AddItem") == 0 || 
					method.compareTo("UpdateItem") == 0  || 
					method.compareTo("MAddItem") == 0 ) {
					if (!inputJSONObject.has("iid")){
						collector.ack(arg0);
						return;
					}
					
					if (item_type.compareTo("ItemBase") == 0){ //Item
						if (item.LoadFromJson(kfk2sql, inputJSONObject))
							//item.WriteToVoltdb(client);
							if (!item.WriteToVoltdb(client))
								System.err.println(json);
					}
					else if (item_type.compareTo("NewsBase") == 0){ // News
						if (news.LoadFromJson(kfk2sql, inputJSONObject))
							//news.WriteToVoltdb(client);
							if (!news.WriteToVoltdb(client))
								System.err.println(json);
					}
				}
				
				else if (method.compareTo("RmItem") == 0 || 
						method.compareTo("MRmItem") == 0 ) {
						if (!inputJSONObject.has("iid")){
							collector.ack(arg0);
							return;
						}
						
						if (item_type.compareTo("ItemBase") == 0){ //Item							
							//item.Update_active(client, inputJSONObject.getString("iid"));
							if(!item.Update_active(client, inputJSONObject.getString("iid")))
								System.err.println(json);
						}
						else if (item_type.compareTo("NewsBase") == 0){ // News
							//news.Update_active(client, inputJSONObject.getString("iid"));
							if(!news.Update_active(client, inputJSONObject.getString("iid")))
								System.err.println(json);
						}
				}
				
				else if (method.compareTo("RmNews") == 0 ) {
						if (!inputJSONObject.has("iid")){
							collector.ack(arg0);
							return;
						}						
						//news.Update_active(client, inputJSONObject.getString("iid"));
						if(!news.Update_active(client, inputJSONObject.getString("iid")))
							System.err.println(json);
				}
				
				boolean isLst = false;				
				if (method.compareTo("AddCart") == 0 || method.compareTo("MAddCart") == 0 ||
					method.compareTo("MOrder") == 0 || method.compareTo("MPay") == 0  ||
					method.compareTo("Order") == 0  || method.compareTo("Buy") == 0 ||
					method.compareTo("Pay") == 0 || method.compareTo("Commit") == 0 ||
					method.compareTo("Cart") == 0 ){
					
					if (!inputJSONObject.has("lst") || !(inputJSONObject.get("lst") instanceof JSONArray)){
						collector.ack(arg0);
						return;
					}
					isLst  = true;
				}
				
				//event.SetCid(line.substring("{DS.Input.All.".length(), line.indexOf("}{")));
				
				if (event.LoadFromJson(kfk2sql, inputJSONObject))
					//event.WriteToVoltdb(client);
					if (!event.WriteToVoltdb(client))
						System.err.println(json);
				else{
					collector.ack(arg0);
					return;
				}
				
				if (isLst){
					
					JSONArray lst = inputJSONObject.getJSONArray("lst");
					int len = lst.length();
					for (int i=0; i<len; ++i){
						
						String iid = null;
						Double price = null;
						Long quantity = null;
						if(lst.get(i) instanceof JSONArray){
							JSONArray elem = lst.getJSONArray(i);
							iid = elem.getString(0);
							quantity = elem.getLong(1);
							price = elem.getDouble(2);
						}
						else{
							JSONObject elem = lst.getJSONObject(i);
							if (elem.has("i"))
								iid = elem.getString("i");
							if (elem.has("n"))
								quantity = elem.getLong("n");
							if (elem.has("p"))
								price = elem.getDouble("p");
						}
						//event.WriteToVoltdb(client, iid, quantity, price);
						if (!event.WriteToVoltdb(client, iid, quantity, price))
							System.err.println(json);
					}
				}
			
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.err.println(json);
			}
			
		}
		else if (line.startsWith("{Rec.Request.")){
			try {
				if (rq.LoadFromJson(kfk2sql, inputJSONObject))
					//rq.WriteToVoltdb(client);
					if (!rq.WriteToVoltdb(client))
						System.err.println(json);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.err.println(json);
			}
		}
		else if (line.startsWith("{Rec.Result.")){
			try {
				if (rs.LoadFromJson(kfk2sql, inputJSONObject))
					//rs.WriteToVoltdb(client);
					if (!rs.WriteToVoltdb(client))
						System.err.println(json);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.err.println(json);
			}
		}
		else if (line.startsWith("{Cfg.Zookeeper")){
			String[] table_iid = inputJSONObject.optString("table_iid").split(",");
			iidTableCids.clear();
			for(int i=0; i < table_iid.length; ++i)
				iidTableCids.add(table_iid[i]);
		}
				
        //collector.emit(arg0, new Values(val*2, val*3));
        collector.ack(arg0);
        
	}


	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub
		this.collector = arg2;
		rs = new CommonResult(this);
		event = new Event(this);
		rq = new Request(this);
		item = new Item();
		news = new News();
		taskId = arg1.getThisTaskId();
		try{
					
			String[] addrs = servers.split(",");
			ClientConfig config = new ClientConfig(user, pass);
			config.setProcedureCallTimeout(90 * 1000);
			config.setConnectionResponseTimeout(180 * 1000);
			config.setReconnectOnConnectionLoss(true);

			client = ClientFactory.createClient(config);
			for (int i = 0; i < addrs.length; ++i){
				client.createConnection(addrs[i]);
			}
			
			kfk2sql = new JSONObject(mapcfg).getJSONObject("kafkatosql");
			
		} catch (Exception e) {
			System.out.println("prepare failed");
			e.printStackTrace();
		}

	}


	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		arg0.declare(new Fields("voltdb_key", "voltdb_details"));
	}


	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}


	
	
}
