import java.io.File;
import java.io.FileInputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.BrokerHosts;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;


public class VoltdbTopology {
	
	public Properties prop = new Properties();
	
	void initConfig(){
		FileInputStream in;
		try {
			in = new FileInputStream("config.properties");
			prop.load(in);
			in.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		/*
		if(prop.containsKey("NUM_OF_THREAD")){  
	        this.threadNum = Integer.valueOf(prop.getProperty("NUM_OF_THREAD"));  
	    }
	    */
				
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub		
		VoltdbTopology tpl = new VoltdbTopology();
		tpl.initConfig();
		
		String spout_kafka_parl     = tpl.prop.getProperty("spout_kafka_parl");
        String bolt_vdb_parl 		= tpl.prop.getProperty("bolt_voltdb_parl");
        String spout_kafka_pending      = tpl.prop.getProperty("max_spout_pending");
        String num_workers            = tpl.prop.getProperty("num_workers");
        String num_ackers             = tpl.prop.getProperty("num_ackers");
        String topic				= tpl.prop.getProperty("topic");
        String usr					= tpl.prop.getProperty("voltdb.user");
        String pwd					= tpl.prop.getProperty("voltdb.pass");

        //String sleep_time             = tpl.prop.getProperty("sleep_time");

        if (spout_kafka_parl    == null ||
            num_workers         == null ||
            bolt_vdb_parl 		== null ||
            num_ackers          == null ||
            //topic          		== null ||
            spout_kafka_pending == null ||
            usr == null ||
            pwd == null) {

            System.out.println("parameters configured in config.properties is not complete.");
            return;
        }
        
        if (topic == null){
        	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        	topic = sdf.format(new Date());
        }

        TopologyBuilder builder = new TopologyBuilder();
        
        String zks = tpl.prop.getProperty("zks");
        String zkpath = tpl.prop.getProperty("zkpath");
        String zkroot = tpl.prop.getProperty("zkRoot");
        
        String servers;
		if(tpl.prop.containsKey("voltdb.servers")){  
	        servers = tpl.prop.getProperty("voltdb.servers");  
	    }
		else
			servers = "127.0.0.1";
        
        BrokerHosts brokerHosts = new ZkHosts(zks, zkpath);
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkroot, topic);
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConf.forceFromStart = true;
        
        String spout_id = "kafka_spout-"+topic;
        
        builder.setSpout(spout_id, new KafkaSpout(spoutConf), Integer.parseInt(spout_kafka_parl));
        VoltdbBolt bolt = new VoltdbBolt();
        
        File file = new File("map.json");  
        Long filelength = file.length();  
        byte[] filecontent = new byte[filelength.intValue()];   
        FileInputStream in = new FileInputStream(file);  
        in.read(filecontent);  
        in.close();   
        bolt.InitConfig(servers, usr, pwd, new String(filecontent));
        
        builder.setBolt("vdb_bolt-"+topic, bolt, Integer.parseInt(bolt_vdb_parl)).shuffleGrouping(spout_id);

        Config conf = new Config();
        conf.setMaxSpoutPending(Integer.parseInt(spout_kafka_pending));
        conf.setNumWorkers(Integer.parseInt(num_workers));
        conf.setNumAckers(Integer.parseInt(num_ackers));

        String topology_name = "Voltdb-" + topic;
        StormSubmitter.submitTopology(topology_name, conf, builder.createTopology());
	}
	
	public static boolean doTest(){
		//zs = new ZhuiShuResult();
		CommonResult rs = new CommonResult();
		Event event = new Event();//
		Request rq = new Request();
		Item item = new Item();
		News news = new News();
		String servers = "10.0.2.15";
		try{
					
			String[] addrs = servers.split(",");
			ClientConfig config = new ClientConfig("cloud", "cloud");
			config.setProcedureCallTimeout(90 * 1000);
			config.setConnectionResponseTimeout(180 * 1000);
			config.setReconnectOnConnectionLoss(true);

			Client client = ClientFactory.createClient(config);
			for (int i = 0; i < addrs.length; ++i){
				client.createConnection(addrs[i]);
			}
			
			File file = new File("map.json");  
	        Long filelength = file.length();  
	        byte[] filecontent = new byte[filelength.intValue()];   
	        FileInputStream in = new FileInputStream(file);  
	        in.read(filecontent);  
	        in.close();   
			JSONObject kfk2sql = new JSONObject(new String(filecontent)).getJSONObject("kafkatosql");
			
			// lst
			String line = "{DS.Input.All.Gmedia_13}{\"ItemType\":\"NewsBase\",\"appkey\":\"1413697c84ff002bcdddee9ede58c971\",\"cid\":\"Czhuishusq\",\"gid\":\"a0934717dec8\",\"ignore_domain\":\"1\",\"ip\":\"222.169.11.34\",\"lst\":[{\"i\":\"53fd8e05bba009835925b3e2\",\"n\":1,\"p\":0}],\"method\":\"MAddCart\",\"sid\":\"6d597522133d43b5e700b1ac0c0f4f9b\",\"timestamp\":1450368004.5990,\"uid\":\"5332fd800367e83e7a001d36\",\"uuid\":\"DS.Input:918cecf4bbd9c210:00003b53:00327c97:5672dc04\",\"v\":\"1.1.1\"}";  
			// event
			line = "{DS.Input.All.Gbudejie_m}{\"ItemType\":\"NewsBase\",\"attr\":{\"love\":\"1872\"},\"author\":[\"閲屾\"],\"callback\":\"BCore.instances[3].callbacks[0]\",\"ccnt\":171,\"cid\":\"Cbudejie_m\",\"ds\":\"wap\",\"gid\":\"882eecf4bbc243d00000638b000036a256711812\",\"iid\":\"16521732\",\"ip\":\"117.177.250.156\",\"is_newgid\":false,\"method\":\"UpdateItem\",\"p_t\":\"dt\",\"pic\":\"http://mpic.spriteapp.cn/picture/2015/1210/566930d5a729c_wpd.jpg\",\"ptime\":1449802802.0,\"random\":\"1450368009783\",\"ref_page\":\"http://c.f.winapp111.com.cn/budejie/land.php?pid=16521732\",\"sid\":\"130651785.5694482.1450358468860\",\"tag\":[\"瑙嗛\"],\"timestamp\":1450368009.0,\"title\":\"2鍒嗛挓鏁欎綘缁檌Phone 6/6p鎹釜鍙戝厜logo\",\"typ\":\"true\",\"uid\":\"13065178.1450358468860\",\"url\":\"http://c.f.winapp111.com.cn/budejie/land.php?pid=16521732\",\"user_agent\":\"Mozilla/5.0 (iPhone; CPU iPhone OS 8_1_3 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Mobile/12B466 MicroMessenger/6.3.8 NetType/WIFI Language/zh_CN\",\"uuid\":\"DS.Input:9de2782bcb754fd7:00007ad8:0010222e:5672dc09\"}";
			// result
			line = "{Rec.Result.Gbudejie}{\"__abtest__\":null,\"_ref_page_\":\"http://www.budejie.com/pc/land/?pid=16578324&from=baiduhd\",\"_spent_\":0.03854084014892578,\"_supinfo_\":\"beforesup:0|sup:0|beforesup:0|sup:2|beforesup:2|sup:1\",\"ab_bypass\":null,\"ab_name\":null,\"ab_tactics\":null,\"cid\":\"Cbudejie_pc\",\"gid\":\"970aecf4bbcd53b000004dd1000071c4561f6055\",\"p_bid\":\"34692704_8296_E862_80A7_AD70795ECB31\",\"plugins_time\":{\"history\":0.002792119979858398},\"reason\":\"mosthot:0|mosthot:30|unique:30|mosthot:30|unique:30\",\"rec_item_ids\":[\"16552533\",\"16368173\",\"16556331\"],\"req_id\":\"b605782bcb64aa4f00028ce400015d505672dc03rec_34692704_8296_E862_80A7_AD70795ECB31\",\"sid\":\"43102518.89504156.1450366712222\",\"timestamp\":1450368003.633873,\"tma\":\"43102518.76002375.1449509272074.1449509272074.1449598005574.2\",\"tmc\":\"8.43102518.6674400.1450366712224.1450367898009.1450368003074\",\"tmd\":\"107.43102518.76002375.1449509272074.\",\"uid\":\"43102518.89504156.1450366712222\",\"uuid\":\"b605782bcb64aa4f9f82000014935672dc03P\"}";
			// request
			line = "{Rec.Request.Gmedia_8}{\"_customer_type_\":0,\"_isp_\":1,\"author\":\"逐没\",\"callback\":\"BCore.instances[2].callbacks[5]\",\"cat\":\"17k小说网|仙侠武侠|奇幻修真\",\"cid\":\"C17k\",\"comb_ids\":null,\"d_s\":\"pc\",\"et\":\"NewsBase\",\"gid\":\"8d94ecf4bbcd473800006f0b00bdbbda55deb4ae\",\"iid\":\"458754\",\"ip\":\"183.19.116.127\",\"is_newgid\":\"0\",\"method\":\"rec_F28FEE67_156E_AE39_B1A5_11209B89BD97\",\"p_bid\":\"F28FEE67_156E_AE39_B1A5_11209B89BD97\",\"p_t\":\"dt\",\"pack_items\":{\"10049\":\"\",\"1017513\":\"\",\"1175189\":\"\",\"1182580\":\"\",\"1182799\":\"\",\"124484\":\"\",\"1253355\":\"\",\"1393022\":\"\",\"145886\":\"\",\"32048\":\"\",\"322331\":\"\",\"392891\":\"\",\"423646\":\"\",\"480280\":\"\",\"493868\":\"\",\"634909\":\"\",\"646521\":\"\",\"68076\":\"\",\"68765\":\"\",\"890372\":\"\",\"89114\":\"\",\"898242\":\"\",\"918742\":\"\",\"987453\":\"\"},\"random\":\"1450368003816\",\"ref_page\":\"http://www.17k.com/chapter/458754/20150937.html\",\"req_id\":\"88e1782bcb64b8300002cee000000fa55672dc03rec_F28FEE67_156E_AE39_B1A5_11209B89BD97\",\"rid\":\"88e1782bcb64b8300002cee000000fa55672dc03rec_F28FEE67_156E_AE39_B1A5_11209B89BD97\",\"sid\":\"189289195.73257097.1450367654188\",\"timestamp\":1450368003.714455,\"title\":\"少年医仙\",\"tma\":\"189289195.21668048.1445436028442.1446998632101.1449308309879.17\",\"tmc\":\"2.189289195.84264110.1450367654194.1450367654194.1450368003015\",\"tmd\":\"746.189289195.21668048.1445436028442.\",\"uid\":\"189289195.73257097.1450367654188\",\"unq\":\"1\",\"user_agent\":\"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; InfoPath.2; .NET4.0C; .NET4.0E; {D9D54F49-E51C-445e-92F2-1EE3C2313240}; GWX:RESERVED; SE 2.X MetaSr 1.0)\",\"uuid\":\"88e1782bcb64b8301aa70000dba15672dc03P\"}";
			
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
							return true;
						}
						
						if (item_type.compareTo("ItemBase") == 0){ //Item
							if (item.LoadFromJson(kfk2sql, inputJSONObject))
								item.WriteToVoltdb(client);
						}
						else if (item_type.compareTo("NewsBase") == 0){ // News
							if (news.LoadFromJson(kfk2sql, inputJSONObject))
								news.WriteToVoltdb(client);
						}
					}
					
					else if (method.compareTo("RmItem") == 0 || 
							method.compareTo("MRmItem") == 0 ) {
							if (!inputJSONObject.has("iid")){
								return true;
							}
							
							if (item_type.compareTo("ItemBase") == 0){ //Item							
								item.Update_active(client, inputJSONObject.getString("iid"));
							}
							else if (item_type.compareTo("NewsBase") == 0){ // News
								news.Update_active(client, inputJSONObject.getString("iid"));
							}
					}
					
					else if (method.compareTo("RmNews") == 0 ) {
							if (!inputJSONObject.has("iid")){
								return true;
							}						
							news.Update_active(client, inputJSONObject.getString("iid"));
					}
					
					boolean isLst = false;				
					if (method.compareTo("AddCart") == 0 || method.compareTo("MAddCart") == 0 ||
						method.compareTo("MOrder") == 0 || method.compareTo("MPay") == 0  ||
						method.compareTo("Order") == 0  || method.compareTo("Buy") == 0 ||
						method.compareTo("Pay") == 0 || method.compareTo("Commit") == 0 ||
						method.compareTo("Cart") == 0 ){
						
						if (!inputJSONObject.has("lst") || !(inputJSONObject.get("lst") instanceof JSONArray)){
							return true;
						}
						isLst  = true;
					}
					
					event.SetCid(line.substring("{DS.Input.All.".length(), line.indexOf("}{")));
									
					if (event.LoadFromJson(kfk2sql, inputJSONObject))
						event.WriteToVoltdb(client);
					else{
						return true;
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
							event.WriteToVoltdb(client, iid, quantity, price);
						}
					}
				
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			/*
			else if (line.startsWith("{Rec.Result.Gmedia_13}")){
				if (zs.LoadFromJson(json))
					zs.WriteToVoltdb(client);			
			}
			*/
			else if (line.startsWith("{Rec.Request.")){
				if (rq.LoadFromJson(kfk2sql, inputJSONObject))
					rq.WriteToVoltdb(client);
			}
			else if (line.startsWith("{Rec.Result.")){
				if (rs.LoadFromJson(kfk2sql, inputJSONObject))
					rs.WriteToVoltdb(client);								 				 
			}
		}
		catch (Exception e){
			e.printStackTrace();
		}
		return true;
	}

}
