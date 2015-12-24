
import org.voltdb.client.Client;

import java.util.HashMap;

import org.json.JSONObject;

public interface JsonToVoltdb {
	void Cleanup();
	boolean WriteToVoltdb(Client client);
	boolean LoadFromJson(JSONObject root);
	boolean LoadFromJson(JSONObject kfk2sql, JSONObject json);
}
