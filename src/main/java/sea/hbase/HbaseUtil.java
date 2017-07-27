package sea.hbase;

import java.io.IOException;

import java.util.HashMap;

import java.util.Iterator;

import java.util.Map;

import java.util.Map.Entry;

//import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.HColumnDescriptor;

import org.apache.hadoop.hbase.HTableDescriptor;

import org.apache.hadoop.hbase.KeyValue;

import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.Admin;

import org.apache.hadoop.hbase.client.Connection;

import org.apache.hadoop.hbase.client.ConnectionFactory;

import org.apache.hadoop.hbase.client.Get;

import org.apache.hadoop.hbase.client.Put;

import org.apache.hadoop.hbase.client.Result;

import org.apache.hadoop.hbase.client.ResultScanner;

import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.client.Table;

import org.apache.hadoop.hbase.util.Bytes;

public class HbaseUtil {

	private static String SERIES = "s";

	private static String TABLENAME = "duantTest";

	private static Connection conn;

	private static String hbaseIp = "192.168.1.250";

	public static void init() {

		Configuration config = HBaseConfiguration.create();
		
		config.set("hbase.zookeeper.quorum", "192.168.1.250:2181");
		config.set("zookeeper.znode.parent", "/hbase13");
		
		
		config.set("hbase.client.write.buffer", "62914560");
		config.set("hbase.client.pause", "1000");
		config.set("hbase.client.retries.number", "10");
		config.set("hbase.security.authentication", "simple");
		config.set("hbase.rpc.timeout", "6000");
		System.setProperty("hadoop.home.dir", "D:\\vm\\linux\\hadoop");
		//config.addResource("/hbase-site.xml");

		try {

			conn = ConnectionFactory.createConnection(config);
			

			//createTable(TABLENAME, SERIES);

		} catch (IOException e) {

			e.printStackTrace();

		}

	}

	public static void createTable(String tableName, String seriesStr) throws IllegalArgumentException, IOException {

		Admin admin = null;

		TableName table = TableName.valueOf(tableName);

		try {

			admin = conn.getAdmin();

			//if (!admin.tableExists(table)) {

				System.out.println(tableName + " table not Exists");

				HTableDescriptor descriptor = new HTableDescriptor(table);

				String[] series = seriesStr.split(",");

				for (String s : series) {

					descriptor.addFamily(new HColumnDescriptor(s.getBytes()));

				}

				admin.createTable(descriptor);

			//}

		} finally {

			//IOUtils.closeQuietly(admin);

		}

	}

	/*public static void add(String rowKey, Map<String, Object> columns) throws IOException {

		Table table = null;

		try {

			table = conn.getTable(TableName.valueOf(TABLENAME));

			Put put = new Put(Bytes.toBytes(rowKey));

			for (Map.Entry<String, Object> entry : columns.entrySet()) {

				put.addColumn(SERIES.getBytes(), Bytes.toBytes(entry.getKey()),

						new ObjectAndByte().toByteArray(entry.getValue()));

			}

			table.put(put);

		} finally {

			IOUtils.closeQuietly(table);

		}

	}*/
	
	public static void addData(String rowKey, String name) throws IOException {
		Table table = null;

		try {

			table = conn.getTable(TableName.valueOf(TABLENAME));

			Put put = new Put(Bytes.toBytes(rowKey));

			
				put.addColumn("name".getBytes(), "name1".getBytes(),

						name.getBytes());

			table.put(put);

		} finally {

			//IOUtils.closeQuietly(table);

		}
	}

	public static Map<String, String> getAllValue(String rowKey) throws IllegalArgumentException, IOException {

		Table table = null;

		Map<String, String> resultMap = null;

		try {

			table = conn.getTable(TableName.valueOf(TABLENAME));

			Get get = new Get(Bytes.toBytes(rowKey));

			get.addFamily(SERIES.getBytes());

			Result res = table.get(get);

			Map<byte[], byte[]> result = res.getFamilyMap(SERIES.getBytes());

			Iterator<Entry<byte[], byte[]>> it = result.entrySet().iterator();

			resultMap = new HashMap<String, String>();

			while (it.hasNext()) {

				Entry<byte[], byte[]> entry = it.next();

				resultMap.put(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));

			}

		} finally {

			//IOUtils.closeQuietly(table);

		}

		return resultMap;

	}

	// rowkey and column get data

	public static String getValueBySeries(String rowKey, String column) throws IllegalArgumentException, IOException {

		Table table = null;

		String resultStr = null;

		try {

			table = conn.getTable(TableName.valueOf(TABLENAME));

			Get get = new Get(Bytes.toBytes(rowKey));

			get.addColumn(Bytes.toBytes(SERIES), Bytes.toBytes(column));

			Result res = table.get(get);

			byte[] result = res.getValue(Bytes.toBytes(SERIES), Bytes.toBytes(column));

			resultStr = Bytes.toString(result);

		} finally {

			//IOUtils.closeQuietly(table);

		}

		return resultStr;

	}

	public static void getValueByTable() throws Exception {

		Map<String, String> resultMap = null;

		Table table = null;

		try {

			table = conn.getTable(TableName.valueOf(TABLENAME));

			ResultScanner rs = table.getScanner(new Scan());

			for (Result r : rs) {

				System.out.println("获得到rowkey:" + new String(r.getRow()));

				for (KeyValue keyValue : r.raw()) {

					System.out.println(

							"列：" + new String(keyValue.getFamily()) + "====值:" + new String(keyValue.getValue()));

				}

			}

		} finally {

			//IOUtils.closeQuietly(table);

		}

	}

	public static void dropTable(String tableName) throws IOException {

		Admin admin = null;

		TableName table = TableName.valueOf(tableName);

		try {

			admin = conn.getAdmin();

			if (admin.tableExists(table)) {

				admin.disableTable(table);

				admin.deleteTable(table);

			}

		} finally {

			//IOUtils.closeQuietly(admin);

		}

	}

	public static void main(String[] args) throws Exception {

		init();
		//createTable("duantTest3","name,address");
		//addData("10","duant");
		getValueByTable();

		//createTable("duantTest2", "age,name,test");

		/*String rowKey1 = "u1001c1001";

		Map<String, Object> columns = new HashMap<String, Object>();

		columns.put("original_data", "original_data_u1001c1001_1");

		columns.put("original_data", "original_data_u1001c1001_2");

		add(rowKey1, columns);

		String rowKey2 = "u1001c1001";

		Map<String, Object> columns2 = new HashMap<String, Object>();

		columns2.put("original_data", "original_data_u1001c1002_1");

		columns2.put("original_data", "original_data_u1001c1002_2");

		add(rowKey2, columns2);

		// query data 1-1

		Map<String, String> map1 = getAllValue(rowKey1);

		for (Map.Entry<String, String> entry : map1.entrySet()) {

			System.out.println("map1-" + entry.getKey() + ":" + entry.getValue());

		}

		// query data 1-2

		Map<String, String> map2 = getAllValue(rowKey2);

		for (Map.Entry<String, String> entry : map2.entrySet()) {

			System.out.println("map2-" + entry.getKey() + ":" + entry.getValue());

		}

		// query data 2

		String original_data_value = getValueBySeries(rowKey1, "original_data");

		System.out.println("original_data_value->" + original_data_value);

		// query data

		getValueByTable();
*/
	}
}