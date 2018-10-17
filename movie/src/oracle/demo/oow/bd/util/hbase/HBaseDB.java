package oracle.demo.oow.bd.util.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseDB {
	private static final HBaseDB single = new HBaseDB();
	private static Connection conn;
	// 鍗曚緥鑾峰彇璇ョ被鐨勫璞�

	private HBaseDB() {
		Configuration conf = HBaseConfiguration.create();
		//zookeeper鐨勪富鏈哄湴鍧�锛岃櫄鎷熸満鐨勪富鏈哄悕
		conf.set("hbase.zookeeper.quorum", ConstantsHBase.ZOOKEEPER);
		conf.set("hbase.rootdir", ConstantsHBase.HBASE_ROOT_DIR);
		
		try {
			conn = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// 闈欐�佸伐鍘傛柟娉�
	public static HBaseDB getInstance() {
		return single;
	}
	
	private Connection getConn() {
		return conn;
	}
	
	/**
	 * 鍒涘缓hbase琛�
	 * @param tableName  琛ㄥ悕绉�
	 * @param columnFamilies  鍒楁棌锛屽彲鑳芥湁澶氫釜
	 * @param version锛屽垪鏃忓瓨鍌ㄧ殑鐗堟湰鏁伴噺
	 */
	public void createTable(String tableName, String[] columnFamilies, int version){
		try {
			deleteTable(tableName);
			
			Connection conn = getConn();
			Admin admin = conn.getAdmin();
			//鍒涘缓琛ㄦ弿杩帮紝鎸囧畾琛ㄥ悕绉�
			HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
			
			for (String str : columnFamilies) {
				//鎶婂悇涓垪鏃忔坊鍔犲埌desc
				HColumnDescriptor columnDescriptor = new HColumnDescriptor(str);
				columnDescriptor.setMaxVersions(version);
				//娣诲姞鍒楁棌
				desc.addFamily(columnDescriptor);
			}
			admin.createTable(desc);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 鍒犻櫎琛紝鍏堟煡璇㈣琛ㄦ槸鍚﹀瓨鍦�
	 * @param tableName
	 */
	public void deleteTable(String tableName){
		try {
			Connection conn = getConn();
			Admin admin = conn.getAdmin();
			if(admin.tableExists(TableName.valueOf(tableName))) {
				admin.disableTable(TableName.valueOf(tableName));
				admin.deleteTable(TableName.valueOf(tableName));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 鏍规嵁table鍚嶇О鑾峰彇table瀵硅薄
	 * @param tableName
	 * @return
	 */
	public Table getTable(String tableName) {
		Table table = null;
		try {
			Connection conn = getConn();
			table = conn.getTable(TableName.valueOf(tableName));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return table;
	}
	
	/**
	 * 鏍规嵁璁℃暟鍣ㄦ潵鑾峰彇鍏朵粬琛ㄧ殑琛屽仴
	 * 璁℃暟鍣ㄨ〃鏄凡缁忓垱寤哄ソ鐨勶紝琛屽仴锛屽垪鏃忥紝鍒楅兘鏄凡鐭ャ��
	 * 
	 * @param tableName
	 * @param family
	 * @param qualifier
	 * @return
	 */
	public Long getId(String tableName, String rowKey, String family, String qualifier) {
		Table table = null;
		Long id = 0l;
		try {
			table = getTable(tableName);
			id = table.incrementColumnValue(Bytes.toBytes(rowKey), Bytes.toBytes(family), Bytes.toBytes(qualifier), 1l);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return id;
	}
	
	/**
	 * 鍚戣〃涓坊鍔犱竴鏉℃暟鎹�
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param quelifier
	 * @param value
	 */
	public void put(String tableName, String rowKey, String family, String quelifier, String value) {
		Table table = null;
		try {
			table = getTable(tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes(family), Bytes.toBytes(quelifier), Bytes.toBytes(value));
			table.put(put);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 鍚戣〃涓坊鍔犱竴鏉℃暟鎹�
	 * rokey int
	 * value String
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param quelifier
	 * @param value
	 */
	public void put(String tableName, Integer rowKey, String family, String quelifier, String value) {
		Table table = null;
		try {
			table = getTable(tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes(family), Bytes.toBytes(quelifier), Bytes.toBytes(value));
			table.put(put);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 鍚戣〃涓坊鍔犱竴鏉℃暟鎹�
	 * rokey int
	 * value double
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param quelifier
	 * @param value
	 */
	public void put(String tableName, Integer rowKey, String family, String quelifier, Double value) {
		Table table = null;
		try {
			table = getTable(tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes(family), Bytes.toBytes(quelifier), Bytes.toBytes(value));
			table.put(put);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 鍚戣〃涓坊鍔犱竴鏉℃暟鎹�
	 * rokey int
	 * value int
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param quelifier
	 * @param value
	 */
	public void put(String tableName, Integer rowKey, String family, String quelifier, Integer value) {
		Table table = null;
		try {
			table = getTable(tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes(family), Bytes.toBytes(quelifier), Bytes.toBytes(value));
			table.put(put);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 鍚戣〃涓坊鍔犱竴鏉℃暟鎹�
	 * rokey String
	 * value int
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param quelifier
	 * @param value
	 */
	public void put(String tableName, String rowKey, String family, String quelifier, Integer value) {
		Table table = null;
		try {
			table = getTable(tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes(family), Bytes.toBytes(quelifier), Bytes.toBytes(value));
			table.put(put);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 鍚戣〃涓坊鍔犱竴鏉℃暟鎹�
	 * rokey long
	 * value int
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param quelifier
	 * @param value
	 */
	public void put(String tableName, Long rowKey, String family, String quelifier, Integer value) {
		Table table = null;
		try {
			table = getTable(tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes(family), Bytes.toBytes(quelifier), Bytes.toBytes(value));
			table.put(put);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 鍚戣〃涓坊鍔犱竴鏉℃暟鎹�
	 * rokey long
	 * value String
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param quelifier
	 * @param value
	 */
	public void put(String tableName, Long rowKey, String family, String quelifier, String value) {
		Table table = null;
		try {
			table = getTable(tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes(family), Bytes.toBytes(quelifier), Bytes.toBytes(value));
			table.put(put);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 鍚戣〃涓坊鍔犱竴鏉℃暟鎹�
	 * rokey long
	 * value double
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param quelifier
	 * @param value
	 */
	public void put(String tableName, Long rowKey, String family, String quelifier, Double value) {
		Table table = null;
		try {
			table = getTable(tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes(family), Bytes.toBytes(quelifier), Bytes.toBytes(value));
			table.put(put);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * 鍚戣〃涓坊鍔犱竴鏉℃暟鎹�
	 * rokey long
	 * value long
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param quelifier
	 * @param value
	 */
	public void put(String tableName, Long rowKey, String family, String quelifier, Long value) {
		Table table = null;
		try {
			table = getTable(tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes(family), Bytes.toBytes(quelifier), Bytes.toBytes(value));
			table.put(put);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
