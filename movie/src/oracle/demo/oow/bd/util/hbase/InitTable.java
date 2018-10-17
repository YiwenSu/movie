package oracle.demo.oow.bd.util.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

//������
public class InitTable {
	
  public static void main(String[] args) {
	    Configuration conf;
		Connection conn;
	   
	  
	  try {
		/*conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop");
		conf.set("hbase.rootdir", "hdfs://hadoop:9000/hbase");
		conn = ConnectionFactory.createConnection(conf);
		Admin admin = conn.getAdmin();
		
		HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("user"));
		//�������е���������
		HColumnDescriptor id = new HColumnDescriptor(Bytes.toBytes("id"));
		HColumnDescriptor info = new HColumnDescriptor(Bytes.toBytes("info"));
		HColumnDescriptor genre = new HColumnDescriptor(Bytes.toBytes("genre"));
		hTableDescriptor.addFamily(id).addFamily(info).addFamily(genre);
		
		admin.createTable(hTableDescriptor);
		admin.close();*/
		  HBaseDB db = HBaseDB.getInstance();
			//gid
			System.out.println("����gid");
			String table_gid = ConstantsHBase.TABLE_GID;
			String[] fam_gid = {ConstantsHBase.FAMILY_GID_GID};
			db.createTable(table_gid, fam_gid, 1);
			System.out.println("����gid���");
			//user
			System.out.println("����user");
			String table_user = ConstantsHBase.TABLE_USER;
			String[] fam_user = {ConstantsHBase.FAMILY_USER_ID, ConstantsHBase.FAMILY_USER_INFO, ConstantsHBase.FAMILY_USER_GENRE};
			db.createTable(table_user, fam_user,1);
			System.out.println("����user���");
			//genre
			System.out.println("����genre");
			String table_genre = ConstantsHBase.TABLE_GENRE;
			String[] fam_genre = {ConstantsHBase.FAMILY_GENRE_GENRE, ConstantsHBase.FAMILY_GENRE_MOVIE};
			db.createTable(table_genre, fam_genre,1);
			System.out.println("����genre���");
			//movie
			System.out.println("����movie");
			String table_movie = ConstantsHBase.TABLE_MOVIE;
			String[] fam_movie = {ConstantsHBase.FAMILY_MOVIE_CAST, ConstantsHBase.FAMILY_MOVIE_CREW, 
						ConstantsHBase.FAMILY_MOVIE_GENRE,ConstantsHBase.FAMILY_MOVIE_MOVIE};
			db.createTable(table_movie, fam_movie,1);
			System.out.println("����movie���");
			
			//cast
			System.out.println("����cast");
			String table_cast = ConstantsHBase.TABLE_CAST;
			String[] fam_cast = {ConstantsHBase.FAMILY_CAST_CAST, ConstantsHBase.FAMILY_CAST_MOVIE};
			db.createTable(table_cast, fam_cast,1);
			System.out.println("����cast���");
			
			//crew
			System.out.println("����crew");
			String table_crew = ConstantsHBase.TABLE_CREW;
			String[] fam_crew = {ConstantsHBase.FAMILY_CREW_CREW, ConstantsHBase.FAMILY_CREW_MOVIE};
			db.createTable(table_crew, fam_crew,1);
			System.out.println("����crew���");
			
			//activity
			System.out.println("����activity");
			String table_activity = ConstantsHBase.TABLE_ACTIVITY;
			String[] fam_activity = {ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY};
			db.createTable(table_activity, fam_activity,1);
			System.out.println("����activity���");
		
		  
		  
		
	} catch (Exception e) {
		// TODO: handle exception
	}
	
}
}
