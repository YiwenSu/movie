package oracle.demo.oow.bd.util;

import java.io.FileInputStream;
import java.util.Properties;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

import oracle.demo.oow.bd.util.hbase.ConstantsHBase;

public class InitServlet extends HttpServlet{

	@Override
	public void init(ServletConfig config) throws ServletException {
		
		//璇诲彇conf.properties閰嶇疆鏂囦欢锛屾妸FileWriterUtil.OUTPUT_FILE杩涜璧嬪��
		super.init(config);
		String path;
		FileInputStream fis;
		try {
			path = InitServlet.class.getResource("/").getPath();
			fis = new FileInputStream(path+"conf.properties");
			Properties properties = new Properties();
			properties.load(fis);
			fis.close();
			/*System.out.println("path:"+path);
			System.out.println();*/
			String outputFile = properties.getProperty("output_file");
			if(outputFile!=null) {
				FileWriterUtil.OUTPUT_FILE = outputFile;
			}
			String zookeeper = properties.getProperty("zookeeper");
			if(zookeeper!=null) {
				ConstantsHBase.ZOOKEEPER = zookeeper;
			}
			String hbaseRootDir = properties.getProperty("hbase.root.dir");
			if(hbaseRootDir!=null) {
				ConstantsHBase.HBASE_ROOT_DIR = hbaseRootDir;
			}
			String mysqlDriver = properties.getProperty("mysql.driver");
			if(mysqlDriver!=null) {
				ConstantsHBase.MYSQL_DRIVER = mysqlDriver;
			}
			String mysqlURL = properties.getProperty("mysql.url");
			if(mysqlURL!=null) {
				ConstantsHBase.MYSQL_URL = mysqlURL;
			}
			String mysqlUsername = properties.getProperty("mysql.username");
			if(mysqlUsername!=null) {
				//报错注释了ConstantsHBase.MYSQL_USERNAME = mysqlUsername;
			}
			String mysqlPassword = properties.getProperty("mysql.password");
			if(mysqlPassword!=null) {
				//报错注释了ConstantsHBase.MYSQL_PASSWORD = mysqlPassword;
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}
	
}
