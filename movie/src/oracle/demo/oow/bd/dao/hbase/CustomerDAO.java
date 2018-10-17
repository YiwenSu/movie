package oracle.demo.oow.bd.dao.hbase;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import oracle.demo.oow.bd.dao.MovieDAO;
import oracle.demo.oow.bd.to.ActivityTO;
import oracle.demo.oow.bd.to.CustomerGenreMovieTO;
import oracle.demo.oow.bd.to.CustomerTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.KeyUtil;
import oracle.demo.oow.bd.util.StringUtil;
import oracle.demo.oow.bd.util.hbase.ConstantsHBase;
import oracle.demo.oow.bd.util.hbase.HBaseDB;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.TableIterator;


public class CustomerDAO {
	//把电影信息写入到HBase中
    public void insertCustomerProfile(CustomerTO customerTO, boolean force) {
    	
    	try {
          HBaseDB baseDB = HBaseDB.getInstance();
         
         Table table = baseDB.getTable(ConstantsHBase.TABLE_USER);
         
         Put put = new Put(Bytes.toBytes(customerTO.getUserName()));  
         put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_USER_ID),Bytes.toBytes(ConstantsHBase.QUALIFIER_USER_ID),Bytes.toBytes(customerTO.getId()));        
         table.put(put);
         
          put = new Put(Bytes.toBytes(customerTO.getId())); 
         put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_USER_INFO),Bytes.toBytes(ConstantsHBase.QUALIFIER_USER_NAME),Bytes.toBytes(customerTO.getName()));           
         table.put(put);
         
          put = new Put(Bytes.toBytes(customerTO.getId())); 
         put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_USER_INFO),Bytes.toBytes(ConstantsHBase.QUALIFIER_USER_EMAIL),Bytes.toBytes(customerTO.getEmail()));           
         table.put(put);
         
          put = new Put(Bytes.toBytes(customerTO.getId())); 
         put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_USER_INFO),Bytes.toBytes(ConstantsHBase.QUALIFIER_USER_USERNAME),Bytes.toBytes(customerTO.getUserName()));  
         table.put(put);
         
          put = new Put(Bytes.toBytes(customerTO.getId())); 
         put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_USER_INFO),Bytes.toBytes(ConstantsHBase.QUALIFIER_USER_PASSWORD),Bytes.toBytes(customerTO.getPassword()));           
         table.put(put);   		
			
		} catch (Exception e) {
			e.printStackTrace();
		}

         
         
         
         
         
      
    } //insertCustomerProfile

	public CustomerTO getCustomerByCredential(String username, String password) {
		CustomerTO customerTO = null;
		int userId = getIdByUsername(username);
		if(userId>0)
			customerTO = getById(userId);
		
		return customerTO;
	}
	private int getIdByUsername(String username) {
		HBaseDB db = HBaseDB.getInstance();
		Table table = db.getTable(ConstantsHBase.TABLE_USER);
		Get get = new Get(Bytes.toBytes(username));
		get.addColumn(Bytes.toBytes(ConstantsHBase.QUALIFIER_USER_ID), Bytes.toBytes(ConstantsHBase.QUALIFIER_USER_ID));
		int id = 0;
		try {
			Result result = table.get(get);
			if(!result.isEmpty())
				id = Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_USER_ID), Bytes.toBytes(ConstantsHBase.QUALIFIER_USER_ID)));
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return id;
		

    }
	
	public CustomerTO getById(int userId) {
		CustomerTO customerTO = null;
		HBaseDB db = HBaseDB.getInstance();
		Table table = db.getTable(ConstantsHBase.TABLE_USER);
		Get get = new Get(Bytes.toBytes(userId));
		
		get.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_USER_INFO));
		try {
			Result result = table.get(get);
			if(!result.isEmpty()) {
				customerTO = new CustomerTO();
				customerTO.setUserName(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_USER_INFO), Bytes.toBytes(ConstantsHBase.QUALIFIER_USER_USERNAME))));
				customerTO.setName(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_USER_INFO), Bytes.toBytes(ConstantsHBase.QUALIFIER_USER_NAME))));
				customerTO.setPassword(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_USER_INFO), Bytes.toBytes(ConstantsHBase.QUALIFIER_USER_PASSWORD))));
				customerTO.setEmail(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_USER_INFO), Bytes.toBytes(ConstantsHBase.QUALIFIER_USER_EMAIL))));
				customerTO.setId(userId);
			
			
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return customerTO;
	}
	
    public List<MovieTO> getMovies4CustomerByGenre(int custId, int genreId) {
       HBaseDB hBaseDB = HBaseDB.getInstance();
       Table table = hBaseDB.getTable(ConstantsHBase.TABLE_GENRE);
       Scan scan =new Scan();
       scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_MOVIE));
       Filter filter = new PrefixFilter(Bytes.toBytes(genreId+"_"));
       //Filter filter2 = new PageFilter(MOVIE_MAX_COUNT);
       Filter filter2 = new PageFilter(25);
       FilterList filterList = new FilterList(filter,filter2);
       scan.setFilter(filterList);
       
       ResultScanner resultScanner = null;
       try {
		resultScanner = table.getScanner(scan);
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
      List<MovieTO> movieTOs = new ArrayList();
      MovieTO movieTO = null;
      if(resultScanner != null) {
    	  Iterator<Result> iter = resultScanner.iterator();
    	  MovieDao movieDao = new MovieDao();
    	  while(iter.hasNext()) {
    		  Result result = iter.next();
    		  if(result!=null&!result.isEmpty()) {
    			  int movieId = Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_MOVIE), 
    					  Bytes.toBytes(ConstantsHBase.QUALIFIER_GENRE_MOVIE_ID)));
    			  movieTO = movieDao.getById(movieId);
    			  if(StringUtil.isNotEmpty(movieTO.getPosterPath())) {
    				  movieTO.setOrder(100);
    			  }else {
    				  movieTO.setOrder(0);
    			  }
    			  movieTOs.add(movieTO);
    		  }
    	  }
      }
       Collections.sort(movieTOs);
       return movieTOs;
    } //getMovie4CustomerByGenre
	
	

}
