package oracle.demo.oow.bd.dao.hbase;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import oracle.demo.oow.bd.pojo.ActivityType;
import oracle.demo.oow.bd.pojo.BooleanType;
import oracle.demo.oow.bd.pojo.RatingType;
import oracle.demo.oow.bd.to.ActivityTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.FileWriterUtil;
import oracle.demo.oow.bd.util.hbase.ConstantsHBase;
import oracle.demo.oow.bd.util.hbase.HBaseDB;

public class ActivityDao {
	public void insertCustomerActivity(ActivityTO activityTO) {
		FileWriterUtil.writeOnFile(activityTO.getActivityJsonOriginal().toString());
		
		HBaseDB db = HBaseDB.getInstance();
		long id = db.getId(ConstantsHBase.TABLE_GID, ConstantsHBase.ROW_KEY_GID_ACTIVITY_ID, 
				ConstantsHBase.FAMILY_GID_GID, ConstantsHBase.QUALIFIER_GID_ACTIVITY_ID);
		
		db.put(ConstantsHBase.TABLE_ACTIVITY, id, ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY, 
				ConstantsHBase.QUALIFIER_ACTIVITY_ACTIVITY, activityTO.getActivity().getValue());
		db.put(ConstantsHBase.TABLE_ACTIVITY, id, ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY, 
				ConstantsHBase.QUALIFIER_ACTIVITY_GENRE_ID, activityTO.getGenreId());
		
		db.put(ConstantsHBase.TABLE_ACTIVITY, id, ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY, 
				ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID, activityTO.getMovieId());
		
		db.put(ConstantsHBase.TABLE_ACTIVITY, id, ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY, 
				ConstantsHBase.QUALIFIER_ACTIVITY_POSITION, activityTO.getPosition());
		db.put(ConstantsHBase.TABLE_ACTIVITY, id, ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY, 
				ConstantsHBase.QUALIFIER_ACTIVITY_PRICE, activityTO.getPrice());
		db.put(ConstantsHBase.TABLE_ACTIVITY, id, ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY, 
				ConstantsHBase.QUALIFIER_ACTIVITY_RATING, activityTO.getRating().getValue());
		db.put(ConstantsHBase.TABLE_ACTIVITY, id, ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY, 
				ConstantsHBase.QUALIFIER_ACTIVITY_RECOMMENDED, activityTO.isRecommended().getValue());
		db.put(ConstantsHBase.TABLE_ACTIVITY, id, ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY, 
				ConstantsHBase.QUALIFIER_ACTIVITY_TIME, activityTO.getTimeStamp());
		db.put(ConstantsHBase.TABLE_ACTIVITY, id, ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY, 
				ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID, activityTO.getCustId());
		
	}
	public List<MovieTO> getCustomerCurrentWatchList(int custId){
		List<MovieTO> movieTOs = new ArrayList<>();
		
		HBaseDB db = HBaseDB.getInstance();
		Table table = db.getTable(ConstantsHBase.TABLE_ACTIVITY);
		Scan scan = new Scan();
		
		Filter filter = new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
				Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID),CompareOp.EQUAL , Bytes.toBytes(custId));
		Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
				Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_ACTIVITY),CompareOp.EQUAL , Bytes.toBytes(ActivityType.STARTED_MOVIE.getValue()));
		Filter filter3 = new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
				Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_ACTIVITY),CompareOp.EQUAL , Bytes.toBytes(ActivityType.PAUSED_MOVIE.getValue()));
		FilterList filterListTmp = new FilterList(Operator.MUST_PASS_ONE,filter2,filter3);
		
		FilterList filterList = new FilterList(filter,filterListTmp);
		scan.setFilter(filterList);
		try {
			ResultScanner resultScanner = table.getScanner(scan);
			Iterator<Result> iterator = resultScanner.iterator();
			MovieDao movieDao = new MovieDao();
			while(iterator.hasNext()) {
				Result result = iterator.next();
				int movieId = Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), 
						Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID)));
				MovieTO movieTO = movieDao.getById(movieId);
				movieTOs.add(movieTO);
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return movieTOs;
	}
	
	public List<MovieTO> getCustomerBrowseList(int custId){
		List<MovieTO> movieTOs = new ArrayList<>();
		
		HBaseDB db = HBaseDB.getInstance();
		Table table = db.getTable(ConstantsHBase.TABLE_ACTIVITY);
		Scan scan = new Scan();
		
		Filter filter = new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
				Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID),CompareOp.EQUAL , Bytes.toBytes(custId));
		
		Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
				Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_ACTIVITY),CompareOp.EQUAL ,
				Bytes.toBytes(ActivityType.STARTED_MOVIE.getValue()));
		
		FilterList filterList = new FilterList(filter,filter2);
		
		scan.setFilter(filterList);
		try {
			ResultScanner resultScanner = table.getScanner(scan);
			Iterator<Result> iterator = resultScanner.iterator();
			MovieDao movieDao = new MovieDao();
			while(iterator.hasNext()) {
				Result result = iterator.next();
				int movieId = Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), 
						Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID)));
				MovieTO movieTO = movieDao.getById(movieId);
				movieTOs.add(movieTO);
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return movieTOs;
	}
	
	public List<MovieTO> getCustomerHistoricWatchList(int custId){
		List<MovieTO> movieTOs = new ArrayList<>();
		
		HBaseDB db = HBaseDB.getInstance();
		Table table = db.getTable(ConstantsHBase.TABLE_ACTIVITY);
		Scan scan = new Scan();
		
		Filter filter = new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
				Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID),CompareOp.EQUAL , Bytes.toBytes(custId));
		
		Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
				Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_ACTIVITY),CompareOp.EQUAL ,
				Bytes.toBytes(ActivityType.STARTED_MOVIE.getValue()));
		
		FilterList filterList = new FilterList(filter,filter2);
		
		scan.setFilter(filterList);
		try {
			ResultScanner resultScanner = table.getScanner(scan);
			Iterator<Result> iterator = resultScanner.iterator();
			MovieDao movieDao = new MovieDao();
			while(iterator.hasNext()) {
				Result result = iterator.next();
				int movieId = Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), 
						Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID)));
				MovieTO movieTO = movieDao.getById(movieId);
				movieTOs.add(movieTO);
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return movieTOs;
	}
	
    public ActivityTO getActivityTO(int custId, int movieId) {
		//1. 根据条件从activity表中，把movieid查询出来
		HBaseDB db = HBaseDB.getInstance();
		Table table = db.getTable(ConstantsHBase.TABLE_ACTIVITY);
		
		Scan scan = new Scan();
		//查询user_id=custId过滤器
		Filter filter = new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID), CompareOp.EQUAL, Bytes.toBytes(custId));
		//ActivityType.STARTED_MOVIE或ActivityType.PAUSED_MOVIE
		Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_ACTIVITY), CompareOp.EQUAL, Bytes.toBytes(ActivityType.COMPLETED_MOVIE.getValue()));
		
		FilterList filterList = new FilterList(filter, filter2);
		scan.setFilter(filterList);
		ActivityTO activityTO = null;
		try {
			ResultScanner resultScanner = table.getScanner(scan);
			Iterator<Result> iterator = resultScanner.iterator();
			if(iterator.hasNext()) {
				Result result = iterator.next();
				//获取activity的行健
				int id = Bytes.toInt(result.getRow());
				
				activityTO = getById(id);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return activityTO;
    }
    
    private ActivityTO getById(int id) {
		HBaseDB db = HBaseDB.getInstance();
		Table table = db.getTable(ConstantsHBase.TABLE_ACTIVITY);
		Get get = new Get(Bytes.toBytes(id));
		ActivityTO activityTO = null;
		try {
			Result result = table.get(get);
			if(!result.isEmpty()) {
				activityTO = new ActivityTO();
				byte[] userIds = result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID));
				if(userIds!=null) {
					activityTO.setCustId(Bytes.toInt(userIds));
				}
				byte[] movieIds = result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID));
				if(movieIds!=null) {
					activityTO.setMovieId(Bytes.toInt(userIds));
				}
				byte[] genreIds = result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_GENRE_ID));
				if(genreIds!=null) {
					activityTO.setGenreId(Bytes.toInt(userIds));
				}
				byte[] activitys = result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_ACTIVITY));
				if(activitys!=null) {
					activityTO.setActivity(ActivityType.getType(Bytes.toInt(userIds)));
				}
				byte[] recommendeds = result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_RECOMMENDED));
				if(recommendeds!=null) {
					activityTO.setRecommended(BooleanType.getType(Bytes.toString(userIds)));
				}
				byte[] times = result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_TIME));
				if(times!=null) {
					activityTO.setTimeStamp(Bytes.toInt(userIds));
				}
				byte[] ratings = result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_RATING));
				if(ratings!=null) {
					activityTO.setRating(RatingType.getType(Bytes.toInt(userIds)));
				}
				byte[] prices = result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_PRICE));
				if(prices!=null) {
					activityTO.setPrice(Bytes.toDouble(userIds));
				}
				byte[] positions = result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_POSITION));
				if(positions!=null) {
					activityTO.setPosition(Bytes.toInt(userIds));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
		return null;
	}
	
	

}
