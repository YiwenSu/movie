package oracle.demo.oow.bd.dao.hbase;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import oracle.demo.oow.bd.dao.GenreDAO;
import oracle.demo.oow.bd.to.CastMovieTO;
import oracle.demo.oow.bd.to.CastTO;
import oracle.demo.oow.bd.to.GenreTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.KeyUtil;
import oracle.demo.oow.bd.util.hbase.ConstantsHBase;
import oracle.demo.oow.bd.util.hbase.HBaseDB;
import oracle.kv.Value;
import oracle.kv.Version;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;

public class CastDao {
	  public void insertCastInfo(CastTO castTO) {
	        int castId;
	        try {
	        if (castTO != null) {
	            //把cast信息写入HBase
	        	HBaseDB hBaseDB = HBaseDB.getInstance();
	        	Table table = hBaseDB.getTable(ConstantsHBase.TABLE_CAST);
	        	Put put = new Put(Bytes.toBytes(castTO.getId()));
	        	put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_CAST), Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_NAME), Bytes.toBytes(castTO.getName()));
	        	table.put(put);
	        	/*//cast和电影的映射信息写入
	        	List<CastMovieTO> list = castTO.getCastMovieList();
	        	for(CastMovieTO castMovieTO : list) {
	        		put = new Put(Bytes.toBytes(castTO.getId()+"_"+castMovieTO.getId()));
	        		put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_CHARACTER), Bytes.toBytes(castMovieTO.getCharacter()));
	        		put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_ORDER), Bytes.toBytes(castMovieTO.getOrder()));
	        		put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_MOVIE_ID), Bytes.toBytes(castMovieTO.getId()));
	        		table.put(put);*/
	        	insertCastToMovie(castTO);
	        	}   	        	
			
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}

	    } 
	  private void insertCastToMovie(CastTO castTO) {
		  HBaseDB db = HBaseDB.getInstance();
		  List<CastMovieTO> movieTOs = castTO.getCastMovieList();
		  MovieDao movieDao = new MovieDao();
		  for(CastMovieTO castMovieTO : movieTOs) {
			  db.put(ConstantsHBase.TABLE_CAST, castTO.getId()+"_"+castMovieTO.getId(),
					  ConstantsHBase.FAMILY_CAST_MOVIE, ConstantsHBase.QUALIFIER_CAST_MOVIE_ID, castMovieTO.getId());
			  db.put(ConstantsHBase.TABLE_CAST, castTO.getId()+"_"+castMovieTO.getId(),
					  ConstantsHBase.FAMILY_CAST_MOVIE, ConstantsHBase.QUALIFIER_CAST_CHARACTER, castMovieTO.getCharacter());
			  db.put(ConstantsHBase.TABLE_CAST, castTO.getId()+"_"+castMovieTO.getId(),
					  ConstantsHBase.FAMILY_CAST_MOVIE, ConstantsHBase.QUALIFIER_CAST_ORDER, castMovieTO.getOrder());
			  movieDao.insertMovieCast(castTO,castMovieTO.getId());
		  }
		
	}
	  public CastTO getById(int castId) {
			// TODO Auto-generated method stub
			HBaseDB db = HBaseDB.getInstance();
			Table table=db.getTable(ConstantsHBase.TABLE_CAST);
			Get get=new Get(Bytes.toBytes(castId));
			CastTO c=null;
			
			try {
				Result r=table.get(get);
				if(!r.isEmpty()) {
					c=new CastTO();
					c.setName(Bytes.toString(r.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_CAST),
							Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_NAME))));		
					c.setOrder(Bytes.toInt(r.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_MOVIE),
							Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_ORDER))));
					c.setCharacter(Bytes.toString(r.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_MOVIE),
							Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_CHARACTER))));
					c.setId(castId);
					
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return c;
		}
		public List<MovieTO> getMoviesByCast(int castId){
			List<MovieTO> list = new ArrayList<MovieTO>();
			HBaseDB db = HBaseDB.getInstance();
			Table table = db.getTable(ConstantsHBase.TABLE_CAST);
			Scan scan = new Scan();
			scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_MOVIE));
			
			Filter filter = new PrefixFilter(Bytes.toBytes(castId+"_"));
			scan.setFilter(filter);
			MovieDao movieDao = new MovieDao();
			
			try {
				ResultScanner resultScanner = table.getScanner(scan);
				Iterator<Result> iter = resultScanner.iterator();
				
				while(iter.hasNext()) {
					Result result = iter.next();
					if(!result.isEmpty()) {
						int movieId = Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_MOVIE), 
								Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_MOVIE_ID)));
						if(movieId>0) {
							MovieTO movieTO = movieDao.getMovieById(movieId);
							list.add(movieTO);
						}
					}
				}
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}
			return list;
		}
}
