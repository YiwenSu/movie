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

import oracle.demo.oow.bd.to.CastMovieTO;
import oracle.demo.oow.bd.to.CastTO;
import oracle.demo.oow.bd.to.CrewTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.hbase.ConstantsHBase;
import oracle.demo.oow.bd.util.hbase.HBaseDB;
import oracle.kv.table.Row;

public class CrewDao {
	   public void insertCrewInfo(CrewTO crewTO) {
	    
	        try {
	        if (crewTO != null) {
	            //把crew信息写入HBase
	        	HBaseDB hBaseDB = HBaseDB.getInstance();
	        	Table table = hBaseDB.getTable(ConstantsHBase.TABLE_CREW);
	        	Put put = new Put(Bytes.toBytes(crewTO.getId()));
	        	put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_CREW_CREW), Bytes.toBytes(ConstantsHBase.QUALIFIER_CREW_NAME), Bytes.toBytes(crewTO.getName()));
	        	put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_CREW_CREW), Bytes.toBytes(ConstantsHBase.QUALIFIER_CREW_JOB), Bytes.toBytes(crewTO.getJob()));
	        	table.put(put);
	        	//crew和电影的映射信息写入
	        	/*List<CrewMovieTO> list = crewTO.getMovieList();
	        	for(CrewMovieTO crewMovieTO : list) {
	        		put = new Put(Bytes.toBytes(castTO.getId()+"_"+castMovieTO.getId()));
	        		put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_CHARACTER), Bytes.toBytes(castMovieTO.getCharacter()));
	        		put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_ORDER), Bytes.toBytes(castMovieTO.getOrder()));
	        		put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_MOVIE_ID), Bytes.toBytes(castMovieTO.getId()));
	        		table.put(put);
	        	}   	  */      	

                insertCrewToMovie(crewTO);
	        }				
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}

	    } 
		  private void insertCrewToMovie(CrewTO crewTO) {
			  HBaseDB db = HBaseDB.getInstance();
			  List<String> movieTOs = crewTO.getMovieList();
			  MovieDao movieDao = new MovieDao();
			  for(String movieId : movieTOs) {
				  db.put(ConstantsHBase.TABLE_CREW, crewTO.getId()+"_"+movieId,
						  ConstantsHBase.FAMILY_CREW_MOVIE, ConstantsHBase.QUALIFIER_CREW_MOVIE_ID, Integer.valueOf(movieId));
				  movieDao.insertMovieCrew(crewTO,Integer.valueOf(movieId));
			  }
			
		}
		  public CrewTO getById(int crewId) {
				// TODO Auto-generated method stub
				HBaseDB db = HBaseDB.getInstance();
				Table table=db.getTable(ConstantsHBase.TABLE_CAST);
				Get get=new Get(Bytes.toBytes(crewId));
				CrewTO c=null;
				
				try {
					Result r=table.get(get);
					if(!r.isEmpty()) {
						c=new CrewTO();
						c.setName(Bytes.toString(r.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CREW_CREW),
								Bytes.toBytes(ConstantsHBase.QUALIFIER_CREW_NAME))));		
						c.setJob(Bytes.toString(r.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CREW_CREW),
								Bytes.toBytes(ConstantsHBase.QUALIFIER_CREW_JOB))));
						c.setMovieId(Bytes.toInt(r.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CREW_MOVIE),
								Bytes.toBytes(ConstantsHBase.QUALIFIER_CREW_MOVIE_ID))));
						c.setId(crewId);
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return c;
			} 
			public List<MovieTO> getMoviesByCrew(int crewId){
				List<MovieTO> list = new ArrayList<MovieTO>();
				HBaseDB db = HBaseDB.getInstance();
				Table table = db.getTable(ConstantsHBase.TABLE_CREW);
				Scan scan = new Scan();
				scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_CREW_MOVIE));
				
				Filter filter = new PrefixFilter(Bytes.toBytes(crewId+"_"));
				scan.setFilter(filter);
				MovieDao movieDao = new MovieDao();
				
				try {
					ResultScanner resultScanner = table.getScanner(scan);
					Iterator<Result> iter = resultScanner.iterator();
					
					while(iter.hasNext()) {
						Result result = iter.next();
						if(!result.isEmpty()) {
							int movieId = Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CREW_MOVIE), 
									Bytes.toBytes(ConstantsHBase.QUALIFIER_CREW_MOVIE_ID)));
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
