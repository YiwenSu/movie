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
import oracle.demo.oow.bd.to.CastCrewTO;
import oracle.demo.oow.bd.to.CastTO;
import oracle.demo.oow.bd.to.CrewTO;
import oracle.demo.oow.bd.to.GenreTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.hbase.ConstantsHBase;
import oracle.demo.oow.bd.util.hbase.HBaseDB;
import oracle.kv.table.Row;

public class MovieDao {
    public void insertMovie(MovieTO movieTO) {
        boolean flag = false;
        String name = null;

   

        int movieId;
        try {
            if (movieTO != null) {
                //把电影信息写入HBase
            	HBaseDB hBaseDB = HBaseDB.getInstance();
            	Table table = hBaseDB.getTable(ConstantsHBase.TABLE_MOVIE);
            	Put put = new Put(Bytes.toBytes(movieTO.getId()));
            	put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_ORIGINAL_TITLE), Bytes.toBytes(movieTO.getTitle()));
            	put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_OVERVIEW), Bytes.toBytes(movieTO.getOverview()));
            	put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_POSTER_PATH), Bytes.toBytes(movieTO.getPosterPath()));
            	put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_RELEASE_DATE), Bytes.toBytes(movieTO.getReleasedYear()));
            	put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_VOTE_COUNT), Bytes.toBytes(movieTO.getVoteCount()));
            	put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_RUNTIME), Bytes.toBytes(movieTO.getRunTime()));
            	put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_POPULARITY), Bytes.toBytes(movieTO.getPopularity()));
            	
            	table.put(put);
            	/*//写入分类信息
            	table = hBaseDB.getTable(ConstantsHBase.TABLE_GENRE);
            	ArrayList<GenreTO> genres = movieTO.getGenres();
            	for(GenreTO genreTO : genres) {
            		put = new Put(Bytes.toBytes(movieTO.getId()+"_"+genreTO.getId()));
            		put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_GENRE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_GENRE_ID), Bytes.toBytes(genreTO.getId()));
            		put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_GENRE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_GENRE_NAME), Bytes.toBytes(genreTO.getName()));
            		table.put(put);	
            	}*/
            	ArrayList<GenreTO> genreTOs = movieTO.getGenres();
            	GenreDao genreDao = new GenreDao();
            	for(GenreTO genreTO : genreTOs) {
            		genreDao.insert(genreTO);
            		insertMovieToGenre(movieTO,genreTO);
            		genreDao.insertGenreToMovie(movieTO,genreTO);
            	}

            }     	
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}


    } 
    
    private void insertMovieToGenre(MovieTO movieTO, GenreTO genreTO) {
    	HBaseDB db = HBaseDB.getInstance();
    	db.put(ConstantsHBase.TABLE_MOVIE, movieTO.getId()+"_"+genreTO.getId(),
    			ConstantsHBase.FAMILY_MOVIE_GENRE, ConstantsHBase.QUALIFIER_MOVIE_GENRE_ID, genreTO.getId());
    	db.put(ConstantsHBase.TABLE_MOVIE, movieTO.getId()+"_"+genreTO.getId(),
    			ConstantsHBase.FAMILY_MOVIE_GENRE, ConstantsHBase.QUALIFIER_MOVIE_GENRE_NAME, genreTO.getName());
		
	}   
    public void insertMovieCrew(CrewTO c, Integer valueOf) {
		// TODO Auto-generated method stub
		HBaseDB db=HBaseDB.getInstance();
		db.put(ConstantsHBase.TABLE_MOVIE,""+valueOf+'_'+c.getId(), ConstantsHBase.FAMILY_MOVIE_CREW,
				ConstantsHBase.QUALIFIER_MOVIE_CREW_ID,c.getId());
	}
	public void insertMovieCast(CastTO c, Integer valueOf) {
		// TODO Auto-generated method stub
		HBaseDB db=HBaseDB.getInstance();
		db.put(ConstantsHBase.TABLE_MOVIE,""+valueOf+'_'+c.getId(), ConstantsHBase.FAMILY_MOVIE_CAST,
				ConstantsHBase.QUALIFIER_MOVIE_CAST_ID,c.getId());
	}
	public MovieTO getById(int movieId) {
		HBaseDB db = HBaseDB.getInstance();
		Table table = db.getTable(ConstantsHBase.TABLE_MOVIE);
		Get get = new Get(Bytes.toBytes(movieId));
		MovieTO movieTO = null;
		try {
			Result result = table.get(get);
			if(!result.isEmpty()) {
				movieTO = new MovieTO();
				movieTO.setTitle(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_ORIGINAL_TITLE))));
				movieTO.setPosterPath(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_POSTER_PATH))));
				movieTO.setPopularity(Bytes.toDouble(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_POPULARITY))));				
				movieTO.setDate(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_RELEASE_DATE))));				
				movieTO.setRunTime(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_RUNTIME))));
				
				movieTO.setOverview(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_OVERVIEW))));
				movieTO.setVoteCount(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_VOTE_COUNT))));
				movieTO.setId(movieId);
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return movieTO;
	}
	
	public MovieTO getMovieById(int movieId) {
		MovieTO movieTO = getById(movieId);
		
		ArrayList<GenreTO> genres = getGenresByMovieId(movieId);
		movieTO.setGenres(genres);
		
		CastCrewTO castCrewTO = new CastCrewTO();
		
		List<CastTO> castList = getCastsByMovieId(movieId);
		castCrewTO.setCastList(castList);
		
		List<CrewTO> crewList = getCrewsByMovieId(movieId);
		castCrewTO.setCrewList(crewList);
		
		movieTO.setCastCrewTO(castCrewTO);
		
		return movieTO;
	}
	
	private ArrayList<CastTO> getCastsByMovieId(int movieId) {
		ArrayList<CastTO> casts = new ArrayList<>();
		HBaseDB db = HBaseDB.getInstance();
		Table table = db.getTable(ConstantsHBase.TABLE_MOVIE);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_CAST));
		Filter filter = new PrefixFilter(Bytes.toBytes(movieId+"_"));
		scan.setFilter(filter);
		try {
			ResultScanner resultScanner = table.getScanner(scan);
			Iterator<Result> iterator = resultScanner.iterator();
			CastTO castTO = null;
			CastDao castDao = new CastDao();
			while(iterator.hasNext()) {
				Result result = iterator.next();
				int castId = Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_CAST),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_CAST_ID)));
				castTO = castDao.getById(castId);
				casts.add(castTO);
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return casts;
		
	}
	private ArrayList<GenreTO> getGenresByMovieId(int movieId){
		ArrayList<GenreTO> genreTOs = new ArrayList<>();
		HBaseDB db = HBaseDB.getInstance();
		Table table = db.getTable(ConstantsHBase.TABLE_MOVIE);
		Scan scan = new Scan();
		//用列族约束结果集
		scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_GENRE));
		Filter filter = new PrefixFilter(Bytes.toBytes(movieId+"_"));
		scan.setFilter(filter);
		try {
			ResultScanner resultScanner = table.getScanner(scan);
			Iterator<Result> iter = resultScanner.iterator();
			GenreTO genreTO = null;
			while(iter.hasNext()) {
				Result result = iter.next();
				if(!result.isEmpty()) {
					genreTO = new GenreTO();
					genreTO.setId(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_GENRE),
							Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_GENRE_ID))));
					genreTO.setName(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_GENRE),
							Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_GENRE_NAME))));
					genreTOs.add(genreTO);
				}
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return genreTOs;
	}
	
	private ArrayList<CrewTO> getCrewsByMovieId(int movieId) {
		ArrayList<CrewTO> crewTOs = new ArrayList<>();
		HBaseDB db = HBaseDB.getInstance();
		Table table = db.getTable(ConstantsHBase.TABLE_MOVIE);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_CREW));
		Filter filter = new PrefixFilter(Bytes.toBytes(movieId+"_"));
		scan.setFilter(filter);
		try {
			ResultScanner resultScanner = table.getScanner(scan);
			Iterator<Result> iterator = resultScanner.iterator();
			CrewTO crewTO = null;
			CrewDao crewDao = new CrewDao();
			while(iterator.hasNext()) {
				Result result = iterator.next();
				int crewId = Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_CREW),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_CREW_ID)));
				crewTO = crewDao.getById(crewId);
				crewTOs.add(crewTO);
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return crewTOs;
	
	}

}
