package oracle.demo.oow.bd.dao.hbase;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import oracle.demo.oow.bd.to.GenreMovieTO;
import oracle.demo.oow.bd.to.GenreTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.hbase.ConstantsHBase;
import oracle.demo.oow.bd.util.hbase.HBaseDB;

public class GenreDao {
	
	public void insert(GenreTO genreTO) {
		HBaseDB db = HBaseDB.getInstance();
		db.put(ConstantsHBase.TABLE_GENRE, genreTO.getId(), ConstantsHBase.FAMILY_GENRE_GENRE,
				ConstantsHBase.QUALIFIER_GENRE_NAME, genreTO.getName());
		
	}
	
	public void insertGenreToMovie(MovieTO movieTO, GenreTO genreTO) {
		HBaseDB db = HBaseDB.getInstance();
		db.put(ConstantsHBase.TABLE_GENRE, genreTO.getId()+"_"+movieTO.getId(),
				ConstantsHBase.FAMILY_GENRE_MOVIE, ConstantsHBase.QUALIFIER_GENRE_MOVIE_ID, movieTO.getId());
		
		
	}
	 public List<GenreMovieTO> getMovies4Customer(int custId, int movieMaxCount, int genreMaxCount){
		List<GenreMovieTO> genreTOs = new ArrayList<>();
		Scan scan = new Scan();
		Filter filter = new PageFilter(genreMaxCount);
		scan.setFilter(filter);
		scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_GENRE));
		HBaseDB db = HBaseDB.getInstance();
		Table table = db.getTable(ConstantsHBase.TABLE_GENRE);
		
		//È«±íÉ¨Ãè
		try {
			ResultScanner resultScanner = table.getScanner(scan);
			Iterator<Result> iter = resultScanner.iterator();
			GenreTO genreTO = null;
			
			while(iter.hasNext()) {
				genreTO = new GenreTO();
				Result result = iter.next();
				genreTO.setId(Bytes.toInt(result.getRow()));
				genreTO.setName(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_GENRE), 
						Bytes.toBytes(ConstantsHBase.QUALIFIER_GENRE_NAME))));
				GenreMovieTO genreMovieTO = new GenreMovieTO();
				genreMovieTO.setGenreTO(genreTO);
				genreTOs.add(genreMovieTO);
			}
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return genreTOs;
	}
	

}
