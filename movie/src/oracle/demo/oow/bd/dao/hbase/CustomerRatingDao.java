package oracle.demo.oow.bd.dao.hbase;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.hbase.DBUtil;


public class CustomerRatingDao {
    public boolean insertCustomerRating(int userId, int movieId,int rating) {
    	Connection connection = DBUtil.getConn();   
    	PreparedStatement stat = null;
    	boolean result = false;
    	try {
			String sql = "insert into cust_rating(userid,movieid,rating) values(?,?,?)";
			stat = connection.prepareStatement(sql);
			stat.setInt(1, userId);
			stat.setInt(2, movieId);
			stat.setInt(3, rating);
			
			result = stat.executeUpdate()>0?true:false;
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}finally {
			DBUtil.close(null,stat,connection);
		}
    	return result;
    }
	public void deleteCustomerRating(int userId) {
        String delete = null;
        PreparedStatement stmt = null;
        Connection conn = DBUtil.getConn();

        delete = "DELETE FROM cust_rating WHERE userid = ?";
        try {
            if (conn != null) {
                stmt = conn.prepareStatement(delete);
                stmt.setInt(1, userId);
                stmt.execute();
                stmt.close();
            }
        } catch (SQLException e) {
            System.out.println(e.getErrorCode() + ":" + e.getMessage());
        } finally {
        	DBUtil.close(null, stmt, conn);
        }
	}
	
	public List<MovieTO> getMoviesByMood(int userId) {
        List<MovieTO> movieList = null;
        String search = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        MovieTO movieTO = null;
        MovieDao movieDao = new MovieDao();

        search =
                "SELECT * FROM " + "(SELECT r.movieid movieid, r.score score FROM cust_rating c JOIN recommend r "+
                		"ON c.userid=r.userid WHERE c.userid=? "+
                		"ORDER BY r.score DESC) " + "WHERE limit 20";
        try {
        	Connection conn = DBUtil.getConn();
            if (conn != null) {
                //initialize movieList only when connection is successful
                movieList = new ArrayList<MovieTO>();
                stmt = conn.prepareStatement(search);
                stmt.setInt(1, userId);
                rs = stmt.executeQuery();
                while (rs.next()) {
                    //Retrieve by column name
                    int id = rs.getInt("movieid");

                    //create new object
                    movieTO = movieDao.getById(id);
                    if (movieTO != null) {
                        movieList.add(movieTO);
                    } //if (movieTO != null)
                } //EOF while
            } //EOF if (conn!=null)
        } catch (Exception e) {
            //No Database is running, so can not recommend item-item similarity             
        }
        return movieList;
    }
}
