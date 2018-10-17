package oracle.demo.oow.bd.util.hbase;

public class ConstantsHBase {
	public static String ZOOKEEPER = "hadoop";
	public static String HBASE_ROOT_DIR = "hdfs://hadoop:9000/hbase";
	
	public static final String MYSQL_USERNAME = "root";
	public static final String MYSQL_PASSWORD = "root";
	public static String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
	public static String MYSQL_URL = "jdbc:mysql://localhost:3306/movie?useUnicode=true&characterEncoding=utf-8";
	
	
	
	//������
	public static final String TABLE_GID = "gid";
	public static final String TABLE_USER = "user";
	public static final String TABLE_GENRE = "genre";
	public static final String TABLE_MOVIE = "movie";
	public static final String TABLE_CAST = "cast";
	public static final String TABLE_CREW = "crew";
	public static final String TABLE_ACTIVITY = "activity";
	
	//����������������   FAMILY_������_��������
	public static final String FAMILY_GID_GID = "gid";
	public static final String FAMILY_USER_ID = "id";
	public static final String FAMILY_USER_INFO = "info";
	public static final String FAMILY_USER_GENRE = "genre";
	public static final String FAMILY_GENRE_GENRE = "genre";
	public static final String FAMILY_GENRE_MOVIE = "movie";
	public static final String FAMILY_MOVIE_MOVIE = "movie";
	public static final String FAMILY_MOVIE_GENRE = "genre";
	public static final String FAMILY_MOVIE_CAST = "cast";
	public static final String FAMILY_MOVIE_CREW = "crew";
	public static final String FAMILY_CAST_CAST = "cast";
	public static final String FAMILY_CAST_MOVIE = "movie";
	public static final String FAMILY_CREW_CREW = "crew";
	public static final String FAMILY_CREW_MOVIE = "movie";
	public static final String FAMILY_ACTIVITY_ACTIVITY = "activity";
	
	//gid�����н����ƣ������н����Ǳ���
	public static final String ROW_KEY_GID_ACTIVITY_ID = "activity_id";
	
	//��������������  QUALIFIER_������_������
	//gid��
	public static final String QUALIFIER_GID_ACTIVITY_ID = "activity_id";
	//user��
	public static final String QUALIFIER_USER_ID = "id";
	public static final String QUALIFIER_USER_NAME = "name";
	public static final String QUALIFIER_USER_EMAIL = "email";
	public static final String QUALIFIER_USER_USERNAME = "username";
	public static final String QUALIFIER_USER_PASSWORD = "password";
	public static final String QUALIFIER_USER_GENRE_ID = "genre_id";
	public static final String QUALIFIER_USER_GENRE_NAME = "genre_name";
	public static final String QUALIFIER_USER_SCORE = "score";
	//genre��
	public static final String QUALIFIER_GENRE_NAME = "name";
	public static final String QUALIFIER_GENRE_MOVIE_ID = "movie_id";
	//movie
	public static final String QUALIFIER_MOVIE_ORIGINAL_TITLE = "original_title";
	public static final String QUALIFIER_MOVIE_OVERVIEW = "overview";
	public static final String QUALIFIER_MOVIE_POSTER_PATH = "poster_path";
	public static final String QUALIFIER_MOVIE_RELEASE_DATE = "release_date";
	public static final String QUALIFIER_MOVIE_VOTE_COUNT = "vote_count";
	public static final String QUALIFIER_MOVIE_RUNTIME = "runtime";
	public static final String QUALIFIER_MOVIE_POPULARITY = "popularity";
	public static final String QUALIFIER_MOVIE_GENRE_ID = "genre_id";
	public static final String QUALIFIER_MOVIE_GENRE_NAME = "genre_name";
	public static final String QUALIFIER_MOVIE_CAST_ID = "cast_id";
	public static final String QUALIFIER_MOVIE_CREW_ID = "crew_id";
	//cast��
	public static final String QUALIFIER_CAST_NAME = "name";
	public static final String QUALIFIER_CAST_MOVIE_ID = "movie_id";
	public static final String QUALIFIER_CAST_CHARACTER = "character";
	public static final String QUALIFIER_CAST_ORDER = "order";
	//crew��
	public static final String QUALIFIER_CREW_NAME = "name";
	public static final String QUALIFIER_CREW_JOB = "job";
	public static final String QUALIFIER_CREW_MOVIE_ID = "movie_id";
	//activity��
	public static final String QUALIFIER_ACTIVITY_USER_ID = "user_id";
	public static final String QUALIFIER_ACTIVITY_MOVIE_ID = "movie_id";
	public static final String QUALIFIER_ACTIVITY_GENRE_ID = "genre_id";
	public static final String QUALIFIER_ACTIVITY_ACTIVITY = "activity";
	public static final String QUALIFIER_ACTIVITY_RECOMMENDED = "recommended";
	public static final String QUALIFIER_ACTIVITY_TIME = "time";
	public static final String QUALIFIER_ACTIVITY_RATING = "rating";
	public static final String QUALIFIER_ACTIVITY_PRICE = "price";
	public static final String QUALIFIER_ACTIVITY_POSITION = "position";

}
