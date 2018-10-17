 <!DOCTYPE html>
<%@page import="oracle.demo.oow.bd.dao.hbase.ActivityDao"%>
<%@page import="oracle.demo.oow.bd.dao.hbase.CustomerDAO"%>
<%@ page contentType="text/html;charset=windows-1252"%>
<%@ page pageEncoding="UTF-8" %>
<%@ page import="oracle.demo.oow.bd.dao.GenreDAO" %>
<%@ page import="oracle.demo.oow.bd.dao.hbase.GenreDao" %>
<%--@ page import="oracle.demo.oow.bd.dao.CustomerDAO" --%>
<%@ page import="oracle.demo.oow.bd.to.GenreTO" %>
<%@ page import="oracle.demo.oow.bd.to.GenreMovieTO" %>
<%@ page import="oracle.demo.oow.bd.to.ActivityTO" %>
<%@ page import="java.util.List" %>
<%@ page import="java.util.Arrays" %>
<%@ page import="oracle.demo.oow.bd.to.MovieTO" %>
<%@ page import="oracle.demo.oow.bd.constant.Constant" %>
<%@ page import="oracle.demo.oow.bd.util.YouTubeUtil" %>

<html>
  <head>
    <meta http-equiv="content-type" content="text/html; charset=utf-8">
    <link href="css/style.css" rel="stylesheet" type="text/css"></link>
    <link href="css/jquery.rating.css" rel="stylesheet" type="text/css"></link>
    <link href="css/dd.css" rel="stylesheet" type="text/css"></link>
    <script type="text/javascript" src="js/jquery-1.7.1.min.js"></script>
    <script type="text/javascript" src="js/jquery.jcarousel.min.js"></script>
    <script type="text/javascript" src="js/jquery.rating.pack.js"></script>
    <script type="text/javascript" src="js/jquery.dd.js"></script>
    <script type="text/javascript" src="js/scripts.js"></script>
    <script type="text/javascript" src="js/analytics.js"></script>
    <script type="text/javascript">var cats = new Array();</script>
    <title>index</title>
  </head>
  <body>

	

  <%
    int userId = (Integer)request.getSession().getAttribute("userId");
    boolean useMoviePosters = (Boolean)request.getSession().getAttribute("useMoviePosters");
    
    //CustomerDAO customerDAO = new CustomerDAO();
    List<GenreMovieTO> genreMovieList = null;
    ActivityDao aDAO = new ActivityDao();
    //ActivityDAO aDao = new ActivityDAO();
    int movN = 0;
    
    //GenreDAO genreDAO = new GenreDAO();
    //List<GenreTO> genresShow =  genreDAO.getGenres();
  %>
  <div id="loading"><img src="images/wait.gif" alt="Loading..."></img></div>
  <div class="header">
    <div id="logo"><img src="images/movielogo_gray_index2.PNG" alt="Logo"></img></div>
    <div class="customerInformation">
    Welcome <%= (String)request.getSession().getAttribute("name") %> | <a href="logout">Logout</a>
    </div>
  </div>
  
  <div id="main">
    
    <form action="search.jsp" method="post">
    	<input name="keyword" /><br/>
    	<input type="submit" value="search" />
    </form>
    
    <div class="wbox dark">
      <div id="basedMood">
      <div class="titbar">
        <div class="tit">Movies based on Mood <input type="button" value="Get Similar Mood Movies" id="getMoviesMood"></div>
      </div>
      <div class="slidebox" id="slideboxMood">
      </div>
      </div>
      </div>
 
   <%
      ///////////////////////WHAT OTHERS WATHCHING///////////////////////
     // movieTOList1 = aDAO.getCommonPlayList();
   List<MovieTO> movieTOList1 = null;
      if (movieTOList1.size()>0){
    %>
    <div class="wbox dark">
      <div id="continueWatching">
      <div class="titbar">
        <div class="tit">What Others Watching</div>
      </div>
      <script type="text/javascript">$(document).ready(function(){createCarousel("CP");});</script>
      
      <div class="slidebox">
        <% if(movieTOList1.size() > 5) { %>
        <a id="prevSlideCP" class="arrow lef" ><span></span></a>
        <a id="nextSlideCP" class="arrow rig"><span></span></a>
        <% } %>
        <div class="box">
          <div class="in jspScrollable" style="overflow: hidden; padding: 0px; width: 782px; outline: none; ">
          <div class="jspContainer" style="width: 782px; height: 216px; ">
            <div class="rightpane"></div><div class="leftpane"></div>
            <div class="jspPane" style="padding: 0px; width: 782px; left: 0px; ">
                <div class="boxes">
                <ul id="mycarouselCP" class="jcarousel-skin-name">
                 <%
                    for (int i =0; i < movieTOList1.size(); i++){
                        MovieTO movieTO = movieTOList1.get(i);
                        %>
                     <li class="item-box jcarousel-item-<%= i %>"  rel="movieInformation.jsp?id=<%= movieTO.getId() %>">
                       <a href="#">
                      <span class="img loaded">
                          <% if (movieTO.getPosterPath().equalsIgnoreCase("") || !useMoviePosters) { %>
                          <div class="" style="height: 184px; overflow: hidden; position: relative; " title="<%= movieTO.getTitle() %>">
                          <img src="images/genericfilm.png" name="genericfilm.png" align="display: block; " class="reflected">
                          <div class="tit3"><%= movieTO.getTitle() %></div>                          
                          
                          <% } else { %>
                          <div class="" style="height: 184px; overflow: hidden; ">
                          <img src="<%= Constant.TMDb_IMG_URL + movieTO.getPosterPath() %>" name="<%= Constant.TMDb_IMG_URL + movieTO.getPosterPath() %>" style="display: block; " class="reflected">

                          <% } %>
                          </div>
                      </span>
                      <br><span class="tit2"><%= movieTO.getTitle() %></span><br>
                    </a>
                  </li>
                <% } %>
                </ul>
                </div>
              </div>
            </div>
          </div>
          </div>
          </div>
        <div class="clear"></div>
      </div>
      </div>
    <% }%>
    
    
      <%
      ///////////////////////PRIVIOUSLY WATCHED//////////////////////
      movieTOList1 = aDAO.getCustomerHistoricWatchList(userId);
      if (movieTOList1.size()>0){
    %>
    <div class="wbox dark">
      <div id="continueWatching">
      <div class="titbar">
        <div class="tit">Previously Watched</div>
      </div>
      <script type="text/javascript">$(document).ready(function(){createCarousel("PW");});</script>
      
      <div class="slidebox">
        <% if(movieTOList1.size() > 5) { %>
        <a id="prevSlidePW" class="arrow lef" ><span></span></a>
        <a id="nextSlidePW" class="arrow rig"><span></span></a>
        <% } %>
        <div class="box">
          <div class="in jspScrollable" style="overflow: hidden; padding: 0px; width: 782px; outline: none; height:230px; ">
          <div class="jspContainer" style="width: 782px; height: 230px; ">
            <div class="rightpane"></div><div class="leftpane"></div>
            <div class="jspPane" style="padding: 0px; width: 782px; left: 0px; ">
                <div class="boxes">
                <ul id="mycarouselPW" class="jcarousel-skin-name">
                 <%
                    for (int i =0; i < movieTOList1.size(); i++){
                        MovieTO movieTO = movieTOList1.get(i);
                        %>
                     <li style="height:240px;" class="item-box jcarousel-item-<%= i %>"  rel="movieInformation.jsp?id=<%= movieTO.getId() %>">
                       <a href="popup">
                      <span class="img loaded">
                          <% if (movieTO.getPosterPath().equalsIgnoreCase("") || !useMoviePosters) { %>
                          <div class="" style="height: 184px; overflow: hidden; position: relative; " title="<%= movieTO.getTitle() %>">
                          <img src="images/genericfilm.png" name="genericfilm.png" align="display: block; " class="reflected">
                          <div class="tit3"><%= movieTO.getTitle() %></div>                          
                          
                          <% } else { %>
                          <div class="" style="height: 184px; overflow: hidden; ">
                          <img src="<%= Constant.TMDb_IMG_URL + movieTO.getPosterPath() %>" name="<%= Constant.TMDb_IMG_URL + movieTO.getPosterPath() %>" style="display: block; " class="reflected">

                          <% } %>
                          </div>
                      </span>
                      <br><span class="tit2"><%= movieTO.getTitle() %></span>
                    </a>
                    <% 
                    //ActivityTO activityTO = customerDAO.getMovieRating(userId,movieTO.getId());
                    ActivityTO activityTO = aDAO.getActivityTO(userId,movieTO.getId());
                    
                    double avg = 1;
                    boolean rated = false;
                    if(activityTO != null && activityTO.getRating()!=null){
                      avg = activityTO.getRating().getValue();
                      rated = true;
                    }else
                      avg =  movieTO.getPopularity()/2;
                    %>
                        <input name="star-pw<%=i%>" type="radio" class="star-pw<%=i%> star required" value="1" <% if (avg > 0 && avg <= 1.5 ) out.print("checked='checked'"); %>/>
                        <input name="star-pw<%=i%>" type="radio" class="star-pw<%=i%> star" value="2" <% if (avg > 1.5 && avg <= 2.5 ) out.print("checked='checked'"); %>/>
                        <input name="star-pw<%=i%>" type="radio" class="star-pw<%=i%> star" value="3" <% if (avg > 2.5 && avg <= 3.5 ) out.print("checked='checked'"); %>/>
                        <input name="star-pw<%=i%>" type="radio" class="star-pw<%=i%> star" value="4" <% if (avg > 3.5 && avg <= 4.5 ) out.print("checked='checked'"); %> />
                        <input name="star-pw<%=i%>" type="radio" class="star-pw<%=i%> star" value="5" <% if (avg > 4.5) out.print("checked='checked'"); %> />
                        <script type="text/javascript">setRating(<%=rated%>,"star-pw<%=i%>",<%=movieTO.getId()%>);</script>

                    </li>
                <% } %>
                </ul>
                </div>
              </div>
            </div>
          </div>
          </div>
          </div>
        <div class="clear"></div>
      </div>
      </div>
    <% }%>
    </div> 
    