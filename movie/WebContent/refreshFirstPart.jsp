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
      ///////////////////////CONTINUE WATCHING///////////////////////
      List<MovieTO> movieTOList1 = null;
      movieTOList1 = aDAO.getCustomerCurrentWatchList(userId);
      
      if (movieTOList1.size()>0){
    %>
    <div class="wbox dark">
      <div id="continueWatching">
      <div class="titbar">
        <div class="tit">Continue Watching</div>
      </div>
      <script type="text/javascript">$(document).ready(function(){createCarousel("CW");});</script>
      
      <div class="slidebox">
        <% if(movieTOList1.size() > 5) { %>
        <a id="prevSlideCW" class="arrow lef" ><span></span></a>
        <a id="nextSlideCW" class="arrow rig"><span></span></a>
        <% } %>
        <div class="box">
          <div class="in jspScrollable" style="overflow: hidden; padding: 0px; width: 782px; outline: none; height:230px;">
          <div class="jspContainer" style="width: 782px; height: 230px; ">
            <div class="rightpane"></div><div class="leftpane"></div>
            <div class="jspPane" style="padding: 0px; width: 782px; left: 0px; ">
                <div class="boxes">
                <ul id="mycarouselCW" class="jcarousel-skin-name">
                 <%
                    for (int i =0; i < movieTOList1.size(); i++){
                        MovieTO movieTO = movieTOList1.get(i);
                        %>
                     <li style="height:240px;" class="item-box jcarousel-item-<%= i %>" movieid="<%= movieTO.getId() %>" rel="movieInformation.jsp?id=<%= movieTO.getId() %>">
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
                      <br><span class="tit2"><%= movieTO.getTitle() %></span>
                      <%
                      ActivityTO activityTO = new ActivityDao().getActivityTO(userId, movieTO.getId());
                      int position = 0;
                      int total = 226;
                      if(activityTO!=null) position = activityTO.getPosition();
                      int actual = position*100/total;
                      String youtubeKey = YouTubeUtil.getKey(movieTO.getId()); 
                      %>
                      <div class="meter" yt="<%=youtubeKey%>"><span style="width: <%=actual%>%"></span></div>
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
      ///////////////////////RECENTLY BROWSED///////////////////////
      movieTOList1 = aDAO.getCustomerBrowseList(userId);
      
      if (movieTOList1.size()>0){
    %>
    <div class="wbox dark">
      <div id="continueWatching">
      <div class="titbar">
        <div class="tit">Recently Browsed</div>
      </div>
      <script type="text/javascript">$(document).ready(function(){createCarousel("RW");});</script>
      
      <div class="slidebox">
         <% if(movieTOList1.size() > 5) { %>
        <a id="prevSlideRW" class="arrow lef" ><span></span></a>
        <a id="nextSlideRW" class="arrow rig"><span></span></a>
        <% } %>
        <div class="box">
          <div class="in jspScrollable" style="overflow: hidden; padding: 0px; width: 782px; outline: none; ">
          <div class="jspContainer" style="width: 782px; height: 216px; ">
            <div class="rightpane"></div><div class="leftpane"></div>
            <div class="jspPane" style="padding: 0px; width: 782px; left: 0px; ">
                <div class="boxes">
                <ul id="mycarouselRW" class="jcarousel-skin-name">
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
