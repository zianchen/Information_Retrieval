<!DOCTYPE html>
<!--<html>
	<head>
		<style type="text/css">
		body {background-image:url(/home/qsing/website/background.png);}
		</style>
    </head>
	<body>
		<img src="search.jpg" alt="yunqinz.com" width="104" height="142">
		<form action = "search.php" method = "post"><br>
			<input type="text" name="keyword"><br>
			<input type="submit" value="Search">
		</form>
	</body>
</html>
-->

<!DOCTYPE html>
<html>
	<head>
		<style type="text/css">
		div#div1{
    		position:fixed;
    		top:0;
    		left:0;
    		bottom:0;
    		right:0;
    		z-index:-1;
		} 
		div#div1 > img {
  		  height:100%;
  		  width:100%;
   		  border:0;
		}
		.wrapper {
            
            height:30%;
            margin-top: auto;
    		margin-left: auto;
    		margin-right: auto;
    		width: 30%;


           
        }

        input[type="text"] {
             display: block;
             margin-left : 800 ;
             margin-top:1000;
             width :350px;
             height:25px;
             
        }

		
		</style>
	</head>
	
    
	<body>
		<div id="div1"><img src="cu.jpg" /></div> 
		<br><br><br><br><br><br><br><br><br><br>
		<div align ="center"><img src="logo1.png" alt="yunqinz.com"width="450" height="100">
		</div>
		<form action = "search.php" method = "post"><br>
			<!--<input type="text"  name="keyword" text-align:center;><br>-->
			<div class='wrapper'>
       			 <input type='text' name='keyword'>
    		 
    				<br>
				&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp
				&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp
				<input type="submit" value="Search">
			</div>
		</form>
	</body>
</html>