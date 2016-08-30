#Saving geoJSON Data To Cassandra Using User-Defined Types, Spark Dataframes and Spark SQL

The geoJSON data format is described at geojson.org as "<em><b>a format for encoding a variety of geographic data structures</b></em>".

In this example I'll be using a set of oil/gas well data supplied by the State of Colorado describing approx 110,000 wells in the state.

I'll start out with some examples of how to manipulate the data loaded into a dataframe, followed by the complete exercise to clean up and store the JSON data in cassandra.

The format of the geoJSON source data looks like this:
<pre lang="javascript">
{
	"type": "Feature",
	"properties": {
		"API": "12508888",
		"API_Label": "05-125-08888",
		"Operator": "OMIMEX PETROLEUM INC",
		"Well_Title": "8-9-5-45 FERGUSON",
		"Facil_Id": 0,                          <-- integer
		"Facil_Type": "WELL",
		"Facil_Stat": "PR",
		"Operat_Num": 66190,                    <-- big int
		"Well_Num": "8-9-5-45",
		"Well_Name": "FERGUSON",
		"Field_Code": 1970,                     <-- integer
		"Dist_N_S": 1980,                       <-- integer
		"Dir_N_S": "N",
		"Dist_E_W": 600,                        <-- integer
		"Dir_E_W": "E",
		"Qtr_Qtr": "SENE",
		"Section": "9",
		"Township": "5N",
		"Range": "45W",
		"Meridian": "6",
		"Latitude": 40.419416,                  <-- decimal
		"Longitude": -102.379999,               <-- decimal
		"Ground_Ele": 0,                        <-- decimal
		"Utm_X": 722281,                        <-- decimal
		"Utm_Y": 4477606,                       <-- decimal
		"Loc_Qual": "PLANNED Footage",
		"Field_Name": "BALLYNEAL",
		"Api_Seq": "08888",
		"API_County": "125",
		"Loc_ID": 304702,                       <-- decimal
		"Loc_Name": "FERGUSON-65N45W 9SENE",
		"Spud_Date": "2004\/09\/07",
		"Citing_Typ": "ACTUAL",
		"Max_MD": 2727,                         <-- decimal
		"Max_TVD": 2727                         <-- decimal
	},
	"geometry": {
		"type": "Point",
		"coordinates": [722281.0, 4477606.0]
	}
}
</pre>

Our objective is to load the data into Cassandra so that we can use it in other applications. For this we are going to use Apache Spark, via DataFrames and Spark SQL.

For this exercise I'm using DataStax Enterprise 5.0.1 that contains Apache Cassandra 3.0.7 and comes integrated with a distribution of Apache Spark 1.6.1
<h2>Start The Spark REPL</h2>
<pre># dse spark
Welcome to
____ __
/ __/__ ___ _____/ /__
_\ \/ _ \/ _ `/ __/ '_/
/___/ .__/\_,_/_/ /_/\_\ version 1.6.1
/_/

Using Scala version 2.10.5 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_72)
Type in expressions to have them evaluated.
Type :help for more information.
Initializing SparkContext with MASTER: spark://127.0.0.1:7077
Created spark context..
Spark context available as sc.
Hive context available as sqlContext. Will be initialized on first use.
</pre>

<h3>Read The json file into a Spark Dataframe</h3>
Using the 'read' method gives us the json file in a dataframe:</span>
<pre lang="scala">
scala> val df = sqlContext.read.json("file:///tmp/wells_geoJSON.geojson")
</pre>
You will get some system output like this:
<pre lang="scala">
df: org.apache.spark.sql.DataFrame = [_corrupt_record: string, geometry: struct&lt;coordinates:array,type:string&gt;, properties: struct&lt;API:string,API_County:string,API_Label:string,Api_Seq:string,Citing_Typ:string,Dir_E_W:string,Dir_N_S:string,Dist_E_W:bigint,Dist_N_S:bigint,Facil_Id:bigint,Facil_Stat:string,Facil_Type:string,Field_Code:bigint,Field_Name:string,Ground_Ele:bigint,Latitude:double,Loc_ID:bigint,Loc_Name:string,Loc_Qual:string,Longitude:double,Max_MD:bigint,Max_TVD:bigint,Meridian:string,Operat_Num:bigint,Operator:string,Qtr_Qtr:string,Range:string,Section:string,Spud_Date:string,Township:string,Utm_X:bigint,Utm_Y:bigint,Well_Name:string,Well_Num:string,Well_Title:string&gt;, type: string]
</pre>

We can now look at the schema of the JSON data that we've loaded into the dataframe:
<pre>
scala> df.printSchema()

root
|-- _corrupt_record: string (nullable = true)
|-- geometry: struct (nullable = true)
| |-- coordinates: array (nullable = true)
| | |-- element: double (containsNull = true)
| |-- type: string (nullable = true)
|-- properties: struct (nullable = true)
| |-- API: string (nullable = true)
| |-- API_County: string (nullable = true)
| |-- API_Label: string (nullable = true)
| |-- Api_Seq: string (nullable = true)
| |-- Citing_Typ: string (nullable = true)
| |-- Dir_E_W: string (nullable = true)
| |-- Dir_N_S: string (nullable = true)
| |-- Dist_E_W: long (nullable = true)
| |-- Dist_N_S: long (nullable = true)
| |-- Facil_Id: long (nullable = true)
| |-- Facil_Stat: string (nullable = true)
| |-- Facil_Type: string (nullable = true)
| |-- Field_Code: long (nullable = true)
| |-- Field_Name: string (nullable = true)
| |-- Ground_Ele: long (nullable = true)
| |-- Latitude: double (nullable = true)
| |-- Loc_ID: long (nullable = true)
| |-- Loc_Name: string (nullable = true)
| |-- Loc_Qual: string (nullable = true)
| |-- Longitude: double (nullable = true)
| |-- Max_MD: long (nullable = true)
| |-- Max_TVD: long (nullable = true)
| |-- Meridian: string (nullable = true)
| |-- Operat_Num: long (nullable = true)
| |-- Operator: string (nullable = true)
| |-- Qtr_Qtr: string (nullable = true)
| |-- Range: string (nullable = true)
| |-- Section: string (nullable = true)
| |-- Spud_Date: string (nullable = true)
| |-- Township: string (nullable = true)
| |-- Utm_X: long (nullable = true)
| |-- Utm_Y: long (nullable = true)
| |-- Well_Name: string (nullable = true)
| |-- Well_Num: string (nullable = true)
| |-- Well_Title: string (nullable = true)
|-- type: string (nullable = true)
</pre>

And we can examine the data that has been read:
<pre>
scala> df.show()

+--------------------+--------------------+--------------------+-------+
|     _corrupt_record|            geometry|          properties|   type|
+--------------------+--------------------+--------------------+-------+
|                   {|                null|                null|   null|
|"type": "FeatureC...|                null|                null|   null|
|"crs": { "type": ...|                null|                null|   null|
|       "features": [|                null|                null|   null|
|                null|[WrappedArray(722...|[12508888,125,05-...|Feature|
|                null|[WrappedArray(524...|[12323461,123,05-...|Feature|
|                null|[WrappedArray(530...|[12323462,123,05-...|Feature|
|                null|[WrappedArray(523...|[12323463,123,05-...|Feature|
|                null|[WrappedArray(523...|[12323464,123,05-...|Feature|
|                null|[WrappedArray(235...|[04511663,045,05-...|Feature|
|                null|[WrappedArray(235...|[04511664,045,05-...|Feature|
|                null|[WrappedArray(236...|[04511665,045,05-...|Feature|
|                null|[WrappedArray(236...|[04511666,045,05-...|Feature|
|                null|[WrappedArray(244...|[04511667,045,05-...|Feature|
|                null|[WrappedArray(524...|[12323467,123,05-...|Feature|
|                null|[WrappedArray(494...|[01306522,013,05-...|Feature|
|                null|[WrappedArray(244...|[04511668,045,05-...|Feature|
|                null|[WrappedArray(244...|[04511669,045,05-...|Feature|
|                null|[WrappedArray(244...|[04511670,045,05-...|Feature|
|                null|[WrappedArray(245...|[04511671,045,05-...|Feature|
+--------------------+--------------------+--------------------+-------+
only showing top 20 rows
</pre>

When the data is in the dataframe we can register it as a SparkSQL table (I've called it "jsontable") so that we can select data using SQL e.g. API and geometry coordinates for all wells into another dataframe called "well_locs":

<pre>
scala> df.registerTempTable("jsonTable")

scala> val well_locs = sqlContext.sql("SELECT properties.API, geometry.coordinates FROM jsontable")
well_locs: org.apache.spark.sql.DataFrame = [API: string, coordinates: array]

scala> well_locs.show()

+--------+--------------------+
|     API|         coordinates|
+--------+--------------------+
|    null|                null|
|    null|                null|
|    null|                null|
|    null|                null|
|12508888|[722281.0, 447760...|
|12323461|[524048.0, 444462...|
|12323462|[530187.0, 445971...|
|12323463|[523218.0, 444455...|
|12323464|[523598.0, 444340...|
|04511663|[235668.0, 437192...|
|04511664|[235672.0, 437193...|
|04511665|[236287.0, 437168...|
|04511666|[236284.0, 437168...|
|04511667|[244604.0, 437456...|
|12323467|[524594.0, 445802...|
|01306522|[494622.0, 444139...|
|04511668|[244666.0, 437484...|
|04511669|[244661.0, 437484...|
|04511670|[244656.0, 437484...|
|04511671|[245144.0, 437490...|
+--------+--------------------+
only showing top 20 rows
</pre>

There are some null records in there that we don't want to see. We can create "well_locs" again and exclude those null records from the query results:

<pre>
scala> val well_locs = sqlContext.sql("SELECT properties.API, geometry.coordinates FROM jsontable where properties.API is not null")
well_locs: org.apache.spark.sql.DataFrame = [API: string, coordinates: array]

scala> well_locs.show()

+--------+--------------------+
|     API|         coordinates|
+--------+--------------------+
|12508888|[722281.0, 447760...|
|12323461|[524048.0, 444462...|
|12323462|[530187.0, 445971...|
|12323463|[523218.0, 444455...|
|12323464|[523598.0, 444340...|
|04511663|[235668.0, 437192...|
|04511664|[235672.0, 437193...|
|04511665|[236287.0, 437168...|
|04511666|[236284.0, 437168...|
|04511667|[244604.0, 437456...|
|12323467|[524594.0, 445802...|
|01306522|[494622.0, 444139...|
|04511668|[244666.0, 437484...|
|04511669|[244661.0, 437484...|
|04511670|[244656.0, 437484...|
|04511671|[245144.0, 437490...|
|04511672|[245141.0, 437490...|
|04511673|[265880.0, 437586...|
|10310665|[204771.0, 441822...|
|04511674|[233487.0, 438827...|
+--------+--------------------+
only showing top 20 rows
</pre>

Spark also doesn't understand the first element (column) in the geoJSON structure, the FeatureCollection wrapper, and shows it as a corrupt column:

<pre>
scala> val x = sqlContext.sql("SELECT _corrupt_record FROM jsontable")
x: org.apache.spark.sql.DataFrame = [_corrupt_record: string]

scala> x.show()

+--------------------+
|     _corrupt_record|
+--------------------+
|                   {|
|"type": "FeatureC...|
|"crs": { "type": ...|
|       "features": [|
|                null|
|                null|
|                null|
|                null|
|                null|
|                null|
|                null|
|                null|
|                null|
|                null|
|                null|
|                null|
|                null|
|                null|
|                null|
|                null|
+--------------------+
only showing top 20 rows
</pre>

We can use Spark SQL to extract geoJSON properties from our Spark SQL table - lets look at the API values for the wells in the dataset:

<pre lang="scala">
scala> val API = sqlContext.sql("SELECT properties.API FROM jsonTable")
API: org.apache.spark.sql.DataFrame = [API: string]

scala> API.collect.foreach(println)
</pre>
-> prints all ~84,000 API numbers

There may be null records in the source data - these are evident as nulls at the beginning and the end, and we need to remove them before we write the data to the Cassandra database. We can see the nulls at the start:

<pre>
scala> API.collect.take(10).foreach(println)

[null]
[null]
[null]
[null]
[12508888]
[12323461]
[12323462]
[12323463]
[12323464]
[04511663]
</pre>

There are some nulls at the end too.
We can use Spark SQL to extract the well API numbers from the table and count them - actually there are two ways we can do this: (1) create a new dataframe and count the rows, or (2) directly count the rows in the table using SQL:

<pre lang="scala">
scala> val API = sqlContext.sql("SELECT properties.API FROM jsonTable")
scala> API.count
res17: Long = 110280
</pre>
or
<pre lang="scala">
scala> sqlContext.sql("SELECT count (*) FROM jsonTable").show
+------+
|   _c0|
+------+
|110280|
+------+
</pre>

Do the same thing, removing null records - we can see that the 6 null records have been removed.
With a dataframe:
<pre lang="scala">
scala> val API = sqlContext.sql("SELECT properties.API FROM jsonTable where properties.API is not null")
API: org.apache.spark.sql.DataFrame = [API: string]

scala> API.count
res21: Long = 110274
</pre>
...and with SQL:
<pre lang="scala">
scala> sqlContext.sql("SELECT count (*) FROM jsonTable where properties.API is not null").show
+------+                                                                        
|   _c0|
+------+
|110274|
+------+

</pre>

<H2>Tidy Up The geoJSON Data And Save It To Cassandra</H2>

OK, so we've seen how we can manipulate and examine the data in our source geoJSON data that we loaded into a Spark dataframe and then represented as a Spark SQL table.
Now we can go through the process of cleaning up the data and formatting it correctly so that we can load it into Cassandra.

<H3>1. Process The geoJSON Source File To Convert All Field (Column) Names To Lower Case</H3>
There is an issue with column names when saving to Cassandra if the column names are not in lower case (this may be a problem with the Spark Cassandra connector, or it could be a problem with Spark itself - I'm using Spark 1.6.1 here).
So we first need to convert our column names to lower case. I've used a script containing the following sed commands:
<pre>
sed -i -e 's/"Api_Seq"/"api_seq"/' wells_geoJSON.geojson
sed -i -e 's/"API_County"/"api_county"/' wells_geoJSON.geojson
sed -i -e 's/"API_Label"/"api_label"/' wells_geoJSON.geojson
sed -i -e 's/"API"/"api"/' wells_geoJSON.geojson
sed -i -e 's/"Api"/"api"/' wells_geoJSON.geojson
sed -i -e 's/"APi"/"api"/' wells_geoJSON.geojson
sed -i -e 's/"Operator"/"operator"/' wells_geoJSON.geojson
sed -i -e 's/"Well_Title"/"well_title"/' wells_geoJSON.geojson
sed -i -e 's/"Facil_Id"/"facil_id"/' wells_geoJSON.geojson
sed -i -e 's/"Facil_Type"/"facil_type"/' wells_geoJSON.geojson
sed -i -e 's/"Facil_Stat"/"facil_stat"/' wells_geoJSON.geojson
sed -i -e 's/"Operat_Num"/"operat_num"/' wells_geoJSON.geojson
sed -i -e 's/"Well_Num"/"well_num"/' wells_geoJSON.geojson
sed -i -e 's/"Well_Name"/"well_name"/' wells_geoJSON.geojson
sed -i -e 's/"Field_Code"/"field_code"/' wells_geoJSON.geojson
sed -i -e 's/"Dist_N_S"/"dist_n_s"/' wells_geoJSON.geojson
sed -i -e 's/"Dir_N_S"/"dir_n_s"/' wells_geoJSON.geojson
sed -i -e 's/"Dist_E_W"/"dist_e_w"/' wells_geoJSON.geojson
sed -i -e 's/"Dir_E_W"/"dir_e_w"/' wells_geoJSON.geojson
sed -i -e 's/"Qtr_Qtr"/"qtr_qtr"/' wells_geoJSON.geojson
sed -i -e 's/"Section"/"section"/' wells_geoJSON.geojson
sed -i -e 's/"Township"/"township"/' wells_geoJSON.geojson
sed -i -e 's/"Range"/"range"/' wells_geoJSON.geojson
sed -i -e 's/"Meridian"/"meridian"/' wells_geoJSON.geojson
sed -i -e 's/"Latitude"/"latitude"/' wells_geoJSON.geojson
sed -i -e 's/"Longitude"/"longitude"/' wells_geoJSON.geojson
sed -i -e 's/"Ground_Ele"/"ground_ele"/' wells_geoJSON.geojson
sed -i -e 's/"Utm_X"/"utm_x"/' wells_geoJSON.geojson
sed -i -e 's/"Utm_Y"/"utm_y"/' wells_geoJSON.geojson
sed -i -e 's/"Loc_Qual"/"loc_qual"/' wells_geoJSON.geojson
sed -i -e 's/"Field_Name"/"field_name"/' wells_geoJSON.geojson
sed -i -e 's/"Loc_ID"/"loc_id"/' wells_geoJSON.geojson
sed -i -e 's/"Loc_Name"/"loc_name"/' wells_geoJSON.geojson
sed -i -e 's/"Spud_Date"/"spud_date"/' wells_geoJSON.geojson
sed -i -e 's/"Citing_Typ"/"citing_typ"/' wells_geoJSON.geojson
sed -i -e 's/"Max_MD"/"max_md"/' wells_geoJSON.geojson
sed -i -e 's/"Max_TVD"/"max_tvd"/' wells_geoJSON.geojson
</pre>

<H3>2. Load The geoJSON File</H3>
<pre lang="scala">
scala> val df = sqlContext.read.json("file:///tmp/wells_geoJSON.geojson")
</pre>

<H3>3. Remove The Corrupt Record</H3>
The FeatureCollection wrapper of the JSON object isn't understood and needs to be removed:

<pre>
scala> df.printSchema()

root
 |-- _corrupt_record: string (nullable = true)
 |-- geometry: struct (nullable = true)
 |    |-- coordinates: array (nullable = true)
 |    |    |-- element: double (containsNull = true)
 |    |-- type: string (nullable = true)
 |-- properties: struct (nullable = true)
 |    |-- api: string (nullable = true)
 |    |-- api_county: string (nullable = true)
 |    |-- api_label: string (nullable = true)
 |    |-- api_seq: string (nullable = true)
 |    |-- citing_typ: string (nullable = true)
 |    |-- dir_e_w: string (nullable = true)
 |    |-- dir_n_s: string (nullable = true)
 |    |-- dist_e_w: long (nullable = true)
 |    |-- dist_n_s: long (nullable = true)
 |    |-- facil_id: long (nullable = true)
 |    |-- facil_stat: string (nullable = true)
 |    |-- facil_type: string (nullable = true)
 |    |-- field_code: long (nullable = true)
 |    |-- field_name: string (nullable = true)
 |    |-- ground_ele: long (nullable = true)
 |    |-- latitude: double (nullable = true)
 |    |-- loc_id: long (nullable = true)
 |    |-- loc_name: string (nullable = true)
 |    |-- loc_qual: string (nullable = true)
 |    |-- longitude: double (nullable = true)
 |    |-- max_md: long (nullable = true)
 |    |-- max_tvd: long (nullable = true)
 |    |-- meridian: string (nullable = true)
 |    |-- operat_num: long (nullable = true)
 |    |-- operator: string (nullable = true)
 |    |-- qtr_qtr: string (nullable = true)
 |    |-- range: string (nullable = true)
 |    |-- section: string (nullable = true)
 |    |-- spud_date: string (nullable = true)
 |    |-- township: string (nullable = true)
 |    |-- utm_x: long (nullable = true)
 |    |-- utm_y: long (nullable = true)
 |    |-- well_name: string (nullable = true)
 |    |-- well_num: string (nullable = true)
 |    |-- well_title: string (nullable = true)
 |-- type: string (nullable = true)

scala> df.show()

+--------------------+--------------------+--------------------+-------+
|     _corrupt_record|            geometry|          properties|   type|
+--------------------+--------------------+--------------------+-------+
|                   {|                null|                null|   null|
|"type": "FeatureC...|                null|                null|   null|
|"crs": { "type": ...|                null|                null|   null|
|       "features": [|                null|                null|   null|
|                null|[WrappedArray(722...|[12508888,125,05-...|Feature|
|                null|[WrappedArray(524...|[12323461,123,05-...|Feature|
|                null|[WrappedArray(530...|[12323462,123,05-...|Feature|
|                null|[WrappedArray(523...|[12323463,123,05-...|Feature|
|                null|[WrappedArray(523...|[12323464,123,05-...|Feature|
|                null|[WrappedArray(235...|[04511663,045,05-...|Feature|
|                null|[WrappedArray(235...|[04511664,045,05-...|Feature|
|                null|[WrappedArray(236...|[04511665,045,05-...|Feature|
|                null|[WrappedArray(236...|[04511666,045,05-...|Feature|
|                null|[WrappedArray(244...|[04511667,045,05-...|Feature|
|                null|[WrappedArray(524...|[12323467,123,05-...|Feature|
|                null|[WrappedArray(494...|[01306522,013,05-...|Feature|
|                null|[WrappedArray(244...|[04511668,045,05-...|Feature|
|                null|[WrappedArray(244...|[04511669,045,05-...|Feature|
|                null|[WrappedArray(244...|[04511670,045,05-...|Feature|
|                null|[WrappedArray(245...|[04511671,045,05-...|Feature|
+--------------------+--------------------+--------------------+-------+
only showing top 20 rows
</pre>

Register the dataframe as a SparkSQL table so that we can see it more easily:

<pre lang="scala">
scala> df.registerTempTable("jsonTable");
</pre>

<H3>4. Drop the _corrupt_record column from the original dataframe:</H3>
We need to get rid of that _corrupt_record column - we do that using the dataframe drop method.
<pre>
scala> val df2=df.drop(df.col("_corrupt_record"))
df2: org.apache.spark.sql.DataFrame = [geometry: struct&lt;coordinates:array,type:string&gt;, properties: struct&lt;API:string,API_County:string,API_Label:string,Api_Seq:string,Citing_Typ:string,Dir_E_W:string,Dir_N_S:string,Dist_E_W:bigint,Dist_N_S:bigint,Facil_Id:bigint,Facil_Stat:string,Facil_Type:string,Field_Code:bigint,Field_Name:string,Ground_Ele:bigint,Latitude:double,Loc_ID:bigint,Loc_Name:string,Loc_Qual:string,Longitude:double,Max_MD:bigint,Max_TVD:bigint,Meridian:string,Operat_Num:bigint,Operator:string,Qtr_Qtr:string,Range:string,Section:string,Spud_Date:string,Township:string,Utm_X:bigint,Utm_Y:bigint,Well_Name:string,Well_Num:string,Well_Title:string&gt;, type: string]

scala> df2.show()

+--------------------+--------------------+-------+
|            geometry|          properties|   type|
+--------------------+--------------------+-------+
|                null|                null|   null|
|                null|                null|   null|
|                null|                null|   null|
|                null|                null|   null|
|[WrappedArray(722...|[12508888,125,05-...|Feature|
|[WrappedArray(524...|[12323461,123,05-...|Feature|
|[WrappedArray(530...|[12323462,123,05-...|Feature|
|[WrappedArray(523...|[12323463,123,05-...|Feature|
|[WrappedArray(523...|[12323464,123,05-...|Feature|
|[WrappedArray(235...|[04511663,045,05-...|Feature|
|[WrappedArray(235...|[04511664,045,05-...|Feature|
|[WrappedArray(236...|[04511665,045,05-...|Feature|
|[WrappedArray(236...|[04511666,045,05-...|Feature|
|[WrappedArray(244...|[04511667,045,05-...|Feature|
|[WrappedArray(524...|[12323467,123,05-...|Feature|
|[WrappedArray(494...|[01306522,013,05-...|Feature|
|[WrappedArray(244...|[04511668,045,05-...|Feature|
|[WrappedArray(244...|[04511669,045,05-...|Feature|
|[WrappedArray(244...|[04511670,045,05-...|Feature|
|[WrappedArray(245...|[04511671,045,05-...|Feature|
+--------------------+--------------------+-------+
only showing top 20 rows
</pre>


<H3>5. Filter Out The Null Records</H3>
Remove the unwanted null records from the data set:
<pre>
scala> val df3=df2.filter("type is not null")
scala> df3.show()

+--------------------+--------------------+-------+
|            geometry|          properties|   type|
+--------------------+--------------------+-------+
|[WrappedArray(722...|[12508888,125,05-...|Feature|
|[WrappedArray(524...|[12323461,123,05-...|Feature|
|[WrappedArray(530...|[12323462,123,05-...|Feature|
|[WrappedArray(523...|[12323463,123,05-...|Feature|
|[WrappedArray(523...|[12323464,123,05-...|Feature|
|[WrappedArray(235...|[04511663,045,05-...|Feature|
|[WrappedArray(235...|[04511664,045,05-...|Feature|
|[WrappedArray(236...|[04511665,045,05-...|Feature|
|[WrappedArray(236...|[04511666,045,05-...|Feature|
|[WrappedArray(244...|[04511667,045,05-...|Feature|
|[WrappedArray(524...|[12323467,123,05-...|Feature|
|[WrappedArray(494...|[01306522,013,05-...|Feature|
|[WrappedArray(244...|[04511668,045,05-...|Feature|
|[WrappedArray(244...|[04511669,045,05-...|Feature|
|[WrappedArray(244...|[04511670,045,05-...|Feature|
|[WrappedArray(245...|[04511671,045,05-...|Feature|
|[WrappedArray(245...|[04511672,045,05-...|Feature|
|[WrappedArray(265...|[04511673,045,05-...|Feature|
|[WrappedArray(204...|[10310665,103,05-...|Feature|
|[WrappedArray(233...|[04511674,045,05-...|Feature|
+--------------------+--------------------+-------+
only showing top 20 rows
</pre>
Check we have the correct record count:
<pre lang="scala">
scala> df3.count
res12: Long = 110274
</pre>
Create a new SparkSQL table for our new filtered dataframe df3:
<pre>
scala> df3.registerTempTable("jsonTable");
scala> val API = sqlContext.sql("SELECT properties.API FROM jsonTable")
API: org.apache.spark.sql.DataFrame = [API: string]

scala> API.show()
+--------+
|     API|
+--------+
|12508888|
|12323461|
|12323462|
|12323463|
|12323464|
|04511663|
|04511664|
|04511665|
|04511666|
|04511667|
|12323467|
|01306522|
|04511668|
|04511669|
|04511670|
|04511671|
|04511672|
|04511673|
|10310665|
|04511674|
+--------+
only showing top 20 rows
</pre>

<H3>6. Add A Primary Key Column</H3>

We need a primary key for our Cassandra table. I'm going to use the well API number as my partition key - it's unique and will distribute the data nicely around our cluster.

Here's how we add a column to a dataframe, selecting the values from the existing properties.API column:
<pre lang="scala">
scala> val df4 = df3.withColumn("api", df3("properties.API"))
</pre>
<b>NB</b> you can also add literal columns using this syntax:
<pre lang="scala">
dataframe.withColumn("newName",lit("newValue")) )
</pre>
After adding the new column we can see it in the schema - the API column is now a new column at the top level rather than a sub-element of properties:
<pre>
scala> df4.show()
+--------------------+--------------------+-------+--------+
|            geometry|          properties|   type|     api|
+--------------------+--------------------+-------+--------+
|[WrappedArray(722...|[12508888,125,05-...|Feature|12508888|
|[WrappedArray(524...|[12323461,123,05-...|Feature|12323461|
|[WrappedArray(530...|[12323462,123,05-...|Feature|12323462|
|[WrappedArray(523...|[12323463,123,05-...|Feature|12323463|
|[WrappedArray(523...|[12323464,123,05-...|Feature|12323464|
|[WrappedArray(235...|[04511663,045,05-...|Feature|04511663|
|[WrappedArray(235...|[04511664,045,05-...|Feature|04511664|
|[WrappedArray(236...|[04511665,045,05-...|Feature|04511665|
|[WrappedArray(236...|[04511666,045,05-...|Feature|04511666|
|[WrappedArray(244...|[04511667,045,05-...|Feature|04511667|
|[WrappedArray(524...|[12323467,123,05-...|Feature|12323467|
|[WrappedArray(494...|[01306522,013,05-...|Feature|01306522|
|[WrappedArray(244...|[04511668,045,05-...|Feature|04511668|
|[WrappedArray(244...|[04511669,045,05-...|Feature|04511669|
|[WrappedArray(244...|[04511670,045,05-...|Feature|04511670|
|[WrappedArray(245...|[04511671,045,05-...|Feature|04511671|
|[WrappedArray(245...|[04511672,045,05-...|Feature|04511672|
|[WrappedArray(265...|[04511673,045,05-...|Feature|04511673|
|[WrappedArray(204...|[10310665,103,05-...|Feature|10310665|
|[WrappedArray(233...|[04511674,045,05-...|Feature|04511674|
+--------------------+--------------------+-------+--------+
only showing top 20 rows
</pre>

<H3>7. Create A New SparkSQL Table For The Dataframe df4</h3>
Register a new SparkSQL table based on the new dataframe we just created that contains the primary key column we need:

<pre lang="scala">
scala> df4.registerTempTable("jsonTable")
</pre>
And again, we can check the data is there:
<pre>
scala> val API = sqlContext.sql("SELECT api, properties.api FROM jsonTable")
API: org.apache.spark.sql.DataFrame = [api: string, api: string]

scala> API.show()
+--------+--------+
|     api|     api|
+--------+--------+
|12508888|12508888|
|12323461|12323461|
|12323462|12323462|
|12323463|12323463|
|12323464|12323464|
|04511663|04511663|
|04511664|04511664|
|04511665|04511665|
|04511666|04511666|
|04511667|04511667|
|12323467|12323467|
|01306522|01306522|
|04511668|04511668|
|04511669|04511669|
|04511670|04511670|
|04511671|04511671|
|04511672|04511672|
|04511673|04511673|
|10310665|10310665|
|04511674|04511674|
+--------+--------+
only showing top 20 rows
</pre>

<h3>8. Use The SparkSQL Table To Build A New DF With The Columns In The Correct Order</h3>

We want the columns in our dataframe to match the layout of the table we are going to save into - we can achieve that by selecting the data into a new dataframe from our latest SparkSQL table:

<pre lang="scala">
scala> val wells=sqlContext.sql("SELECT api,geometry, properties, type from jsontable")

wells: org.apache.spark.sql.DataFrame = [api: string, geometry: struct<coordinates:array,type:string>, properties: struct<API:string,API_County:string,API_Label:string,Api_Seq:string,Citing_Typ:string,Dir_E_W:string,Dir_N_S:string,Dist_E_W:bigint,Dist_N_S:bigint,Facil_Id:bigint,Facil_Stat:string,Facil_Type:string,Field_Code:bigint,Field_Name:string,Ground_Ele:bigint,Latitude:double,Loc_ID:bigint,Loc_Name:string,Loc_Qual:string,Longitude:double,Max_MD:bigint,Max_TVD:bigint,Meridian:string,Operat_Num:bigint,Operator:string,Qtr_Qtr:string,Range:string,Section:string,Spud_Date:string,Township:string,Utm_X:bigint,Utm_Y:bigint,Well_Name:string,Well_Num:string,Well_Title:string>, type: string]
</pre>

And here is our nicely formatted dataset ready to go into Cassandra:
<pre>
scala> wells.show()
+--------+--------------------+--------------------+-------+
|     api|            geometry|          properties|   type|
+--------+--------------------+--------------------+-------+
|12508888|[WrappedArray(722...|[12508888,125,05-...|Feature|
|12323461|[WrappedArray(524...|[12323461,123,05-...|Feature|
|12323462|[WrappedArray(530...|[12323462,123,05-...|Feature|
|12323463|[WrappedArray(523...|[12323463,123,05-...|Feature|
|12323464|[WrappedArray(523...|[12323464,123,05-...|Feature|
|04511663|[WrappedArray(235...|[04511663,045,05-...|Feature|
|04511664|[WrappedArray(235...|[04511664,045,05-...|Feature|
|04511665|[WrappedArray(236...|[04511665,045,05-...|Feature|
|04511666|[WrappedArray(236...|[04511666,045,05-...|Feature|
|04511667|[WrappedArray(244...|[04511667,045,05-...|Feature|
|12323467|[WrappedArray(524...|[12323467,123,05-...|Feature|
|01306522|[WrappedArray(494...|[01306522,013,05-...|Feature|
|04511668|[WrappedArray(244...|[04511668,045,05-...|Feature|
|04511669|[WrappedArray(244...|[04511669,045,05-...|Feature|
|04511670|[WrappedArray(244...|[04511670,045,05-...|Feature|
|04511671|[WrappedArray(245...|[04511671,045,05-...|Feature|
|04511672|[WrappedArray(245...|[04511672,045,05-...|Feature|
|04511673|[WrappedArray(265...|[04511673,045,05-...|Feature|
|10310665|[WrappedArray(204...|[10310665,103,05-...|Feature|
|04511674|[WrappedArray(233...|[04511674,045,05-...|Feature|
+--------+--------------------+--------------------+-------+
only showing top 20 rows

scala> wells.printSchema()
root
 |-- api: string (nullable = true)
 |-- geometry: struct (nullable = true)
 |    |-- coordinates: array (nullable = true)
 |    |    |-- element: double (containsNull = true)
 |    |-- type: string (nullable = true)
 |-- properties: struct (nullable = true)
 |    |-- api: string (nullable = true)
 |    |-- api_county: string (nullable = true)
 |    |-- api_label: string (nullable = true)
 |    |-- api_seq: string (nullable = true)
 |    |-- citing_typ: string (nullable = true)
 |    |-- dir_e_w: string (nullable = true)
 |    |-- dir_n_s: string (nullable = true)
 |    |-- dist_e_w: long (nullable = true)
 |    |-- dist_n_s: long (nullable = true)
 |    |-- facil_id: long (nullable = true)
 |    |-- facil_stat: string (nullable = true)
 |    |-- facil_type: string (nullable = true)
 |    |-- field_code: long (nullable = true)
 |    |-- field_name: string (nullable = true)
 |    |-- ground_ele: long (nullable = true)
 |    |-- latitude: double (nullable = true)
 |    |-- loc_id: long (nullable = true)
 |    |-- loc_name: string (nullable = true)
 |    |-- loc_qual: string (nullable = true)
 |    |-- longitude: double (nullable = true)
 |    |-- max_md: long (nullable = true)
 |    |-- max_tvd: long (nullable = true)
 |    |-- meridian: string (nullable = true)
 |    |-- operat_num: long (nullable = true)
 |    |-- operator: string (nullable = true)
 |    |-- qtr_qtr: string (nullable = true)
 |    |-- range: string (nullable = true)
 |    |-- section: string (nullable = true)
 |    |-- spud_date: string (nullable = true)
 |    |-- township: string (nullable = true)
 |    |-- utm_x: long (nullable = true)
 |    |-- utm_y: long (nullable = true)
 |    |-- well_name: string (nullable = true)
 |    |-- well_num: string (nullable = true)
 |    |-- well_title: string (nullable = true)
 |-- type: string (nullable = true)
</pre>

<h3>9. Create The Cassandra Database Schema</h3>
We need to create the table to store our data. The table structure needs to reflect the nested nature of the JSON data that we want to store.
<h4> Create A Cassandra Keyspace</h4>
I'm running this on a single node of Cassandra and Spark. If you're using multiple nodes you can increase the replication factor as required. If you're using multiple datacentres you should replace the replication strategy with 'NetworkTopologyStrategy'.
<pre lang="sql">
CREATE KEYSPACE IF NOT EXISTS wells WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 };
</pre>
<h4> Create A User-Defined Type For The Well Properties Structure </h4>
<pre lang="sql">
CREATE TYPE well_data (
api text,
api_Label text,
operator text,
well_title text,
facil_Id int,
facil_type text,
facil_stat text,
operat_num bigint,
well_num text,
well_name text,
field_code int,
dist_n_s int,
dir_n_s text,
dist_e_w int,
dir_e_w text,
qtr_qtr text,
section text,
township text,
range text,
meridian text,
latitude decimal,
longitude decimal,
ground_ele decimal,
utm_x decimal,
utm_y decimal,
loc_qual text,
field_name text,
api_seq text,
api_county text,
loc_id decimal,
loc_name text,
spud_date text,
citing_typ text,
max_MD decimal,
max_tvd decimal);
</pre>
	
<h4>Create A User-Defined Type For the Geometry Structure</h4>	
<pre lang="sql">
CREATE TYPE geometry_data (		
type text,
coordinates list<double>);
</pre>

<h4>Create The Wells Table</h4>
<pre lang="sql">
CREATE TABLE wells (
api text,
type text,
properties frozen<well_data>,
geometry frozen<geometry_data>,
PRIMARY KEY(api));
</pre>

Let's test that the table matches the JSON layout using a single record insert:
<pre lang="sql">
INSERT into wells.wells JSON '{"API": "12508888", "type": "Feature","properties": {"API": "12508888","API_Label": "05-125-08888","Operator": "OMIMEX PETROLEUM INC","Well_Title": "8-9-5-45 FERGUSON","Facil_Id": "0","Facil_Type": "WELL","Facil_Stat": "PR","Operat_Num": "66190","Well_Num": "8-9-5-45","Well_Name": "FERGUSON","Field_Code": "1970","Dist_N_S": "1980","Dir_N_S": "N","Dist_E_W": "600","Dir_E_W": "E","Qtr_Qtr": "SENE","Section": "9","Township": "5N","Range": "45W","Meridian": "6","Latitude": "40.419416","Longitude": "-102.379999","Ground_Ele": 0,"Utm_X": "722281","Utm_Y": "4477606","Loc_Qual": "PLANNED Footage","Field_Name": "BALLYNEAL","Api_Seq": "08888","API_County": "125","Loc_ID": "304702","Loc_Name": "FERGUSON-65N45W 9SENE","Spud_Date": "2004\/09\/07","Citing_Typ": "ACTUAL","Max_MD": "2727","Max_TVD": "2727"},"geometry": {"type": "Point","coordinates": [722281.0, 4477606.0]}}';
</pre>

<h4>Update A Record Using An Upsert</h4>
<pre lang="sql">
INSERT into wells.wells JSON '{"API": "12508888", "type": "Feature","properties": {"Operator": "CAPIREX PETROLEUM INC"}}';
</pre>


<h3>10. Write the dataframe back to Cassandra</h3>
Now that we have the data in a correctly formatted dataframe we can write it to a Cassandra table using the Spark Cassandra connector.
<pre lang="scala">
scala> wells.write.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "wells", "keyspace" -> "wells")).save()
</pre>
This command expects the table to be empty. There are other options for the save command that you can explore for example <pre lang="scala">df.write.format.options.mode(SaveMode.Append).save()</pre>

<h3>11. Reading From A Cassandra Table</H3>
Of course, you may also want to read data BACK into a dataframe from a Cassandra table. To achieve this we can use the following Scala command:
<pre lang="scala">
val df5 = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "wells", "keyspace" -> "wells")).load()
</pre>
And we can demonstrate that we have all the records returned in our new dataframe:
<pre lang="scala">
scala> df5.count()
res29: Long = 110274
</pre>
