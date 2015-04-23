This is a Spark/Cassandra demo using the open-source **[Spark Cassandra Connector]**

There are 2 packages with 2 distinct demos

* _us.unemployment.demo_
<ul>
    <li> Ingestion
        <ol>
            <li> <strong>FromCSVToCassandra</strong>: read US employment data from CSV file into Cassandra</li>
            <li> <strong>FromCSVCaseClassToCassandra</strong>: read US employment data from CSV file, create case class and insert into Cassandra</li>
        </ol>
    </li>
    <li> Read
        <ol>
            <li> <strong>FromCassandraToRow</strong>: read US employment data from Cassandra into <strong>CassandraRow</strong> low-level object</li>
            <li> <strong>FromCassandraToCaseClass</strong>: read US employment data from Cassandra into custom Scala case class, leveraging the built-in object mapper</li>
            <li> <strong>FromCassandraToSQL</strong>: read US employment data from Cassandra using <strong>SparkSQL</strong> a the connector integration
        </ol>
    </li>        
</ul>

* _twitter.stream_
<ul>
    <li> <strong>TwitterStreaming</strong>: demo of Twitter stream saved back to Cassandra (<strong>stream IN</strong>). To make this demo work, you need to start the job with the following info:

        <ol>
            <li>-Dtwitter4j.oauth.consumerKey="value"</li>
            <li>-Dtwitter4j.oauth.consumerSecret="value"</li>
            <li>-Dtwitter4j.oauth.accessToken="value"</li>
            <li>-Dtwitter4j.oauth.accessTokenSecret="value"</li>
        </ol>
        
        If you don't have a Twitter app credentials, create a new apps at <a href="https://apps.twitter.com/" target="_blank">https://apps.twitter.com/</a>
  </li>
</ul>

* _analytics.music_
<ul>
    <li> Data preparation
        <ol>
            <li> Go to the folder main/data</li>
            <li> Execute <em>$CASSANDRA_HOME/bin/cqlsh -f music.cql</em> from this folder. It should create the keyspace <strong>spark_demo</strong> and some tables </li>
            <li> the script will then load into Cassandra the content of <em>performers.csv</em> and <em>albums.csv</em></li>
        </ol>
    </li>
    <li> Scenarios
        <br/>
        <br/>
        All examples extend the `BaseExample` class which configures a SparkContext and truncate some tables automatically for you
        so that the example can be executed several times and be consistent
        <br/>
        <ol>
            <li> <strong>Example1</strong> : in this example, we read data from the `performers` table to extract performers and styles into the `performers_by_style` table</li>
            <li> <strong>Example2</strong> : in this example, we read data from the `performers` table, group styles by performer for aggregation. The results are saved back into the `performers_distribution_by_style` table</li>
            <li> <strong>Example3</strong> : similar to <strong>Example2</strong> we only want to extract the top 10 styles for <strong>artists</strong> and <strong>groups</strong> and save the results into the `top10_styles` table</li>
            <li> <strong>Example4</strong> : in this example, we want to know, for each decade, the number of albums released by each artist, group by their origin country. For this we join the table `performers` with `albums`. The results are saved back into the `albums_by_decade_and_country` table</li>
            <li> <strong>Example5</strong> : similar to <strong>Example4</strong>, we perform the join using the <strong>SparkSQL</strong> language. We also filter out low release count countries. The results are saved back into the `albums_by_decade_and_country_sql` table</li>                                                
        </ol>
    </li>        
</ul>

* _usecases_ 
<ul>
    <br/>
    <br/>
    Those scenarios examplify how Spark can be used to achieved various real world use-cases
    <br/>
    <li> Scenarios
        <ol>
            <li> <strong>CrossClusterDataMigration</strong> : this is a sample code to show how to perform effective cross cluster operations. <strong>DO NOT EXECUTE IT</strong></li>                                                
            <li> <strong>CrossDCDataMigration</strong> : this is a sample code to show how to perform effective cross data-centers operations. <strong>DO NOT EXECUTE IT</strong></li>
            <li> <strong>DataCleaningForPerformers</strong> : in this scenario, we read data from the ```performers``` table to clean up empty _country_ and reformatting the _born_ and _died_ dates, if present. The data are saved back into Cassandra, thus achieving <strong>perfect data locality</strong></li>
            <li> <strong>DisplayPerformersData</strong> : an utility class to show data <strong>before</strong> and <strong>after</strong> the cleaning</li>
            <li> <strong>MigrateAlbumnsData</strong> : in this scenario, we read source date from `albums` and save them back into a new table `albums_by_country` purposedly built for fast query on contry and year</li>
        </ol>
    </li>        
</ul>
 
* _weather.data.demo_
<ul>
    <li> Data preparation
        <ol>
            <li> Go to the folder main/data</li>
            <li> Execute <em>$CASSANDRA_HOME/bin/cqlsh -f weather_data_schema.cql</em> from this folder. It should create the keyspace <strong>spark_demo</strong> and some tables </li>
            <li> Download the <em>Weather_Raw_Data_2014.csv.gz</em> from <strong><a target="blank_" href="https://drive.google.com/file/d/0B6wR2aj4Cb6wOF95QUZmVTRPR2s/view?usp=sharing">here</a></strong> (&gt;200Mb)</li>
            <li> Unzip it somewhere on your disk </li>
        </ol>
    </li>
    <li> Ingestion
        <ol>
            <li> <strong>WeatherDataIntoCassandra</strong>: read all the <em>Weather_Raw_Data_2014.csv</em> file (30.10<sup>6</sup> lines) and insert the data into Cassandra. It may take some time before the ingestion is done so go take a long coffee ( &lt; 1 hour on my MacBookPro 15") 
            Please do not forget set the path to this file by changing the <strong>WeatherDataIntoCassandra.WEATHER_2014_CSV</strong> value</li>
        </ol>
        This step should take a while since there are 30.10<sup>6</sup> lines to be inserted into Cassandra
    </li>
    <li> Read
        <ol>
            <li> <strong>WeatherDataFromCassandra</strong>: read all raw weather data plus all weather stations details, 
            filter the data by French station and take data only between March and June 2014. 
            Then compute average on temperature and pressure</li>
        </ol>
        This step should take a while since there are 30.10<sup>6</sup> lines to be read from Cassandra
    </li>        
</ul>

[Spark Cassandra Connector]: https://github.com/datastax/spark-cassandra-connector

