This is a Spark/Cassandra demo using the open-source **[Spark Cassandra Connector]**

There are 2 packages with 2 distinct demos

* _us.unemployment.demo_
<ul>
    <li> Ingestion
        <ol>
            <li> **FromCSVToCassandra**: read US employment data from CSV file into Cassandra</li>
            <li> **FromCSVCaseClassToCassandra**: read US employment data from CSV file, create case class and insert into Cassandra</li>
        </ol>
    </li>
    <li> Read
        <ol>
            <li> **FromCassandraToRow**: read US employment data from Cassandra into **CassandraRow** low-level object</li>
            <li> **FromCassandraToCaseClass**: read US employment data from Cassandra into custom Scala case class, leveraging the built-in object mapper</li>
        </ol>
    </li>        
</ul>

* _weather.data.demo_
<ul>
    <li> Data preparation
        <ol>
            <li> Go to the folder main/data</li>
            <li> Execute _$CASSANDRA_HOME/bin/cqlsh -f weather_data_schema.cql_ from this folder. It should create the keyspace **spark_demo** and some tables </li>
            <li> Download the _Weather_Raw_Data_2014.csv.gz_ from **[here]** </li>
            <li> Unzip it somewhere on your disk </li>
        </ol>
    </li>
    <li> Ingestion
        <ol>
            <li> **WeatherDataIntoCassandra**: read all the _Weather_Raw_Data_2014.csv_ file (30.10<sup>6</sup> lines) and insert the data into Cassandra. 
            Please do not forget set the path to this file by changing the **WeatherDataIntoCassandra.WEATHER_2014_CSV** value</li>
        </ol>
        This step should take a while since there are 30.10<sup>6</sup> lines to be inserted into Cassandra
    </li>
    <li> Read
        <ol>
            <li> **WeatherDataFromCassandra**: read all raw weather data plus all weather stations details, 
            filter the data by French station and take data only between March and June 2014. 
            Then compute average on temperature and pressure</li>
        </ol>
        This step should take a while since there are 30.10<sup>6</sup> lines to be read from Cassandra
    </li>        
</ul>

[Spark Cassandra Connector]: https://github.com/datastax/spark-cassandra-connector
[here]: https://drive.google.com/a/datastax.com/file/d/0B6wR2aj4Cb6wOF95QUZmVTRPR2s/view?usp=sharing