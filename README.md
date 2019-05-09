# Haxwell
A simple to use HBase region server implementation which works like maxwell for mysql.
It uses HBase replication to allow users to listen to row mutations.  

### Compatible hbase version(s)
* 1.1.x
* 1.2.x

Note: Built and tested using HBase 1.1.2 on HDP 2.6.1.0-129

### Build 
* Checkout the repository `https://github.com/phaneesh/haxwell`
* Change HBase dependencies if required (This should be compatible with 1.1.x)
* Build the haxwell binaries using`mvn clean install` command


### Setup
* Copy `target/haxwell*.jar` to `hbase/lib`
* Add the following configuration in `hbase-site.xml`

```xml
<configuration>
  <property>
    <name>hbase.replication</name>
    <value>true</value>
  </property>
  <property>
    <name>replication.source.ratio</name>
    <value>1.0</value>
  </property>
  <property>
    <name>replication.source.nb.capacity</name>
    <value>1000</value>
  </property>
  <property>
    <name>replication.replicationsource.implementation</name>
    <value>com.hbase.haxwell.HaxwellReplicationSource</value>
  </property>
</configuration>
```  
* Restart/Start HBase

### Demo
* Use `HaxwellDemo.java` to start a simple console logging consumer
* Use `HaxwellDemoIngester.java` to start writing sample data into `haxwell-demo` table
* You can even do a `put` to `haxwell-demo` table from `hbase shell`


### Configuration
* `hbase.haxwell.consumers.handler.count` Total no of threads that consumer will use to process the events default value is `10`
* `hbase.haxwell.consumers.events.batchsize` Event batch size. Default value `100`
* `hbase.haxwell.consumers.execution.timeout.ms` Execution timeout set of each handler thread. Default value `-1` (No timeout)
* `hbase.haxwell.consumers.handler.queue.size` Queue size for handler . Default value `100`