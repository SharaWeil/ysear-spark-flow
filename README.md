##说明
这个工程主要是为了练习模块架构设计和spark、scala学习。实现了可以通过xml提交Spark任务，xml如下示例

    <?xml version="1.0" encoding="UTF-8"?>
    <flow xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:noNamespaceSchemaLocation="http://com.ysear.com/xml/spark-flow SparkSqlFlowSchema.xsd">
    
        <!--定义spark程序的基本信息-->
        <info>
            <name>mysql测试</name>
            <file>win.xml</file>
            <version>1.0</version>
        </info>
    
        <properties>
            <!--是否忽略大小写-->
            <ignoreCase>true</ignoreCase>
            <sparkConfs>
                <spark.default.parallelism>10</spark.default.parallelism>
            </sparkConfs>
        </properties>
    
        <!--定义数据源-->
        <dataSources>
            <dataSource type="mysql" sourceId="mysqlSource">
                <driverClass>com.mysql.cj.jdbc.Driver</driverClass>
                <url>jdbc:mysql://localhost:3306/employees?useSSL=false&amp;useUnicode=true&amp;characterEncoding=UTF-8&amp;useOldAliasMetadataBehavior=true&amp;serverTimezone=Asia/Shanghai</url>
                <userName>root</userName>
                <passWord>123456</passWord>
            </dataSource>
        </dataSources>
    
        <!--定义udf函数-->
        <!--    <methods>-->
        <!--        <udf name="GET_PROVICE" class="com.jsdata.udf.NatPhoneNumberProvinceUDF" returnType="Map&lt;String,String&gt;"/>-->
        <!--        <udf name="QUERY_HBASE" class="com.jsdata.udf.QueryInfoByIdCardUDF" returnType="Map&lt;String,String&gt;"/>-->
        <!--    </methods>-->
    
    
        <sources>
            <source type="mysql" sourceId="mysqlSource" alias="employees" repartition="10">
                SELECT * FROM select * from employees
            </source>
            <source type="mysql" sourceId="mysqlSource" alias="dept_emp" cache="MEMORY_ONLY">
                select * from dept_emp
            </source>
            <source type="mysql" sourceId="mysqlSource" alias="departments">
                select * from departments
            </source>
        </sources>
    
        <!--
        1、定义数据源信息
            type:mysql
        -->
        <transforms>
            <transform type="sql" tableName="t1">
                SELECT e.emp_no,dp.dept_no,dm.dept_name,e.first_name,e.last_name,e.gender,e.birth_date FROM employees e LEFT JOIN dept_emp dp ON e.emp_no = dp.emp_no LEFT JOIN departments dm ON dp.dept_no = dm.dept_no
            </transform>
            
        </transforms>
    
        <!--
        1、定义数据源信息
            type:mysql
        -->
        <targets>
                    <target type="console" rowNum="100" tableName="t1"></target>
    <!--        <target type="mysql" mode="overwrite" partition="10" sourceId="mysqlSource" tableName="t4" toTarget="test4"/>-->
        </targets>
    </flow>
通过spark-submit -f[filePath] 提交任务
### 模块说明
**ysear-spark-common**:公共组件  
**ysear-spark-core**:xml解析，任务执行  
**ysear-spark-spi**:ysearSPI加载插件模块  
**ysear-spark-plugin**:ysearSpark插件模块，可以自己定义实现flowSource和flowSink
 - 实现flowSource请实现FlowDataSourceFactory特质
 - 实现flowSink请实现FlowSourceFactory特质
 - 在META-INF文件下创建ysear.factories文件指定spi文件




### 启动命令
    spark-default.xml文件添加配置
    spark.yarn.jars    hdfs:///spark/jars/*.jar
    
    ./bin/spark-submit --class com.ysera.elt.flow.YseraSparkApplication \
    --master yarn \
    --driver-memory 4g \
    --executor-memory 2g \
    --executor-cores 1 \
    --jars $(echo /opt/software/spark/ysera/jar/*.jar | tr ' ' ',') \
    /opt/software/spark/ysera/jars/ysera_core-1.0-SNAPSHOT.jar \
    10 \
    -f /opt/software/spark/ysera/mysql_hbase.xml
