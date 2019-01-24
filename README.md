# UDF用户自定义函数

针对单行输入,返回一个输出

## 案例: 截取字符串长度(UDF)  strLenUDF.java

* 定义和注册 自定义函数

```java
//注册一张names表
namesDF.registerTempTable("names");

//定义函数:自己写匿名函数
//注册函数:SqlContext.udf.register()
sqlContext.udf().register("strLen",(String s) -> s.length(),DataTypes.IntegerType);


//使用自定义函数
DataFrame udfDF = sqlContext.sql("select name,strLen(name) from names");

udfDF.show();
```



* 案例中出现不支持lambda表达式的情况,解决方法在pom.xml文件中修改

[Java “lambda expressions not supported at this language level”](https://stackoverflow.com/questions/22703412/java-lambda-expressions-not-supported-at-this-language-level)

```java
<plugin>
  <artifactId>maven-compiler-plugin</artifactId>
  <version>3.8.0</version>
  <configuration>
    <source>1.8</source>
    <target>1.8</target>
  </configuration>
</plugin>
```

---

# UDAF 用户自定义聚合函数

[Java Code Examples for org.apache.spark.sql.expressions.MutableAggregationBuffer](https://www.programcreek.com/java-api-examples/index.php?api=org.apache.spark.sql.expressions.MutableAggregationBuffer)

针对多行输入,进行聚合计算,一个输出

## 案例:统计字符串出现的次数 StringCountUDAF.java

UDAF方法重写 StringCount.java

```java
public class StringCount extends UserDefinedAggregateFunction {
    ArrayList<StructField> structFields = new ArrayList<>();
    //指输入数据的类型
    @Override
    public StructType inputSchema() {
        structFields.clear();
        structFields.add(DataTypes.createStructField("str", DataTypes.StringType, true));
        return DataTypes.createStructType(structFields);
    }

    //中间聚合时所处理的数据类型
    @Override
    public StructType bufferSchema() {
        structFields.clear();
        structFields.add(DataTypes.createStructField("count", DataTypes.IntegerType, true));
        return DataTypes.createStructType(structFields);
    }

    //函数返回值的类型
    @Override
    public DataType dataType() {
        return DataTypes.IntegerType;
    }

    @Override
    public boolean deterministic() {
        return true;
    }

    //为每个分组的数据执行初始化操作
    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0, 0);
    }

    //指的是,每个分组,有新的值进来的时候,如何进行分组对应的聚合值的计算
    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        buffer.update(0,buffer.getInt(0)+1);
    }

    //由于Spark是分布式的,所以一个分组的数据,可能会在不同节点上进行局部聚合,就是update
    //但是,最后一个分组,在各个节点上的聚合值,要进行merge,也就是合并
    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        buffer1.update(0,buffer1.getInt(0)+buffer2.getInt(0));
    }


    //指的是,一个分组的聚合值,如何通过中间的缓存聚合值,最后返回一个最终的聚合值
    @Override
    public Object evaluate(Row buffer) {
        return buffer.getAs(0);
    }

}
```

UDAF 使用StringCountUDAF.java

```java
//注册一张names表
namesDF.registerTempTable("names");

//定义和注册 自定义函数
//定义函数:自己写匿名函数
//注册函数:SqlContext.udf.register()
sqlContext.udf().register("strCount",new StringCount());


//使用自定义函数
DataFrame udfDF = sqlContext.sql("select name,strCount(name) from names group by name ");
```
























