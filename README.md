# UDF用户自定义函数

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































