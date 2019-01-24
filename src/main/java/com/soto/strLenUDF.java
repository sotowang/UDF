package com.soto;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class strLenUDF {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("strLenUDF")
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(jsc);


        //构造模拟数据
        List<String> names = Arrays.asList("Leo", "Jack", "Tom");

        JavaRDD<String> namesRDD = jsc.parallelize(names);
        JavaRDD<Row> namesRowRDD = namesRDD.map(new Function<String, Row>() {
            @Override
            public Row call(String name) throws Exception {
                return RowFactory.create(name);
            }
        });



        ArrayList<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));

        StructType structType = DataTypes.createStructType(structFields);

        //创建DataFrame
        DataFrame namesDF = sqlContext.createDataFrame(namesRowRDD, structType);

        //注册一张names表
        namesDF.registerTempTable("names");

        //定义和注册 自定义函数
        //定义函数:自己写匿名函数
        //注册函数:SqlContext.udf.register()
        sqlContext.udf().register("strLen",(String s) -> s.length(),DataTypes.IntegerType);


        //使用自定义函数
        DataFrame udfDF = sqlContext.sql("select name,strLen(name) from names");

        udfDF.show();

        jsc.close();

    }
}
