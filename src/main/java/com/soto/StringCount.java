package com.soto;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.*;

import java.util.ArrayList;

/**
 * 统计字符串出现的次数
 */
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
