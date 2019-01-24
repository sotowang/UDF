//package com.soto;
//
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.text.NumberFormat;
//import java.util.HashMap;
//import java.util.Properties;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;
//
///**
// * UDF实例
// * 需求： 分析节假日订单与平时订单的不同
// */
//public class UDF extends org.apache.hadoop.hive.ql.exec.UDF {
//    public static void main(String[] args) throws IOException {
//        UDF udf = new UDF();
//        String date_str1 = "20150101";
//        System.out.println(udf.evalute(date_str1));
//
//        String date_str2 = "20160101";
//        String result = udf.evalute(date_str2);
//        System.out.println("result is:" + result);
//
//        double date_dou = 20160101;
//        int result_dou = udf.evalute(date_dou, "count");
//        System.out.println(result_dou);
//        System.out.println(date_dou);
//    }
//
//    private HashMap<String, String> festivalMap = new HashMap<String, String>();
//
//
//    /**
//     * 初始化构造器->将date文件内容转为 key-value 的HashMap
//     * @throws IOException
//     */
//    public UDF() throws IOException {
//        InputStreamReader proFile = new InputStreamReader(
//                getClass().getClassLoader().getResourceAsStream("date"), "utf8");
//
//        Properties properties = new Properties();
//        properties.load(proFile);
//
//        for (Object key : properties.keySet()) {
//            festivalMap.put(key.toString(), properties.getProperty(key.toString()));
//        }
//
//    }
//
//    /**
//     * 匹配 date_str 返回20160101的形式
//     * @param date_str
//     * @return
//     */
//    public String match_date(String date_str) {
//        //匹配20160101这种日期格式
//        Pattern pattern_common = Pattern.compile("\\d{8}");
//        Matcher matcher_common = pattern_common.matcher(date_str);
//
//
//        //匹配2016-01-01日期格式
//        Pattern pattern_strike = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");
//        Matcher matcher_strike = pattern_strike.matcher(date_str);
//
//
//        //匹配2016-01-01 10:35:46 这种日期格式
//        Pattern pattern_colon = Pattern.compile("\\d{4}-\\d{2}-\\d{2}(\\s)+\\d{2}:\\d{2}:\\d{2}");
//        Matcher matcher_colon = pattern_colon.matcher(date_str);
//
//        if (matcher_colon.find()) {
//            return date_str.replace("-", "").substring(0, 8);
//        } else if (matcher_strike.find()) {
//            return date_str.replace("-", "");
//        } else if (matcher_common.find()) {
//            return date_str;
//        }else
//            return "null";
//    }
//
//    /**
//     *
//     解决了double转String的科学计数法的问题
//     */
//    public String doubleToString(double dou) {
//        Double dou_obj = new Double(dou);
//        NumberFormat numberFormat = NumberFormat.getInstance();
//        numberFormat.setGroupingUsed(false);
//        String dou_str = numberFormat.format(dou_obj);
//        return dou_str;
//    }
//
//    public String evalute(double date_dou) {
//        String date_str = this.doubleToString(date_dou);
//        return evalute(date_str);
//    }
//
//    public String evalute(String date_str) {
//        if (!this.match_date(date_str).equals("null")) {
//            date_str = this.match_date(date_str);
//            return festivalMap.get(date_str) == null ? "null" : festivalMap.get(date_str);
//        }else
//            return "null";
//    }
//
//    public int evalute(double date_dou, String flag) {
//        String date_str = this.doubleToString(date_dou);
//        return evalute(date_str, flag);
//    }
//
//    public int evalute(String date_str, String flag) {
//        if (flag.equals("count") && !this.match_date(date_str).equals("null")) {
//            date_str = this.match_date(date_str);
//            return festivalMap.get(date_str) == null ? 0 : 1;
//        }else {
//            return 0;
//        }
//    }
//
//}
