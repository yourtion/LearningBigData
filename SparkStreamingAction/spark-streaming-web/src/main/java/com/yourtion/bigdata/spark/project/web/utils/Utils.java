package com.yourtion.bigdata.spark.project.web.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author yourtion
 */
public class Utils {

    public static String getDay() {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        formatter.setLenient(false);
        return formatter.format(new Date());
    }
}
