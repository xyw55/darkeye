package com.xyw55.dao;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class TimeCounter {
    final String time_format;
    final int interval;
    final Long ttl;//过期时间，单位秒
    
    public final static TimeCounter DayCounter=new TimeCounter("yyyyMMdd",24*3600,0);
    public final static TimeCounter HourTCounter=new TimeCounter("yyyyMMddHH",3600,720*3600);//one month
    public final static TimeCounter MinuteCounter=new TimeCounter("yyyyMMddHHmm",60,24*3600);//one day
    

    //public TimeCounter(){}
    
    public TimeCounter(String time_format, int interval, long ttl) {
        this.time_format = time_format;
        this.interval = interval;
        this.ttl = ttl;
    }
    public String key(String name,TimeCounter tc,Date time){
        return new StringBuilder("counter:").append(name).append(":").append(FastDateFormat.getInstance(tc.time_format).format(time)).toString();
    }
    public static Date parseCurrent(String key,TimeCounter tc) {
        String date=StringUtils.substringAfterLast(key, ":");
        DateTime dt=null;
        try {
            dt=DateTimeFormat.forPattern(tc.time_format).parseDateTime(date);
        } catch (IllegalArgumentException e) {
        }
        return (null!=dt)?dt.toDate():null;
    }
    public static List<TimeCounter> getTimeCounters(){
        return Arrays.asList(DayCounter,HourTCounter,MinuteCounter);
    }
}