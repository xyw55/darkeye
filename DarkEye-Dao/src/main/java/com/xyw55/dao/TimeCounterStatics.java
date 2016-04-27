package com.xyw55.dao;

import java.util.Date;
import java.util.Map;

public class TimeCounterStatics {
    private Date time;
    private Map<String,Integer> statics;
    //just for json searilize!
    public TimeCounterStatics() {
    }
    public TimeCounterStatics(Date time, Map<String,Integer> statics) {
        this.time=time;
        this.statics=statics;
    }
    public Date getTime() {
        return time;
    }
    public void setTime(Date time) {
        this.time = time;
    }
    public Map<String, Integer> getStatics() {
        return statics;
    }
    public void setStatics(Map<String, Integer> statics) {
        this.statics = statics;
    }
    
}
