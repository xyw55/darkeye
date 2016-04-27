package com.xyw55.dao;

import org.apache.commons.lang.math.NumberUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class CounterHelper {
    static JedisTemplate<String> jedisTemplate;
    
    public CounterHelper(JedisTemplate<String> jedisTemplate) {
        this.jedisTemplate = jedisTemplate;
    }
    
    public int getTimeCounterCnt(String name,TimeCounter tc,Date time){
        String key=tc.key(name, tc, time);
        //System.out.println(key);
        return getValAsInt(key,0);
    }
    public int hgetTimeCounterCnt(String name,TimeCounter tc,Date time,String field){
        String key=tc.key(name, tc, time);
        return hgetValAsInt(key, field, 0);
    }
    
    public long incr(String key,int delta) {
        return jedisTemplate.incrBy(key,delta);
    }
    public long incr(String key) {
        return jedisTemplate.incrBy(key,1);
    }
    public long hincr(String key,String field) {
        return jedisTemplate.hincrBy(key, field, 1);
    }
    public long incrTimeCounter(String name,TimeCounter tc,Date time) {
        if (null==time) {
            time=new Date();
        }
        long ret= incr(tc.key(name, tc, time));
        //TODO as below
        return ret;
    }
    public long hincrTimeCounter(String name,TimeCounter tc,Date time,String field) {
        if (null==time) {
            time=new Date();
        }
        long ret= hincr(tc.key(name, tc, time),field);
        //TODO expire的逻辑?evileye该代码废弃？否则原逻辑是错误的
        return ret;
    }
    
    public long incrTimeCounter(String name,TimeCounter tc,Date time,String field) {
        if (null==time) {
            time=new Date();
        }
        return hincr(tc.key(name, tc, time),field);
    }
    
    private int getValAsInt(String key,int def) {
        Integer ret=jedisTemplate.getAsInt(key);
        return (null==ret)?def:ret;
    }
    private int hgetValAsInt(String key,String field,int def) {
        String ret=jedisTemplate.hget(key, field);
        return NumberUtils.toInt(ret,def);
    }
    /**
     * sum all[start,end] TimeCounter's counter metrics
     * 逻辑：至endtime(默认current)截止 num次（间隔为tc.interval）数据的sum
     */
    public void getTimeCounter_Counts(String name,TimeCounter tc,int num) {
        Date end=new Date();
        List<TimeCounterStatics> statics=new ArrayList<>(6);
        for (int i = num; i >0; i--) {
            String key=tc.key(name, tc, end);
            //FIXME to be continued
            
        }
    }
}

/*
@classmethod
def get_counts(cls, name, start=None, end=None, num=6):
    if start and end:
        num = int((end - start).total_seconds() / cls.interval) + 1
    elif start and not end:
        end = start + datetime.timedelta(seconds=cls.interval * num)
    elif not end:
        end = datetime.datetime.now()
    counts = []
    currents = []
    while num > 0:
        key = cls.generate_key(name, end)
        currents.append(cls.parse_current(key))
        dct = rc.hgetall(key) or {}
        counts.append({k: int(v) for k, v in dct.iteritems()})
        end -= datetime.timedelta(seconds=cls.interval)
        num -= 1
    counts = zip(currents, counts)
    counts.reverse()
    return counts
*/



