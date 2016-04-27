package com.xyw55.dao;


import redis.clients.jedis.JedisPool;

import java.util.Date;

public class TimeCounterHelper {

    static JedisTemplate<String> jedisTemplate = new JedisTemplate<String>(new JedisPool());
    
    public void incrTimecounters(TimeCounter tc,String name,String field,Date current,int incr){
        if (null==current) {
            current=new Date();
        }
        String key=tc.key(name, tc, current);
        //FIXME to add pipe
        jedisTemplate.hincrBy(key, field, incr);
        if (tc.ttl!=null&&tc.ttl!=0) {
            jedisTemplate.expire(key, tc.ttl.intValue());
        }
    }
    
    public void incrTimeCounterSnaps(String name,String field){
        for(TimeCounter tc:TimeCounter.getTimeCounters()){
            incrTimecounters(tc, name, field,new Date(),1);
        }
    }
    
    public void incrTimeCounterSnaps(String name,String field,Date current,int incr){
        for(TimeCounter tc:TimeCounter.getTimeCounters()){
            incrTimecounters(tc, name, field, current, incr);
        }
    }
}
