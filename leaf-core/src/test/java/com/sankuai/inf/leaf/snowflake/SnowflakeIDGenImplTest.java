package com.sankuai.inf.leaf.snowflake;

import com.google.common.util.concurrent.RateLimiter;
import com.sankuai.inf.leaf.IDGen;
import com.sankuai.inf.leaf.common.PropertyFactory;
import com.sankuai.inf.leaf.common.Result;
import org.junit.Test;

import java.util.Properties;

public class SnowflakeIDGenImplTest {
    @Test
    public void testGetId() {
        Properties properties = PropertyFactory.getProperties();

        IDGen idGen = new SnowflakeIDGenImpl(properties.getProperty("leaf.zk.list"), 8080);
        for (int i = 1; i < 1000; ++i) {
            Result r = idGen.get("a");
            System.out.println(r);
        }
    }

    @Test
    public void test01(){
        RateLimiter rateLimiter = RateLimiter.create(1);
        for(int i = 1; i < 10; i = i + 2 ) {
            double waitTime = rateLimiter.acquire(i);
            System.out.println("cutTime=" + System.currentTimeMillis() + " acq:" + i + " waitTime:" + waitTime);
        }
    }
}
