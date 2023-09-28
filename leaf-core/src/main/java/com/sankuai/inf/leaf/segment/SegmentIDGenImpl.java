package com.sankuai.inf.leaf.segment;

import com.sankuai.inf.leaf.IDGen;
import com.sankuai.inf.leaf.common.Result;
import com.sankuai.inf.leaf.common.Status;
import com.sankuai.inf.leaf.segment.dao.IDAllocDao;
import com.sankuai.inf.leaf.segment.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StopWatch;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;


public class SegmentIDGenImpl implements IDGen {
    private static final Logger logger = LoggerFactory.getLogger(SegmentIDGenImpl.class);

    /**
     * IDCache未初始化成功时的异常码
     */
    private static final long EXCEPTION_ID_IDCACHE_INIT_FALSE = -1;
    /**
     * key不存在时的异常码
     */
    private static final long EXCEPTION_ID_KEY_NOT_EXISTS = -2;
    /**
     * SegmentBuffer中的两个Segment均未从DB中装载时的异常码
     */
    private static final long EXCEPTION_ID_TWO_SEGMENTS_ARE_NULL = -3;
    /**
     * 最大步长不超过100,0000
     */
    private static final int MAX_STEP = 1000000;
    /**
     * 一个Segment维持时间为15分钟
     */
    private static final long SEGMENT_DURATION = 15 * 60 * 1000L;
    /**
     * 自定义线程池
     */
    private ExecutorService service = new ThreadPoolExecutor(5, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), new UpdateThreadFactory());
    /**
     * 是否将为每个业务tag创建segement
     */
    private volatile boolean initOK = false;
    /**
     * Key为业务主键,value为对应缓存
     */
    private Map<String, SegmentBuffer> cache = new ConcurrentHashMap<>(256);
    /**
     * 数据库操作对象
     */
    private IDAllocDao dao;

    /**
     * 线程工厂
     */
    public static class UpdateThreadFactory implements ThreadFactory {

        private static int threadInitNumber = 0;

        private static synchronized int nextThreadNum() {
            return threadInitNumber++;
        }

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "Thread-Segment-Update-" + nextThreadNum());
        }
    }

    // 这个方法会被spring识别为初始化方法
    // TODO: 2022/12/8 init方法不应该有返回值,应该换成try-catch方式
    @Override
    public boolean init() {
        logger.info("Init ...");
        // 确保加载到kv后才初始化成功
        updateCacheFromDb();
        initOK = true;
        // 设置周期性任务更新tag
        updateCacheFromDbAtEveryMinute();
        return initOK;
    }

    /**
     * 每分钟更新缓存
     */
    private void updateCacheFromDbAtEveryMinute() {
        // TODO: 2022/12/8 不要使用Exectors去创建线程,这里使用一个无界队列
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("check-idCache-thread");
                t.setDaemon(true);
                return t;
            }
        });
        service.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                updateCacheFromDb();
            }
        }, 60, 60, TimeUnit.SECONDS);
    }

    /**
     * 从数据库中更新缓存
     */
    private void updateCacheFromDb() {
        logger.info("update cache from db");
        StopWatch sw = new StopWatch("updateCacheFromDb");
        sw.start();
        try {
            // 拿到所有的业务tag
            List<String> dbTags = dao.getAllTags();
            // TODO: 2022/12/8 使用CollectionsUtils判空更方便
            if (CollectionUtils.isEmpty(dbTags)) {
                return;
            }
            // 以下逻辑是判断数据库中是否有新的业务
            // 先把Set变成List?
            List<String> cacheTags = new ArrayList<>(cache.keySet());
            // 从数据库拿到的
            Set<String> insertTagsSet = new HashSet<>(dbTags);
            // 原来的Key
            Set<String> removeTagsSet = new HashSet<>(cacheTags);
            // TODO: 2022/12/8 计算差集,可以使用Guava优化调用api直接计算 
            for(int i = 0; i < cacheTags.size(); i++){
                String tmp = cacheTags.get(i);
                if(insertTagsSet.contains(tmp)){
                    insertTagsSet.remove(tmp);
                }
            }
            // 给新key初始化缓存
            for (String tag : insertTagsSet) {
                // 初始化一个buffer
                SegmentBuffer buffer = new SegmentBuffer();
                buffer.setKey(tag);

                Segment segment = buffer.getCurrent();
                // 得到当前的segment初始化
                segment.setValue(new AtomicLong(0));
                segment.setMax(0);
                segment.setStep(0);
                cache.put(tag, buffer);
                logger.info("Add tag {} from db to IdCache, SegmentBuffer {}", tag, buffer);
            }
            //cache中已失效的tags从cache删除
            for(int i = 0; i < dbTags.size(); i++){
                String tmp = dbTags.get(i);
                if(removeTagsSet.contains(tmp)){
                    removeTagsSet.remove(tmp);
                }
            }
            for (String tag : removeTagsSet) {
                cache.remove(tag);
                logger.info("Remove tag {} from IdCache", tag);
            }
        } catch (Exception e) {
            logger.error("update cache from db exception", e);
        } finally {
            sw.stop();
            logger.info("updateFromDB cost time~~~~~~~~~~~~~~~~~{}",sw.prettyPrint());
        }
    }

    /**
     * 通过http调用,考虑线程安全问题
     * @param key
     * @return
     */
    @Override
    public Result get(final String key) {
        // 是否初始化好segmentbuffer
        if (!initOK) {
            return new Result(EXCEPTION_ID_IDCACHE_INIT_FALSE, Status.EXCEPTION);
        }
        if (cache.containsKey(key)) {
            SegmentBuffer buffer = cache.get(key);
            // 双重锁(DCL)判断
            if (!buffer.isInitOk()) {
                // 只要没有init,其他线程就得在这里阻塞着
                synchronized (buffer) {
                    if (!buffer.isInitOk()) {
                        try {
                            // 传入当前使用的segment,更新key的缓存
                            updateSegmentFromDb(key, buffer.getCurrent());
                            logger.info("Init buffer. Update leafkey {} {} from db", key, buffer.getCurrent());
                            // 标记缓存初始化完成
                            buffer.setInitOk(true);
                        } catch (Exception e) {
                            logger.warn("Init buffer {} exception", buffer.getCurrent(), e);
                        }
                    }
                }
            }
            return getIdFromSegmentBuffer(cache.get(key));
        }
        return new Result(EXCEPTION_ID_KEY_NOT_EXISTS, Status.EXCEPTION);
    }
    // 将对应key的下一个区间填充segment
    public void updateSegmentFromDb(String key, Segment segment) {
        //todo:这部分用法需要优化
        StopWatch sw = new StopWatch("updateSegmentFromDb"+ key + " " + segment);
        sw.start();
        // 获取当前buffer
        SegmentBuffer buffer = segment.getBuffer();
        LeafAlloc leafAlloc;
        // 如果是第一次访问
        if (!buffer.isInitOk()) {
            leafAlloc = dao.updateMaxIdAndGetLeafAlloc(key);
            buffer.setStep(leafAlloc.getStep());
            buffer.setMinStep(leafAlloc.getStep());//leafAlloc中的step为DB中的step
        } else if (buffer.getUpdateTimestamp() == 0) {// 第二次开始开始记录时间？
            leafAlloc = dao.updateMaxIdAndGetLeafAlloc(key);
            buffer.setUpdateTimestamp(System.currentTimeMillis());
            buffer.setStep(leafAlloc.getStep());
            buffer.setMinStep(leafAlloc.getStep());//leafAlloc中的step为DB中的step
        } else {
            long duration = System.currentTimeMillis() - buffer.getUpdateTimestamp();
            int nextStep = buffer.getStep();
            // 如果间隔小于最小值并且步长可以调整,那么调大二倍
            // 如果间隔大于SEGMENT_DURATION * 2,那么减小一倍
//            if (duration < SEGMENT_DURATION) {
//                if (nextStep * 2 > MAX_STEP) {
//                    //do nothing
//                } else {
//                    nextStep = nextStep * 2;
//                }
//            } else if (duration < SEGMENT_DURATION * 2) {
//                //do nothing with nextStep
//            } else {
//                nextStep = nextStep / 2 >= buffer.getMinStep() ? nextStep / 2 : nextStep;
//            }
            if (duration < SEGMENT_DURATION && nextStep * 2 <= MAX_STEP){
                nextStep = nextStep * 2;
            }else if (duration >= SEGMENT_DURATION){
                nextStep = nextStep>>1>=buffer.getMinStep()?nextStep>>1:nextStep;
            }
            logger.info("leafKey[{}], step[{}], duration[{}mins], nextStep[{}]", key, buffer.getStep(), String.format("%.2f",((double)duration / (1000 * 60))), nextStep);
            LeafAlloc temp = new LeafAlloc();
            temp.setKey(key);
            temp.setStep(nextStep);
            // 更新步长并获取完整的数据库对象
            leafAlloc = dao.updateMaxIdByCustomStepAndGetLeafAlloc(temp);
            buffer.setUpdateTimestamp(System.currentTimeMillis());
            buffer.setStep(nextStep);
            buffer.setMinStep(leafAlloc.getStep());//leafAlloc的step为DB中的step
        }
        // must set value before set max
        // 其实节点
        long value = leafAlloc.getMaxId() - leafAlloc.getStep();
        segment.getValue().set(value);
        segment.setMax(leafAlloc.getMaxId());
        segment.setStep(leafAlloc.getStep());
        sw.stop();

    }

    public Result getIdFromSegmentBuffer(final SegmentBuffer buffer) {
        while (true) {
            // 先加读锁,因为后续需要读取到buffer相关信息
            buffer.rLock().lock();
            try {
                final Segment segment = buffer.getCurrent();
                // 首先用量得达到阈值,然后下一个buffer没有准备好,并且没有线程占用就
                if ((segment.getIdle() < 0.9 * segment.getStep())
                        && !buffer.isNextReady()
                        && buffer.getThreadRunning().compareAndSet(false, true)) {
                    // 这里是遵循阿里巴巴规范,自定义线程池
                    // 当前线程启动service会直接接着执行
                    service.execute(new Runnable() {
                        @Override
                        public void run() {
                            // 得到另一个Segment
                            Segment next = buffer.getSegments()[buffer.nextPos()];
                            boolean updateOk = false;
                            try {
                                updateSegmentFromDb(buffer.getKey(), next);
                                updateOk = true;
                                logger.info("update segment {} from db {}", buffer.getKey(), next);
                            } catch (Exception e) {
                                logger.warn(buffer.getKey() + " updateSegmentFromDb exception", e);
                            } finally {
                                // 因为修改的逻辑并不是一条语句就可以完成的,所以需要读写锁
                                // 这个锁需要保证临界资源的访问在一个锁块中
                                if (updateOk) {
                                    buffer.wLock().lock();
                                    // 为什么不好人做到底进行替换操作,                  为了用干净另一个segment
                                    buffer.setNextReady(true);
                                    buffer.getThreadRunning().set(false);
                                    buffer.wLock().unlock();
                                } else {
                                    buffer.getThreadRunning().set(false);
                                }
                            }
                        }
                    });
                }
                long value = segment.getValue().getAndIncrement();
                // 保证没有扩展能力的可以进行
                if (value < segment.getMax()) {
                    return new Result(value, Status.SUCCESS);
                }
            } finally {
                // 释放读锁,以求后面替换Segment的操作
                buffer.rLock().unlock();
            }
            // 上面填充好另一个segment
            // 下面开始判断切换
            waitAndSleep(buffer);
            // 更换期间有一大堆线程在这里阻塞
            buffer.wLock().lock();
            try {
                // 有可能之前的线程填充好了
                final Segment segment = buffer.getCurrent();
                long value = segment.getValue().getAndIncrement();
                if (value < segment.getMax()) {
                    return new Result(value, Status.SUCCESS);
                }
                // 更换buffer的线程需要进到下一次
                if (buffer.isNextReady()) {
                    buffer.switchPos();
                    buffer.setNextReady(false);
                } else {
                    logger.error("Both two segments in {} are not ready!", buffer);
                    return new Result(EXCEPTION_ID_TWO_SEGMENTS_ARE_NULL, Status.EXCEPTION);
                }
            } finally {
                buffer.wLock().unlock();
            }
        }
    }

    private void waitAndSleep(SegmentBuffer buffer) {
        int roll = 0;
        while (buffer.getThreadRunning().get()) {
            roll += 1;
            if(roll > 10000) {
                try {
                    TimeUnit.MILLISECONDS.sleep(10);
                    break;
                } catch (InterruptedException e) {
                    logger.warn("Thread {} Interrupted",Thread.currentThread().getName());
                    break;
                }
            }
        }
    }

    public List<LeafAlloc> getAllLeafAllocs() {
        return dao.getAllLeafAllocs();
    }

    public Map<String, SegmentBuffer> getCache() {
        return cache;
    }

    public IDAllocDao getDao() {
        return dao;
    }

    public void setDao(IDAllocDao dao) {
        this.dao = dao;
    }
}
