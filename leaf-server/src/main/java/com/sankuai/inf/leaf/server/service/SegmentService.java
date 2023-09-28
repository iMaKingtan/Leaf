package com.sankuai.inf.leaf.server.service;

import com.alibaba.druid.pool.DruidDataSource;
import com.sankuai.inf.leaf.IDGen;
import com.sankuai.inf.leaf.common.PropertyFactory;
import com.sankuai.inf.leaf.common.Result;
import com.sankuai.inf.leaf.common.ZeroIDGen;
import com.sankuai.inf.leaf.segment.SegmentIDGenImpl;
import com.sankuai.inf.leaf.segment.dao.IDAllocDao;
import com.sankuai.inf.leaf.segment.dao.impl.IDAllocDaoImpl;
import com.sankuai.inf.leaf.server.Constants;
import com.sankuai.inf.leaf.server.exception.InitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.Properties;

@Service("SegmentService")
public class SegmentService {
    private final static Logger LOGGER = LoggerFactory.getLogger(SegmentService.class);

    private IDGen idGen;
    // TODO: 2022/12/8 这里应该和数据源具体类型解耦
    private DruidDataSource dataSource;

    // TODO: 2022/12/8 可以将这部分逻辑加入到lifestyle,应该把这些代码放到init方法中,并在外部手动调用init方法 
    public SegmentService() throws SQLException, InitException {
        // 读取配置文件
        Properties properties = PropertyFactory.getProperties();
        // 获取是否开启了号段模式
        boolean flag = Boolean.parseBoolean(properties.getProperty(Constants.LEAF_SEGMENT_ENABLE, "true"));
        if (flag) {
            // 配置数据源
            // TODO: 2022/12/8 这里数据源直接写死 
            dataSource = new DruidDataSource();
            dataSource.setUrl(properties.getProperty(Constants.LEAF_JDBC_URL));
            dataSource.setUsername(properties.getProperty(Constants.LEAF_JDBC_USERNAME));
            dataSource.setPassword(properties.getProperty(Constants.LEAF_JDBC_PASSWORD));
            // TODO: 2022/12/8 连接池这两个参数一定要调,否则并发量上去直接超时
            dataSource.setMaxActive(10);
            dataSource.setMinIdle(5);
            dataSource.init();

            // TODO: 2022/12/8 数据库操作对象,可以交给spring管理
            IDAllocDao dao = new IDAllocDaoImpl(dataSource);

            // TODO: 2022/12/8 强转,没必要
            idGen = new SegmentIDGenImpl();
            ((SegmentIDGenImpl) idGen).setDao(dao);
            if (idGen.init()) {
                LOGGER.info("Segment Service Init Successfully");
            } else {
                throw new InitException("Segment Service Init Fail");
            }
        } else {
            idGen = new ZeroIDGen();
            LOGGER.info("Zero ID Gen Service Init Successfully");
        }
    }

    public Result getId(String key) {
        return idGen.get(key);
    }

    public SegmentIDGenImpl getIdGen() {
        if (idGen instanceof SegmentIDGenImpl) {
            return (SegmentIDGenImpl) idGen;
        }
        return null;
    }
}
