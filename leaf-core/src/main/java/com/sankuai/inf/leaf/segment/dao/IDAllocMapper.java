package com.sankuai.inf.leaf.segment.dao;

import com.sankuai.inf.leaf.segment.model.LeafAlloc;
import org.apache.ibatis.annotations.*;


import java.util.List;

public interface IDAllocMapper {

    @Select("SELECT biz_tag, max_id, step, update_time FROM leaf_alloc")
    @Results(value = {
            @Result(column = "biz_tag", property = "key"),
            @Result(column = "max_id", property = "maxId"),
            @Result(column = "step", property = "step"),
            @Result(column = "update_time", property = "updateTime")
    })
    List<LeafAlloc> getAllLeafAllocs();

    /**
     *
     * @param tag
     * @return
     */
    @Select("SELECT biz_tag, max_id, step FROM leaf_alloc WHERE biz_tag = #{tag}")
    @Results(value = {
            @Result(column = "biz_tag", property = "key"),
            @Result(column = "max_id", property = "maxId"),
            @Result(column = "step", property = "step")
    })
    LeafAlloc getLeafAlloc(@Param("tag") String tag);

    @Update("UPDATE leaf_alloc SET max_id = max_id + step WHERE biz_tag = #{tag}")
    void updateMaxId(@Param("tag") String tag);

    /**
     * 更新maxId根据自定义间隔
     * @param leafAlloc
     */
    @Update("UPDATE leaf_alloc SET max_id = max_id + #{step} WHERE biz_tag = #{key}")
    void updateMaxIdByCustomStep(@Param("leafAlloc") LeafAlloc leafAlloc);

    /**
     * 查询所有的业务tag
     * @return
     */
    @Select("SELECT biz_tag FROM leaf_alloc")
    List<String> getAllTags();
}
