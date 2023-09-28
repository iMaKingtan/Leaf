package com.sankuai.inf.leaf;

import com.sankuai.inf.leaf.common.Result;

/**
 * 接口隔离,互不相关的接口单独抽取,总接口合并 spring的application context就是很好的例子,汇总了capEnvironment
 * 所有组件都应该有声明周期
 * id生成接口
 * todo: 定义号段模式和雪花模式两种类别的功能接口
 */
public interface IDGen {
    Result get(String key);
    boolean init();
}
