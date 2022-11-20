package cn.darkjrong.hbase.common.callback;

import cn.darkjrong.hbase.common.exceptions.HbaseException;
import org.apache.hadoop.hbase.client.ResultScanner;

/**
 * 结果提取器
 *
 * @author Rong.Jia
 * @date 2022/11/19
 */
public interface ResultsExtractor<T> {

    /**
     * 提取数据
     *
     * @param scanner 扫描仪
     * @return {@link T}
     * @throws HbaseException hbase异常
     */
    T extractData(ResultScanner scanner) throws HbaseException;

}
