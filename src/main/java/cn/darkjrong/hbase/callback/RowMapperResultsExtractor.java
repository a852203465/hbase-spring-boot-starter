package cn.darkjrong.hbase.callback;

import cn.darkjrong.hbase.HbaseException;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.lang.Assert;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.util.List;
import java.util.Optional;

/**
 * 行映射结果提取器
 *
 * @author Rong.Jia
 * @date 2022/11/19
 */
public class RowMapperResultsExtractor<T> implements ResultsExtractor<List<T>> {

    private final RowMapper<T> rowMapper;

    public RowMapperResultsExtractor(RowMapper<T> rowMapper) {
        Assert.notNull(rowMapper, "RowMapper is required");
        this.rowMapper = rowMapper;
    }

    @Override
    public List<T> extractData(ResultScanner scanner) throws HbaseException {
        List<T> rs = CollectionUtil.newArrayList();
        int rowNum = 0;
        for (Result result : scanner) {
            Optional.ofNullable(rowMapper.mapRow(result, rowNum++)).ifPresent(rs::add);
        }
        return rs;
    }
}
