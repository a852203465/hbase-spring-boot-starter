package cn.darkjrong.hbase;

import cn.darkjrong.hbase.callback.MutatorCallback;
import cn.darkjrong.hbase.callback.TableCallback;
import cn.darkjrong.hbase.callback.ResultsExtractor;
import cn.darkjrong.hbase.callback.RowMapper;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Scan;

import java.util.List;

/**
 * hbase操作类
 *
 * @author Rong.Jia
 * @date 2022/11/19
 */
public interface HbaseOperations {

    /**
     * 执行
     *
     * @param tableName 表名
     * @param action    删除，修改回调
     * @return {@link T}
     */
    <T> T execute(String tableName, MutatorCallback<T> action);

    /**
     * 执行
     *
     * @param tableName 表名
     * @param callback  回调
     * @return {@link T}
     */
    <T> T execute(String tableName, TableCallback<T> callback);

    /**
     * 查询
     *
     * @param tableName    表名
     * @param extractor    结果提取器
     * @return {@link T}
     */
    <T> T find(String tableName, ResultsExtractor<T> extractor);

    /**
     * 查询
     *
     * @param tableName    表名
     * @param columnFamily 列族
     * @param extractor    结果提取器
     * @return {@link T}
     */
    <T> T find(String tableName, String columnFamily, ResultsExtractor<T> extractor);

    /**
     * 查询
     *
     * @param tableName    表名
     * @param columnFamily 列族
     * @param qualifier 字段名
     * @param extractor    结果提取器
     * @return {@link T}
     */
    <T> T find(String tableName, String columnFamily, String qualifier, ResultsExtractor<T> extractor);

    /**
     * 查询
     *
     * @param tableName 表名
     * @param scan      查询对象
     * @param extractor 结果提取器
     * @return {@link T}
     */
    <T> T find(String tableName, Scan scan, ResultsExtractor<T> extractor);

    /**
     * 查询多个
     *
     * @param tableName    表名
     * @param rowMapper    行映射器
     * @return {@link List}<{@link T}>
     */
    <T> List<T> find(String tableName, RowMapper<T> rowMapper);

    /**
     * 查询多个
     *
     * @param tableName    表名
     * @param columnFamily 列族
     * @param rowMapper    行映射器
     * @return {@link List}<{@link T}>
     */
    <T> List<T> find(String tableName, String columnFamily, RowMapper<T> rowMapper);

    /**
     * 查询多个
     *
     * @param tableName    表名
     * @param columnFamily 列族
     * @param qualifier 字段名
     * @param rowMapper    行映射器
     * @return {@link List}<{@link T}>
     */
    <T> List<T> find(String tableName, String columnFamily, String qualifier, RowMapper<T> rowMapper);

    /**
     * 查询多个
     *
     * @param tableName 表名
     * @param scan      查询对象
     * @param rowMapper 行映射器
     * @return {@link List}<{@link T}>
     */
    <T> List<T> find(String tableName, Scan scan, RowMapper<T> rowMapper);

    /**
     * 获取行
     *
     * @param tableName 表名
     * @param rowName   行名称
     * @param rowMapper 行映射器
     * @return {@link T}
     */
    <T> T get(String tableName, String rowName, RowMapper<T> rowMapper);

    /**
     * 获取行
     *
     * @param tableName    表名
     * @param rowName      行名称
     * @param rowMapper       行映射器
     * @param columnFamily 列族
     * @return {@link T}
     */
    <T> T get(String tableName, String rowName, String columnFamily, RowMapper<T> rowMapper);

    /**
     * 获取行
     *
     * @param tableName    表名
     * @param rowName      行名称
     * @param rowMapper    行映射器
     * @param columnFamily 列族
     * @param qualifier    字段名
     * @return {@link T}
     */
    <T> T get(String tableName, String rowName, String columnFamily, String qualifier, RowMapper<T> rowMapper);

    /**
     * 插入单个数据
     *
     * @param tableName    表名
     * @param rowName      行名称
     * @param qualifier    字段名
     * @param data         数据
     * @param columnFamily 列族
     */
    void put(String tableName, String rowName, String columnFamily, String qualifier, byte[] data);

    /**
     * 删除
     *
     * @param tableName    表名
     * @param rowName      行名称
     * @param columnFamily 列族
     */
    void delete(String tableName, final String rowName, final String columnFamily);

    /**
     * 删除
     *
     * @param tableName    表名
     * @param rowName      行名称
     * @param columnFamily 列族
     * @param qualifier    字段名
     */
    void delete(String tableName, final String rowName, final String columnFamily, final String qualifier);

    /**
     * 保存或更新
     *
     * @param tableName 表名
     * @param mutation  操作数据
     */
    void saveOrUpdate(String tableName, Mutation mutation);

    /**
     * 保存或更新
     *
     * @param tableName 表名
     * @param mutations 操作数据
     */
    void saveOrUpdate(String tableName, List<Mutation> mutations);






}
