package cn.darkjrong.hbase;

import cn.darkjrong.hbase.callback.MutatorCallback;
import cn.darkjrong.hbase.callback.ResultsExtractor;
import cn.darkjrong.hbase.callback.RowMapper;
import cn.darkjrong.hbase.callback.TableCallback;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Scan;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

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
     * @param rowKey   行Key
     * @param rowMapper 行映射器
     * @return {@link T}
     */
    <T> T get(String tableName, String rowKey, RowMapper<T> rowMapper);

    /**
     * 获取行
     *
     * @param tableName 表名
     * @param rowKey   行Key
     * @param rowMapper 行映射器
     * @return {@link T}
     */
    <T, ID extends Serializable> T get(String tableName, ID rowKey, RowMapper<T> rowMapper);

    /**
     * 获取行
     *
     * @param tableName    表名
     * @param rowKey      行Key
     * @param rowMapper       行映射器
     * @param columnFamily 列族
     * @return {@link T}
     */
    <T> T get(String tableName, String rowKey, String columnFamily, RowMapper<T> rowMapper);

    /**
     * 获取行
     *
     * @param tableName    表名
     * @param rowKey      行Key
     * @param rowMapper       行映射器
     * @param columnFamily 列族
     * @return {@link T}
     */
    <T, ID extends Serializable> T get(String tableName, ID rowKey, String columnFamily, RowMapper<T> rowMapper);

    /**
     * 获取行
     *
     * @param tableName    表名
     * @param rowKey      行Key
     * @param rowMapper    行映射器
     * @param columnFamily 列族
     * @param qualifier    字段名
     * @return {@link T}
     */
    <T> T get(String tableName, String rowKey, String columnFamily, String qualifier, RowMapper<T> rowMapper);

    /**
     * 获取行
     *
     * @param tableName    表名
     * @param rowKey      行Key
     * @param rowMapper    行映射器
     * @param columnFamily 列族
     * @param qualifier    字段名
     * @return {@link T}
     */
    <T, ID extends Serializable> T get(String tableName, ID rowKey, String columnFamily, String qualifier, RowMapper<T> rowMapper);

    /**
     * 插入单个数据
     *
     * @param tableName    表名
     * @param rowKey       行Key
     * @param qualifier    字段名
     * @param data         数据
     * @param columnFamily 列族
     * @return {@link Boolean}
     */
    Boolean put(String tableName, String rowKey, String columnFamily, String qualifier, byte[] data);

    /**
     * 删除
     *
     * @param tableName    表名
     * @param rowKey       行Key
     * @param columnFamily 列族
     * @return {@link Boolean}
     */
    Boolean delete(String tableName, final String rowKey, final String columnFamily);

    /**
     * 删除
     *
     * @param tableName    表名
     * @param rowKey       行Key
     * @param columnFamily 列族
     * @param qualifier    字段名
     * @return {@link Boolean}
     */
    Boolean delete(String tableName, final String rowKey, final String columnFamily, final String qualifier);

    /**
     * 保存或更新
     *
     * @param tableName 表名
     * @param mutation  操作数据
     * @return {@link Boolean}
     */
    Boolean saveOrUpdate(String tableName, Mutation mutation);

    /**
     * 保存或更新
     *
     * @param tableName 表名
     * @param mutations 操作数据
     * @return {@link Boolean}
     */
    Boolean saveOrUpdate(String tableName, List<Mutation> mutations);

    /**
     * 存在
     *
     * @param tableName 表名
     * @param rowKey    行键
     * @return {@link Boolean}
     */
    <ID extends Serializable> Boolean exists(String tableName, ID rowKey);

    /**
     * 获取多行
     *
     * @param tableName 表名
     * @param rowKey    行Key
     * @param rowMapper 行映射器
     * @return {@link List}<{@link T}>
     */
    <T, ID extends Serializable> List<T> get(String tableName, Set<ID> rowKey, RowMapper<T> rowMapper);

    /**
     * 获取行
     *
     * @param tableName    表名
     * @param rowKey       行Key
     * @param rowMapper    行映射器
     * @param columnFamily 列族
     * @return {@link List}<{@link T}>
     */
    <T, ID extends Serializable> List<T> get(String tableName, Set<ID> rowKey, String columnFamily, RowMapper<T> rowMapper);

    /**
     * 获取行
     *
     * @param tableName    表名
     * @param rowKey       行Key
     * @param rowMapper    行映射器
     * @param columnFamily 列族
     * @param qualifier    字段名
     * @return {@link List}<{@link T}>
     */
    <T, ID extends Serializable> List<T> get(String tableName, Set<ID> rowKey, String columnFamily, String qualifier, RowMapper<T> rowMapper);




}
