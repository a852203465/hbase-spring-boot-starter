package cn.darkjrong.hbase.common.utils;

import cn.darkjrong.hbase.common.domain.TableInfo;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.ObjectUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.TableName;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * 属性工具类
 *
 * @author Rong.Jia
 * @date 2022/11/19
 */
@Slf4j
public class PropertiesUtils {

    /**
     * 表信息
     *
     * @param tableNames 表名
     * @return {@link List}<{@link TableInfo}>
     */
    public static List<TableInfo> tableInfos(List<TableName> tableNames) {
        if (CollectionUtil.isEmpty(tableNames)) return Collections.emptyList();
        List<TableInfo> tableInfos = new ArrayList<>();
        for (TableName tableName : tableNames) {
            Optional.ofNullable(tableInfo(tableName)).ifPresent(tableInfos::add);
        }
        return tableInfos;
    }

    /**
     * 表信息
     *
     * @param tableName 表名
     * @return {@link TableInfo}
     */
    public static TableInfo tableInfo(TableName tableName) {
        if (ObjectUtil.isEmpty(tableName)) return null;
        TableInfo tableInfo = new TableInfo();
        tableInfo.setName(tableName.getNameAsString());
        tableInfo.setNamespace(tableName.getNamespaceAsString());
        tableInfo.setQualifier(tableName.getQualifierAsString());
        tableInfo.setSystemTable(tableName.isSystemTable());
        return tableInfo;
    }











}
