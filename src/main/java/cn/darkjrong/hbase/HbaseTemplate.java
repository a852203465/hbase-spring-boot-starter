package cn.darkjrong.hbase;

import cn.darkjrong.hbase.callback.*;
import cn.darkjrong.hbase.domain.ServerInfo;
import cn.darkjrong.hbase.domain.TableInfo;
import cn.darkjrong.hbase.enums.HbaseExceptionEnum;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.lang.Assert;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.springframework.util.StopWatch;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * hbase模板
 *
 * @author Rong.Jia
 * @date 2022/11/19
 */
@Slf4j
@Getter
public class HbaseTemplate implements HbaseOperations {

    private final HBaseAdmin admin;
    private final Connection connection;

    public HbaseTemplate(Connection connection, HBaseAdmin admin) {
        this.admin = admin;
        this.connection = connection;
    }

    /**
     * 验证表是否存在
     *
     * @param tableName 表名
     * @return {@link Boolean}
     */
    public Boolean tableExists(String tableName) {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        try {
            return admin.tableExists(TableName.valueOf(tableName));
        } catch (IOException e) {
            log.error("tableExists" , e);
        }
        return Boolean.FALSE;
    }

    /**
     * 获取区域
     *
     * @param regionName 区域名字
     * @return {@link RegionInfo}
     */
    public RegionInfo getRegion(String regionName) {
        Assert.notBlank(regionName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "regionName"));

        List<RegionInfo> regionInfos = CollectionUtil.newArrayList();
        List<ServerInfo> regionServers = getRegionServers();
        if (CollectionUtil.isNotEmpty(regionServers)) {
            regionServers.forEach(a -> regionInfos.addAll(getRegionsByServer(a.getName())));
        }

        return regionInfos.stream()
                .filter(a -> StrUtil.equals(regionName, a.getRegionNameAsString()))
                .findAny().orElse(null);
    }

    /**
     * 获取表的区域
     *
     * @param tableName 表名
     * @return {@link List}<{@link RegionInfo}>
     */
    public List<RegionInfo> getRegionsByTable(String tableName) {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        try {
            return admin.getRegions(TableName.valueOf(tableName));
        } catch (IOException e) {
            log.error("getRegionsByTable" , e);
        }
        return Collections.emptyList();
    }

    /**
     * 获取区域根据服务名
     *
     * @param serverName 服务名
     * @return {@link List}<{@link RegionInfo}>
     */
    public List<RegionInfo> getRegionsByServer(String serverName) {
        Assert.notBlank(serverName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "serverName"));
        try {
            return admin.getRegions(ServerName.valueOf(serverName));
        } catch (IOException e) {
            log.error("getRegionsByServer" , e);
        }
        return Collections.emptyList();
    }

    /**
     * 获取表描述器
     *
     * @return {@link List}<{@link TableDescriptor}>
     */
    public List<TableDescriptor> getTableDescriptors() {
        try {
            return admin.listTableDescriptors();
        } catch (Exception e) {
            log.error("listTableDescriptors", e);
        }
        return Collections.emptyList();
    }

    /**
     * 根据正则表达式获取表描述器
     *
     * @param pattern          正则表达式
     * @return {@link List}<{@link TableDescriptor}>
     */
    public List<TableDescriptor> getTableDescriptors(Pattern pattern) {
        return getTableDescriptors(pattern, Boolean.FALSE);
    }

    /**
     * 根据正则表达式获取表描述器
     *
     * @param pattern          正则表达式
     * @param includeSysTables 是否包含系统表
     * @return {@link List}<{@link TableDescriptor}>
     */
    public List<TableDescriptor> getTableDescriptors(Pattern pattern, boolean includeSysTables) {
        Assert.notNull(pattern, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "pattern"));
        try {
           return admin.listTableDescriptors(pattern, includeSysTables);
        } catch (Exception e) {
            log.error("getTableDescriptors", e);
        }
        return Collections.emptyList();
    }

    /**
     * 获取表信息
     *
     * @return {@link List}<{@link TableInfo}>
     */
    public List<TableInfo> getTableNames() {
        try {
            TableName[] tableNames = admin.listTableNames();
            return tableInfos(CollectionUtil.newArrayList(tableNames));
        } catch (IOException e) {
            log.error("getTableNames", e);
        }
        return Collections.emptyList();
    }

    /**
     * 获取表信息
     *
     * @param tableName 表名
     * @return {@link TableInfo}
     */
    public TableInfo getTableName(String tableName) {
        List<TableInfo> tableNames = getTableNames();
        if (CollectionUtil.isNotEmpty(tableNames)) {
            return tableNames.stream()
                    .filter(a -> StrUtil.equals(a.getName(), tableName))
                    .findAny().orElse(null);
        }
        return null;
    }

    /**
     * 获取表信息
     *
     * @param pattern 正则表达式
     * @return {@link List}<{@link TableInfo}>
     */
    public List<TableInfo> getTableNames(Pattern pattern) {
        return getTableNames(pattern, Boolean.FALSE);
    }

    /**
     * 获取表信息
     *
     * @param pattern         正则表达式
     * @param includeSysTables 包括系统表
     * @return {@link List}<{@link TableInfo}>
     */
    public List<TableInfo> getTableNames(Pattern pattern, boolean includeSysTables) {
        Assert.notNull(pattern, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "pattern"));
        try {
            TableName[] tableNames = admin.listTableNames(pattern, includeSysTables);
            return tableInfos(CollectionUtil.newArrayList(tableNames));
        } catch (IOException e) {
            log.error("getTableNames", e);
        }
        return Collections.emptyList();
    }

    /**
     * 获取表描述符
     *
     * @param tableName 表名
     * @return {@link TableDescriptor}
     * @throws HbaseException hbase异常
     */
    public TableDescriptor getTableDescriptor(String tableName) throws HbaseException {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        try {
           return admin.getDescriptor(TableName.valueOf(tableName));
        } catch (IOException e) {
            log.error("getDescriptor", e);
            throw new HbaseException(e);
        }
    }

    /**
     * 创建表
     *
     * @param tableName 名字
     * @return {@link Boolean}
     */
    public Boolean createTable(String tableName) {
        return createTable(tableName, HbaseConstant.DEFAULT_COLUMN_FAMILY);
    }

    /**
     * 创建表
     *
     * @param tableName 名字
     * @return {@link Boolean}
     */
    public Boolean createTable(String tableName, String columnFamily) {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        if (StrUtil.isBlank(columnFamily)) {
            columnFamily = HbaseConstant.DEFAULT_COLUMN_FAMILY;
        }

        if (!tableExists(tableName)) {
            ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.newBuilder(HbaseUtils.toBytes(columnFamily)).setMaxVersions(1).build();
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName)).setColumnFamily(cfd).build();
            try {
                admin.createTable(tableDescriptor);
                return Boolean.TRUE;
            } catch (Exception e) {
                log.error("createTable", e);
                return Boolean.FALSE;
            }
        }
        return Boolean.TRUE;
    }

    /**
     * 创建表
     *
     * @param startKey   开始键
     * @param endKey     结束键
     * @param numRegions 要创建的区域的总数
     * @param tableName  表名
     */
    public Boolean createTable(String tableName, String startKey, String endKey, int numRegions) {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        if (!tableExists(tableName)) {
            ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.newBuilder(HbaseUtils.toBytes(HbaseConstant.DEFAULT_COLUMN_FAMILY)).setMaxVersions(1).build();
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName)).setColumnFamily(cfd).build();
            try {
                admin.createTable(tableDescriptor, HbaseUtils.toBytes(startKey), HbaseUtils.toBytes(endKey), numRegions);
                return Boolean.TRUE;
            } catch (Exception e) {
                log.error("createTable", e);
                return Boolean.FALSE;
            }
        }
        return Boolean.TRUE;
    }

    /**
     * 删除表
     *
     * @param tableName 表名
     * @return {@link Boolean}
     */
    public Boolean deleteTable(String tableName) {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        if (disableTable(tableName)) {
            try {
                admin.deleteTable(TableName.valueOf(tableName));
                return Boolean.TRUE;
            } catch (Exception e) {
                log.error("deleteTable", e);
                return Boolean.FALSE;
            }
        }
        return Boolean.TRUE;
    }

    /**
     * 禁用表
     *
     * @param tableName 表名
     * @return {@link Boolean}
     */
    public Boolean disableTable(String tableName) {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        if (tableExists(tableName) && !isTableDisabled(tableName)) {
            try {
                admin.disableTable(TableName.valueOf(tableName));
                return Boolean.TRUE;
            } catch (Exception e) {
                log.error("disableTable", e);
            }
        }
        return Boolean.TRUE;
    }

    /**
     * 表是否已禁用
     *
     * @param tableName 表名
     * @return {@link Boolean}
     */
    public Boolean isTableDisabled(String tableName) {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        if (tableExists(tableName)) {
            try {
                return admin.isTableDisabled(TableName.valueOf(tableName));
            } catch (Exception e) {
                log.error("isTableDisabled", e);
            }
        }
        return Boolean.FALSE;
    }

    /**
     * 开启表
     *
     * @param tableName 表名
     * @return {@link Boolean}
     */
    public Boolean enableTable(String tableName) {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        if (tableExists(tableName) && !isTableEnabled(tableName)) {
            try {
                admin.enableTable(TableName.valueOf(tableName));
                return Boolean.TRUE;
            } catch (Exception e) {
                log.error("enableTable", e);
            }
        }
        return Boolean.FALSE;
    }

    /**
     * 表是否已开启
     *
     * @param tableName 表名
     * @return {@link Boolean}
     */
    public Boolean isTableEnabled(String tableName) {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        if (tableExists(tableName)) {
            try {
                return admin.isTableEnabled(TableName.valueOf(tableName));
            } catch (Exception e) {
                log.error("isTableEnabled", e);
            }
        }
        return Boolean.FALSE;
    }

    /**
     * 表是否可用
     *
     * @param tableName 表名
     * @return {@link Boolean}
     */
    public Boolean isTableAvailable(String tableName) {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        if (tableExists(tableName)) {
            try {
                return admin.isTableAvailable(TableName.valueOf(tableName));
            } catch (Exception e) {
                log.error("isTableAvailable", e);
            }
        }
        return Boolean.FALSE;
    }

    /**
     * 指定表中添加列族
     *
     * @param tableName    表名
     * @param columnFamily 列族
     * @return {@link Boolean}
     */
    public Boolean addColumnFamily(String tableName, String columnFamily) {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        Assert.notBlank(columnFamily, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "columnFamily"));

        if (tableExists(tableName)) {
            ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.newBuilder(HbaseUtils.toBytes(columnFamily)).setMaxVersions(1).build();
            try {
                admin.addColumnFamily(TableName.valueOf(tableName), cfd);
                return Boolean.TRUE;
            } catch (IOException e) {
                log.error("addColumnFamily", e);
            }
        }
        return Boolean.FALSE;
    }

    /**
     * 指定表中删除列族
     *
     * @param tableName    表名
     * @param columnFamily 列族
     * @return {@link Boolean}
     */
    public Boolean deleteColumnFamily(String tableName, String columnFamily) {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        Assert.notBlank(columnFamily, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "columnFamily"));

        if (tableExists(tableName)) {
            try {
                admin.deleteColumnFamily(TableName.valueOf(tableName), HbaseUtils.toBytes(columnFamily));
                return Boolean.TRUE;
            } catch (IOException e) {
                log.error("deleteColumnFamily", e);
            }
        }
        return Boolean.FALSE;
    }

    /**
     * 修改列族
     *
     * @param tableName    表名
     * @param columnFamily 列族
     * @return {@link Boolean}
     */
    public Boolean modifyColumnFamily(String tableName, String columnFamily) {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        Assert.notBlank(columnFamily, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "columnFamily"));
        if (tableExists(tableName)) {
            ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.newBuilder(HbaseUtils.toBytes(columnFamily))
                    .setMaxVersions(1).build();
            try {
                admin.modifyColumnFamily(TableName.valueOf(tableName), cfd);
                return Boolean.TRUE;
            } catch (IOException e) {
                log.error("modifyColumnFamily", e);
            }
        }
        return Boolean.FALSE;
    }

    /**
     * 刷新表
     *
     * @param tableName 表名
     * @return {@link Boolean}
     */
    public Boolean flushTable(String tableName) {
      return flushTable(tableName, null);
    }

    /**
     * 刷新表
     *
     * @param tableName    表名
     * @param columnFamily 列族
     * @return {@link Boolean}
     */
    public Boolean flushTable(String tableName, String columnFamily) {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        if (tableExists(tableName)) {
            try {
                admin.flush(TableName.valueOf(tableName), StrUtil.isBlank(columnFamily) ? null : HbaseUtils.toBytes(columnFamily));
                return Boolean.TRUE;
            } catch (IOException e) {
                log.error("flushTable", e);
            }
        }
        return Boolean.FALSE;
    }

    /**
     * 刷新区域
     *
     * @param regionName 区域
     * @return {@link Boolean}
     */
    public Boolean flushRegion(String regionName) {
        return flushRegion(regionName, null);
    }

    /**
     * 刷新区域
     * @param columnFamily 列族
     * @param regionName 区域
     * @return {@link Boolean}
     */
    public Boolean flushRegion(String regionName, String columnFamily) {
        Assert.notBlank(regionName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "regionName"));
        try {
            admin.flushRegion(HbaseUtils.toBytes(regionName), StrUtil.isBlank(columnFamily) ? null : HbaseUtils.toBytes(columnFamily));
            return Boolean.TRUE;
        } catch (IOException e) {
            log.error("flushRegion", e);
        }
        return Boolean.FALSE;
    }

    /**
     * 刷新区域服务
     *
     * @param serverName 服务器名称
     * @return {@link Boolean}
     */
    public Boolean flushRegionServer(String serverName) {
        Assert.notBlank(serverName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "serverName"));
        try {
            admin.flushRegionServer(ServerName.valueOf(serverName));
            return Boolean.TRUE;
        } catch (IOException e) {
            log.error("flushRegionServer", e);
        }
        return Boolean.FALSE;
    }

    /**
     * 压缩表
     *
     * @param tableName 表名
     * @return {@link Boolean}
     */
    public Boolean compactTable(String tableName) {
        return compactTable(tableName, null);
    }

    /**
     * 压缩表
     *
     * @param tableName 表名
     * @return {@link Boolean}
     */
    public Boolean compactTable(String tableName, String columnFamily) {
       return compactTable(tableName, columnFamily, CompactType.NORMAL);
    }

    /**
     * 压缩表
     *
     * @param tableName    表名
     * @param columnFamily 列族
     * @param compactType  紧凑型
     * @return {@link Boolean}
     */
    public Boolean compactTable(String tableName, String columnFamily, CompactType compactType) {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        if (ObjectUtil.isEmpty(compactType)) {
            compactType = CompactType.NORMAL;
        }
        try {
            admin.compact(TableName.valueOf(tableName), HbaseUtils.toBytes(columnFamily), compactType);
            return Boolean.TRUE;
        } catch (Exception e) {
            log.error("compactTable", e);
        }
        return Boolean.FALSE;
    }

    /**
     * 压缩区域
     *
     * @param regionName   区域
     * @return {@link Boolean}
     */
    public Boolean compactRegion(String regionName) {
       return compactRegion(regionName, null);
    }

    /**
     * 压缩区域
     *
     * @param regionName   区域
     * @param columnFamily 列族
     * @return {@link Boolean}
     */
    public Boolean compactRegion(String regionName, String columnFamily) {
        Assert.notBlank(regionName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "regionName"));
        try {
            admin.compactRegion(HbaseUtils.toBytes(regionName), HbaseUtils.toBytes(columnFamily));
            return Boolean.TRUE;
        } catch (IOException e) {
            log.error("compactRegion", e);
        }
        return Boolean.FALSE;
    }

    /**
     * 主压缩表
     *
     * @param tableName    表名
     * @return {@link Boolean}
     */
    public Boolean majorCompactTable(String tableName) {
        return majorCompactTable(tableName, null);
    }

    /**
     * 主压缩表
     *
     * @param tableName    表名
     * @param columnFamily 列族
     * @return {@link Boolean}
     */
    public Boolean majorCompactTable(String tableName, String columnFamily) {
       return majorCompactTable(tableName, columnFamily, CompactType.NORMAL);
    }

    /**
     * 主压缩表
     *
     * @param tableName    表名
     * @param columnFamily 列族
     * @param compactType  紧凑型
     * @return {@link Boolean}
     */
    public Boolean majorCompactTable(String tableName, String columnFamily, CompactType compactType) {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        if (ObjectUtil.isEmpty(compactType)) {
            compactType = CompactType.NORMAL;
        }
        try {
            admin.majorCompact(TableName.valueOf(tableName), HbaseUtils.toBytes(columnFamily), compactType);
            return Boolean.TRUE;
        } catch (Exception e) {
            log.error("majorCompactTable", e);
        }
        return Boolean.FALSE;
    }

    /**
     * 主压缩区域
     *
     * @param regionName   区域
     * @return {@link Boolean}
     */
    public Boolean majorCompactRegion(String regionName) {
       return majorCompactRegion(regionName, null);
    }

    /**
     * 主压缩区域
     *
     * @param regionName   区域
     * @param columnFamily 列族
     * @return {@link Boolean}
     */
    public Boolean majorCompactRegion(String regionName, String columnFamily) {
        Assert.notBlank(regionName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "regionName"));
        try {
            admin.majorCompactRegion(HbaseUtils.toBytes(regionName), HbaseUtils.toBytes(columnFamily));
            return Boolean.TRUE;
        } catch (Exception e) {
            log.error("majorCompactRegion", e);
        }
        return Boolean.FALSE;
    }

    /**
     * 压缩区域服务
     *
     * @param serverName 服务器名称
     * @return {@link Boolean}
     */
    public Boolean compactRegionServer(String serverName) {
        Assert.notBlank(serverName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "serverName"));
        try {
            admin.compactRegionServer(ServerName.valueOf(serverName));
            return Boolean.TRUE;
        } catch (Exception e) {
            log.error("compactRegionServer", e);
        }
        return Boolean.FALSE;
    }

    /**
     * 主压缩区域服务
     *
     * @param serverName 服务器名称
     * @return {@link Boolean}
     */
    public Boolean majorCompactRegionServer(String serverName) {
        Assert.notBlank(serverName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "serverName"));
        try {
            admin.majorCompactRegionServer(ServerName.valueOf(serverName));
            return Boolean.TRUE;
        } catch (Exception e) {
            log.error("majorCompactRegionServer", e);
        }
        return Boolean.FALSE;
    }

    /**
     * 分配区域
     *
     * @param regionName 区域
     * @return {@link Boolean}
     */
    public Boolean assignRegion(String regionName) {
        Assert.notBlank(regionName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "regionName"));
        try {
            admin.assign(HbaseUtils.toBytes(regionName));
            return Boolean.TRUE;
        } catch (IOException e) {
            log.error("assignRegion", e);
        }
        return Boolean.FALSE;
    }

    /**
     * 取消分配区域
     *
     * @param regionName 区域
     * @return {@link Boolean}
     */
    public Boolean unassignRegion(String regionName) {
        Assert.notBlank(regionName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "regionName"));
        try {
            admin.unassign(HbaseUtils.toBytes(regionName));
            return Boolean.TRUE;
        } catch (IOException e) {
            log.error("unassignRegion", e);
        }
        return Boolean.FALSE;
    }

    /**
     * 获取区域服务
     *
     * @return {@link List}<{@link ServerInfo}>
     */
    public List<ServerInfo> getRegionServers() {
        return getRegionServers(Boolean.FALSE);
    }

    /**
     * 获取区域服务
     *
     * @param excludeDecommissionedRS 排除退役区域
     * @return {@link List}<{@link ServerInfo}>
     */
    public List<ServerInfo> getRegionServers(boolean excludeDecommissionedRS) {
        try {
            Collection<ServerName> regionServers = admin.getRegionServers(excludeDecommissionedRS);
            if (CollectionUtil.isNotEmpty(regionServers)) {
                return regionServers.stream().map(a -> {
                    ServerInfo serverInfo = new ServerInfo();
                    serverInfo.setName(a.getServerName());
                    serverInfo.setStartCode(a.getStartCode());
                    serverInfo.setHost(a.getHostname());
                    serverInfo.setPort(a.getPort());
                    return serverInfo;
                }).collect(Collectors.toList());
            }
        } catch (IOException e) {
            log.error("getRegionServers", e);
        }
        return Collections.emptyList();
    }

    /**
     * 移动区域
     *
     * @param regionName 区域名字
     */
    public Boolean moveRegion(String regionName) {
      return moveRegion(regionName, null);
    }

    /**
     * 移动区域
     *
     * @param regionName     区域名
     * @param destServerName 目标服务名
     * @return {@link Boolean}
     */
    public Boolean moveRegion(String regionName, String destServerName) {
        Assert.notBlank(regionName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "regionName"));
        RegionInfo regionInfo = getRegion(regionName);
        Assert.notNull(regionInfo, HbaseExceptionEnum.getException(HbaseExceptionEnum.SPECIFIED_VALUE, regionName));
        try {
            admin.move(regionInfo.getEncodedNameAsBytes(), StrUtil.isBlank(destServerName) ? null : ServerName.valueOf(destServerName));
            return Boolean.TRUE;
        } catch (IOException e) {
            log.error("moveRegion", e);
        }
        return Boolean.FALSE;
    }

    /**
     * 离线区域
     *
     * @param regionName 区域名
     * @return {@link Boolean}
     */
    public Boolean offlineRegion(String regionName) {
        Assert.notBlank(regionName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "regionName"));
        RegionInfo regionInfo = getRegion(regionName);
        Assert.notNull(regionInfo, HbaseExceptionEnum.getException(HbaseExceptionEnum.SPECIFIED_VALUE, regionName));
        try {
            admin.offline(HbaseUtils.toBytes(regionName));
            return Boolean.TRUE;
        } catch (IOException e) {
            log.error("offlineRegion", e);
        }
        return Boolean.FALSE;
    }

    /**
     * 合并区域
     *  需要合并的区域>=2
     *
     * @param regionNames 区域名
     * @param forcible   强行合并
     * @return {@link Boolean}
     */
    public Boolean mergeRegions(List<String> regionNames, boolean forcible) {
        Assert.notEmpty(regionNames, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "regionNames"));

        List<byte[]> regions = CollectionUtil.newArrayList();
        for (String regionName : regionNames) {
            Optional.ofNullable(getRegion(regionName)).ifPresent(a -> regions.add(a.getEncodedNameAsBytes()));
        }

        byte[][] bytes = new byte[regions.size()][];
        regions.toArray(bytes);

        try {
            Future<Void> future = admin.mergeRegionsAsync(bytes, forcible);
            future.get(admin.getSyncWaitTimeout(), TimeUnit.MILLISECONDS);
            return Boolean.TRUE;
        } catch (Exception e) {
            log.error("mergeRegions", e);
        }
        return Boolean.FALSE;
    }

    /**
     * 创建命名空间
     *
     * @param name 命名空间名
     * @return {@link Boolean}
     */
    public Boolean createNamespace(String name) {
        Assert.notBlank(name, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "name"));

        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(name).build();
        try {
            admin.createNamespace(namespaceDescriptor);
            return Boolean.TRUE;
        } catch (IOException e) {
            log.error("createNamespace", e);
        }
        return Boolean.FALSE;
    }

    /**
     * 创建命名空间
     *
     * @param name          命名空间名
     * @param configuration 配置
     * @return {@link Boolean}
     */
    public Boolean createNamespace(String name, Map<String, String> configuration) {
        Assert.notBlank(name, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "name"));

        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(name).addConfiguration(configuration).build();
        try {
            admin.createNamespace(namespaceDescriptor);
            return Boolean.TRUE;
        } catch (IOException e) {
            log.error("createNamespace", e);
        }
        return Boolean.FALSE;
    }

    /**
     * 修改命名空间
     *
     * @param name 命名空间名
     * @return {@link Boolean}
     */
    public Boolean modifyNamespace(String name, Map<String, String> configuration) {
        Assert.notBlank(name, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "name"));
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(name).addConfiguration(configuration).build();
        try {
            admin.modifyNamespace(namespaceDescriptor);
            return Boolean.TRUE;
        } catch (IOException e) {
            log.error("modifyNamespace", e);
        }
        return Boolean.FALSE;
    }

    /**
     * 修改命名空间
     *
     * @param name  命名空间名
     * @param key   关键
     * @param value 值
     * @return {@link Boolean}
     */
    public Boolean modifyNamespace(String name, String key, String value) {
        Assert.notBlank(name, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "name"));
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(name).addConfiguration(key, value).build();
        try {
            admin.modifyNamespace(namespaceDescriptor);
            return Boolean.TRUE;
        } catch (IOException e) {
            log.error("modifyNamespace", e);
        }
        return Boolean.FALSE;
    }

    /**
     * 删除命名空间
     *
     * @param name 命名空间名
     * @return {@link Boolean}
     */
    public Boolean deleteNamespace(String name) {
        Assert.notBlank(name, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "name"));
        try {
            admin.deleteNamespace(name);
            return Boolean.TRUE;
        } catch (IOException e) {
            log.error("deleteNamespace", e);
        }
        return Boolean.FALSE;
    }

    /**
     * 删除命名空间
     *
     * @param name 命名空间名
     * @param key  配置KEY
     * @return {@link Boolean}
     */
    public Boolean deleteNamespace(String name, String  key) {
        Assert.notBlank(name, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "name"));
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(name).removeConfiguration(key).build();
        try {
            admin.modifyNamespace(namespaceDescriptor);
            return Boolean.TRUE;
        } catch (IOException e) {
            log.error("deleteNamespace", e);
        }
        return Boolean.FALSE;
    }

    /**
     * 获取命名空间描述符
     *
     * @param name 命名空间
     * @return {@link NamespaceDescriptor}
     * @throws HbaseException hbase异常
     */
    public NamespaceDescriptor getNamespaceDescriptor(String name) throws HbaseException {
        Assert.notBlank(name, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "name"));
        try {
            return admin.getNamespaceDescriptor(name);
        } catch (Exception e) {
            log.error("getNamespaceDescriptor", e);
            throw new HbaseException(e);
        }
    }

    /**
     * 获取命名空间
     *
     * @return {@link List}<{@link String}>
     */
    public List<String> getNamespaces() {
        try {
            return CollectionUtil.newArrayList(admin.listNamespaces());
        } catch (IOException e) {
            log.error("listNamespaces", e);
        }
        return Collections.emptyList();
    }

    /**
     * 获取命名空间描述符
     *
     * @return {@link List}<{@link NamespaceDescriptor}>
     */
    public List<NamespaceDescriptor> getNamespaceDescriptors() {
        try {
            return CollectionUtil.newArrayList(admin.listNamespaceDescriptors());
        } catch (IOException e) {
            log.error("getNamespaceDescriptors", e);
        }
        return Collections.emptyList();
    }

    /**
     * 获取表描述符根据命名空间
     *
     * @param name 名字
     * @return {@link List}<{@link TableDescriptor}>
     */
    public List<TableDescriptor> getTableDescriptorsByNamespace(String name) {
        Assert.notBlank(name, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "name"));
        try {
            return admin.listTableDescriptorsByNamespace(HbaseUtils.toBytes(name));
        } catch (Exception e) {
            log.error("getTableDescriptorsByNamespace", e);
        }
        return Collections.emptyList();
    }

    /**
     * 获取表根据命名空间
     *
     * @param name 名字
     * @return {@link List}<{@link TableName}>
     */
    public List<TableName> getTableNamesByNamespace(String name) {
        Assert.notBlank(name, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "name"));
        try {
            return CollectionUtil.newArrayList(admin.listTableNamesByNamespace(name));
        } catch (Exception e) {
            log.error("getTableNamesByNamespace", e);
        }
        return Collections.emptyList();
    }

    /**
     * 获取表描述符
     *
     * @param tableNames 表名
     * @return {@link List}<{@link TableDescriptor}>
     */
    public List<TableDescriptor> getTableDescriptors(List<String> tableNames) {
        Assert.notEmpty(tableNames, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableNames"));

        List<TableName> names = tableNames.stream().map(TableName::valueOf).collect(Collectors.toList());
        try {
            return admin.listTableDescriptors(names);
        } catch (Exception e) {
            log.error("getTableDescriptors", e);
        }
        return Collections.emptyList();
    }

    /**
     * 获取压缩状态
     *
     * @param tableName 表名
     * @return {@link CompactionState}
     */
    public CompactionState getCompactionStateByTable(String tableName) {
        return getCompactionStateByTable(tableName, CompactType.NORMAL);
    }

    /**
     * 获取压缩状态
     *
     * @param tableName   表名
     * @param compactType 压缩类型
     * @return {@link CompactionState}
     */
    public CompactionState getCompactionStateByTable(String tableName, CompactType compactType) throws HbaseException {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        Assert.isTrue(tableExists(tableName), HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, tableName));

        try {
            return admin.getCompactionState(TableName.valueOf(tableName), compactType);
        } catch (Exception e) {
            log.error("getCompactionStateByTable", e);
            throw new HbaseException(e);
        }
    }

    /**
     * 获取压缩状态根据区域
     *
     * @param regionName 区域
     * @return {@link CompactionState}
     * @throws HbaseException hbase异常
     */
    public CompactionState getCompactionStateByRegion(String regionName) throws HbaseException {
        Assert.notBlank(regionName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "regionName"));
        Assert.notNull(getRegion(regionName), HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, regionName));

        try {
            return admin.getCompactionStateForRegion(HbaseUtils.toBytes(regionName));
        } catch (Exception e) {
            log.error("getCompactionStateByRegion", e);
            throw new HbaseException(e);
        }

    }

    /**
     * 查询表的主要压缩的最新时间
     *
     * @param tableName 表名
     * @return {@link Long}
     */
    public Long getLastMajorCompactionTimestampByTable(String tableName) {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        if (tableExists(tableName)) {
            try {
                return admin.getLastMajorCompactionTimestamp(TableName.valueOf(tableName));
            } catch (IOException e) {
                log.error("getLastMajorCompactionTimestampByTable", e);
            }
        }
        return 0L;
    }

    /**
     * 查询区域的主要压缩的最新时间
     *
     * @param regionName 区域名字
     * @return {@link Long}
     */
    public Long getLastMajorCompactionTimestampByRegion(String regionName) {
        Assert.notBlank(regionName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "regionName"));
        if (ObjectUtil.isNotNull(getRegion(regionName))) {
            try {
                return admin.getLastMajorCompactionTimestampForRegion(HbaseUtils.toBytes(regionName));
            } catch (IOException e) {
                log.error("getLastMajorCompactionTimestampByRegion", e);
            }
        }
        return 0L;
    }

    /**
     * 创建快照
     *
     * @param snapshotName  快照名称
     * @param tableName     表名
     * @return {@link Boolean}
     */
    public Boolean createSnapshot(String snapshotName, String tableName) {
        return createSnapshot(snapshotName, tableName, SnapshotType.FLUSH);
    }

    /**
     * 创建快照
     *
     * @param snapshotName  快照名称
     * @param tableName     表名
     * @param type          类型
     * @return {@link Boolean}
     */
    public Boolean createSnapshot(String snapshotName, String tableName, SnapshotType type) {
        return createSnapshot(snapshotName, tableName, type, null);
    }

    /**
     * 创建快照
     *
     * @param snapshotName  快照名称
     * @param tableName     表名
     * @param type          类型
     * @param snapshotProps 快照属性
     * @return {@link Boolean}
     */
    public Boolean createSnapshot(String snapshotName, String tableName, SnapshotType type, Map<String, Object> snapshotProps) {
        Assert.notBlank(snapshotName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "snapshotName"));
        Assert.isTrue(tableExists(tableName), HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, tableName));

        try {
            admin.snapshot(snapshotName, TableName.valueOf(tableName), type, snapshotProps);
            return Boolean.TRUE;
        } catch (IOException e) {
            log.error("snapshot", e);
        }
        return Boolean.FALSE;
    }

    /**
     * 创建快照
     *
     * @param snapshotName  快照名称
     * @param tableName     表名
     * @param snapshotProps 快照属性
     * @return {@link Boolean}
     */
    public Boolean createSnapshot(String snapshotName, String tableName, Map<String, Object> snapshotProps) {
        return createSnapshot(snapshotName, tableName, SnapshotType.FLUSH, snapshotProps);
    }

    /**
     * 创建快照
     *
     * @param snapshot 快照描述器
     * @return {@link Boolean}
     */
    public Boolean createSnapshot(SnapshotDescription snapshot) {
        Assert.notNull(snapshot, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "snapshot"));
        try {
            admin.snapshot(snapshot);
            return Boolean.TRUE;
        } catch (IOException e) {
            log.error("snapshot", e);
        }
        return Boolean.FALSE;
    }

    /**
     * 查询快照状态
     *
     * @param snapshotName 快照名
     * @return {@link Boolean}
     */
    public Boolean isSnapshotFinished(String snapshotName) {
        Assert.notNull(snapshotName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "snapshotName"));
        SnapshotDescription snapshotDescription = new SnapshotDescription(snapshotName);
        return isSnapshotFinished(snapshotDescription);
    }

    /**
     * 查询快照状态
     *
     * @param snapshotName 快照名
     * @param tableName 表名
     * @return {@link Boolean}
     */
    public Boolean isSnapshotFinished(String snapshotName, String tableName) {
        return isSnapshotFinished(snapshotName, tableName, SnapshotType.FLUSH);
    }

    /**
     * 查询快照状态
     *
     * @param snapshotName 快照名
     * @param tableName 表名
     * @return {@link Boolean}
     */
    public Boolean isSnapshotFinished(String snapshotName, String tableName, SnapshotType type) {
        Assert.notNull(snapshotName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "snapshotName"));
        Assert.notNull(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        Assert.isTrue(tableExists(tableName), HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, tableName));

        SnapshotDescription snapshotDescription = new SnapshotDescription(snapshotName, TableName.valueOf(tableName), type);
        return isSnapshotFinished(snapshotDescription);
    }

    /**
     * 查询快照状态
     *
     * @param snapshot 快照
     * @return {@link Boolean}
     */
    public Boolean isSnapshotFinished(SnapshotDescription snapshot) {
        Assert.notNull(snapshot, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "snapshot"));
        try {
            return admin.isSnapshotFinished(snapshot);
        } catch (IOException e) {
            log.error("isSnapshotFinished", e);
        }
        return Boolean.FALSE;
    }

    /**
     * 恢复快照
     *
     * @param snapshotName 快照名称
     * @return {@link Boolean}
     */
    public Boolean restoreSnapshot(String snapshotName) {
        boolean takeFailSafeSnapshot = connection.getConfiguration()
                .getBoolean(HConstants.SNAPSHOT_RESTORE_TAKE_FAILSAFE_SNAPSHOT,
                        HConstants.DEFAULT_SNAPSHOT_RESTORE_TAKE_FAILSAFE_SNAPSHOT);
        return restoreSnapshot(snapshotName, takeFailSafeSnapshot);
    }

    /**
     * 恢复快照
     *
     * @param snapshotName 快照名称
     * @param takeFailSafeSnapshot 采取故障安全快照
     * @return {@link Boolean}
     */
    public Boolean restoreSnapshot(String snapshotName, boolean takeFailSafeSnapshot) {
        return restoreSnapshot(snapshotName, takeFailSafeSnapshot,Boolean.FALSE);
    }

    /**
     * 恢复快照
     *
     * @param snapshotName         快照名称
     * @param takeFailSafeSnapshot 采取故障安全快照
     * @param restoreAcl           重建acl
     * @return {@link Boolean}
     */
    public Boolean restoreSnapshot(String snapshotName, boolean takeFailSafeSnapshot, boolean restoreAcl) {
        Assert.notNull(snapshotName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "snapshotName"));
        try {
            admin.restoreSnapshot(snapshotName, takeFailSafeSnapshot, restoreAcl);
            return Boolean.TRUE;
        } catch (IOException e) {
            log.error("restoreSnapshot", e);
        }
        return Boolean.FALSE;
    }

    /**
     * 克隆快照
     *
     * @param snapshotName 快照名称
     * @param tableName    表名
     * @param restoreAcl   重建acl
     * @param customSFT    自定义sft
     * @return {@link Boolean}
     */
    public Boolean cloneSnapshot(String snapshotName, String tableName, boolean restoreAcl, String customSFT) {
        Assert.notBlank(snapshotName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "snapshotName"));
        Assert.isTrue(tableExists(tableName), HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, tableName));

        try {
            admin.cloneSnapshot(snapshotName, TableName.valueOf(tableName), restoreAcl, customSFT);
            return Boolean.TRUE;
        } catch (IOException e) {
            log.error("cloneSnapshot", e);
        }
        return Boolean.FALSE;
    }

    /**
     * 克隆快照
     *
     * @param snapshotName 快照名称
     * @param tableName    表名
     * @param restoreAcl   重建acl
     * @return {@link Boolean}
     */
    public Boolean cloneSnapshot(String snapshotName, String tableName, boolean restoreAcl) {
       return cloneSnapshot(snapshotName, tableName, restoreAcl, null);
    }

    /**
     * 克隆快照
     *
     * @param snapshotName 快照名称
     * @param tableName    表名
     * @return {@link Boolean}
     */
    public Boolean cloneSnapshot(String snapshotName, String tableName) {
        return cloneSnapshot(snapshotName, tableName, Boolean.FALSE);
    }

    /**
     * 获取快照
     *
     * @return {@link List}<{@link SnapshotDescription}>
     */
    public List<SnapshotDescription> getSnapshots() {
        try {
            return admin.listSnapshots();
        } catch (IOException e) {
            log.error("getSnapshots", e);
        }
        return Collections.emptyList();
    }

    /**
     * 获取快照
     *
     * @param snapshotName 快照名
     * @return {@link SnapshotDescription}
     */
    public SnapshotDescription getSnapshot(String snapshotName) {
        Assert.notNull(snapshotName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "snapshotName"));

        List<SnapshotDescription> snapshotDescriptions = getSnapshots();
        if (CollectionUtil.isNotEmpty(snapshotDescriptions)) {
            return snapshotDescriptions.stream()
                    .filter(a -> StrUtil.equals(a.getName(), snapshotName))
                    .findAny().orElse(null);
        }
        return null;
    }

    /**
     * 获取快照
     *
     * @param pattern 正则表达式
     * @return {@link List}<{@link SnapshotDescription}>
     */
    public List<SnapshotDescription> getSnapshots(Pattern pattern) {
        Assert.notNull(pattern, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "pattern"));
        try {
            return admin.listSnapshots(pattern);
        } catch (IOException e) {
            log.error("getSnapshots", e);
        }
        return Collections.emptyList();
    }

    /**
     * 获取表快照
     *
     * @param tableNamePattern    表名正则表达式
     * @param snapshotNamePattern 快照名称正则表达式
     * @return {@link List}<{@link SnapshotDescription}>
     */
    public List<SnapshotDescription> getTableSnapshots(Pattern tableNamePattern, Pattern snapshotNamePattern) {
        Assert.notNull(tableNamePattern, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableNamePattern"));
        Assert.notNull(snapshotNamePattern, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "snapshotNamePattern"));

        try {
            return admin.listTableSnapshots(tableNamePattern, snapshotNamePattern);
        } catch (IOException e) {
            log.error("getTableSnapshots", e);
        }
        return Collections.emptyList();
    }

    /**
     * 获取表快照
     *
     * @param tableName    表名
     * @return {@link List}<{@link SnapshotDescription}>
     */
    public List<SnapshotDescription> getTableSnapshots(String tableName) {
        Assert.notNull(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        Assert.isTrue(tableExists(tableName), HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, tableName));

        List<SnapshotDescription> snapshotDescriptions = getSnapshots();
        if (CollectionUtil.isNotEmpty(snapshotDescriptions)) {
            return snapshotDescriptions.stream()
                    .filter(a -> StrUtil.equals(a.getTableName().getNameAsString(), tableName))
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    /**
     * 删除快照
     *
     * @param snapshotName 快照名称
     * @return {@link Boolean}
     */
    public Boolean deleteSnapshot(String snapshotName) {
        Assert.notNull(snapshotName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "snapshotName"));
        if (ObjectUtil.isEmpty(getSnapshot(snapshotName))) return Boolean.TRUE;
        try {
            admin.deleteSnapshot(snapshotName);
            return Boolean.TRUE;
        } catch (IOException e) {
            log.error("deleteSnapshot", e);
        }
        return Boolean.FALSE;
    }

    /**
     * 删除快照
     *
     * @param pattern 正则表达式
     * @return {@link Boolean}
     */
    public Boolean deleteSnapshots(Pattern pattern) {
        Assert.notNull(pattern, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "pattern"));
        try {
            admin.deleteSnapshots(pattern);
            return Boolean.TRUE;
        } catch (IOException e) {
            log.error("deleteSnapshots", e);
        }
        return Boolean.FALSE;
    }

    /**
     * 删除表快照
     *
     * @param tableNamePattern    表名正则表达式
     * @param snapshotNamePattern 快照名称正则表达式
     * @return {@link Boolean}
     */
    public Boolean deleteTableSnapshots(Pattern tableNamePattern, Pattern snapshotNamePattern) {
        Assert.notNull(tableNamePattern, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableNamePattern"));
        Assert.notNull(snapshotNamePattern, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "snapshotNamePattern"));
        try {
            admin.deleteTableSnapshots(tableNamePattern, snapshotNamePattern);
            return Boolean.TRUE;
        } catch (IOException e) {
            log.error("deleteTableSnapshots", e);
        }
        return Boolean.FALSE;
    }

    /**
     * 删除表快照
     *
     * @param tableName    表名
     */
    public void deleteTableSnapshots(String tableName) {
        Assert.notNull(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        Assert.isTrue(tableExists(tableName), HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, tableName));

        List<SnapshotDescription> tableSnapshots = getTableSnapshots(tableName);
        if (CollectionUtil.isNotEmpty(tableSnapshots)) {
            tableSnapshots.forEach(a -> this.deleteSnapshot(a.getName()));
        }
    }

    /**
     * 开启表复制
     *
     * @param tableName 表名
     * @return {@link Boolean}
     */
    public Boolean enableTableReplication(String tableName) {
        Assert.notNull(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        Assert.isTrue(tableExists(tableName), HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, tableName));
        try {
            admin.enableTableReplication(TableName.valueOf(tableName));
            return Boolean.TRUE;
        } catch (IOException e) {
            log.error("enableTableReplication", e);
        }
        return Boolean.FALSE;
    }


    /**
     * 禁用表复制
     *
     * @param tableName 表名
     * @return {@link Boolean}
     */
    public Boolean disableTableReplication(String tableName) {
        Assert.notNull(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        Assert.isTrue(tableExists(tableName), HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, tableName));
        try {
            admin.disableTableReplication(TableName.valueOf(tableName));
            return Boolean.TRUE;
        } catch (IOException e) {
            log.error("disableTableReplication", e);
        }
        return Boolean.FALSE;
    }

    /**
     * 克隆表模
     *
     * @param tableName      表名
     * @param newTableName   新表名
     * @param preserveSplits 保持分裂
     * @return {@link Boolean}
     */
    public Boolean cloneTable(String tableName, String newTableName, boolean preserveSplits) {
        Assert.notNull(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        Assert.notNull(newTableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "newTableName"));
        Assert.isTrue(tableExists(tableName), HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, tableName));
        try {
            admin.cloneTableSchema(TableName.valueOf(tableName), TableName.valueOf(newTableName), preserveSplits);
            return Boolean.TRUE;
        } catch (IOException e) {
            log.error("cloneTable", e);
        }
        return Boolean.FALSE;
    }

    /**
     * 获取表
     *
     * @param tableName 表名
     * @return {@link Table}
     * @throws HbaseException hbase异常
     */
    public Table getTable(String tableName) throws HbaseException {
        Assert.notNull(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        try {
            return connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            log.error("getTable", e);
            throw new HbaseException(e);
        }
    }

    /**
     * 获取集群id
     *
     * @return {@link String}
     */
    public String getClusterId() {
        return connection.getClusterId();
    }

    @Override
    public <T> T execute(String tableName, MutatorCallback<T> action) {
        Assert.notNull(action, "Callback object must not be null");
        Assert.notNull(tableName, "No table specified");

        StopWatch sw = new StopWatch();
        sw.start();

        BufferedMutator mutator = null;
        try {
            BufferedMutatorParams mutatorParams = new BufferedMutatorParams(TableName.valueOf(tableName));
            mutator = this.getConnection().getBufferedMutator(mutatorParams.writeBufferSize(3 * 1024 * 1024));
            return action.doInMutator(mutator);
        } catch (Throwable throwable) {
            log.error("executeMutatorCallback", throwable);
            throw new HbaseException(throwable);
        } finally {
            sw.stop();
            HbaseUtils.close(mutator);
        }
    }

    @Override
    public <T> T execute(String tableName, TableCallback<T> action) {
        Assert.notNull(action, "Callback object must not be null");
        Assert.notNull(tableName, "No table specified");

        StopWatch sw = new StopWatch();
        sw.start();

        Table table = getTable(tableName);
        try {
            return action.doInTable(table);
        } catch (Throwable throwable) {
            log.error("executeTableCallback", throwable);
            throw new HbaseException(throwable);
        } finally {
            sw.stop();
            HbaseUtils.close(table);
        }
    }

    @Override
    public <T> T find(String tableName, ResultsExtractor<T> extractor) {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        return find(tableName, new Scan(), extractor);
    }

    @Override
    public <T> T find(String tableName, String columnFamily, ResultsExtractor<T> extractor) {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        Scan scan = new Scan();
        scan.addFamily(HbaseUtils.toBytes(columnFamily));
        return find(tableName, scan, extractor);
    }

    @Override
    public <T> T find(String tableName, String columnFamily, String qualifier, ResultsExtractor<T> extractor) {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        Scan scan = new Scan();
        scan.addColumn(HbaseUtils.toBytes(columnFamily), HbaseUtils.toBytes(qualifier));
        return find(tableName, scan, extractor);
    }

    @Override
    public <T> T find(String tableName, Scan scan, ResultsExtractor<T> action) {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        return execute(tableName, new TableCallback<T>() {
            @Override
            public T doInTable(Table table) {
                ResultScanner scanner = null;
                try {
                    scanner = table.getScanner(scan);
                    return action.extractData(scanner);
                } catch (Exception e) {
                    log.error("The 【{}】 table fails to query data, 【{}】", tableName, e);
                }finally {
                    HbaseUtils.close(scanner);
                }
                return null;
            }
        });
    }

    @Override
    public <T> List<T> find(String tableName, RowMapper<T> rowMapper) {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        return find(tableName, new Scan(), rowMapper);
    }

    @Override
    public <T> List<T> find(String tableName, String columnFamily, RowMapper<T> action) {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        Scan scan = new Scan();
        scan.addFamily(HbaseUtils.toBytes(columnFamily));
        return find(tableName, scan, action);
    }

    @Override
    public <T> List<T> find(String tableName, String columnFamily, String qualifier, RowMapper<T> action) {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        Scan scan = new Scan();
        scan.addColumn(HbaseUtils.toBytes(columnFamily), HbaseUtils.toBytes(qualifier));
        return find(tableName, scan, action);
    }

    @Override
    public <T> List<T> find(String tableName, Scan scan, RowMapper<T> action) {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        return find(tableName, scan, new RowMapperResultsExtractor<>(action));
    }

    @Override
    public <T> T get(String tableName, String rowKey, RowMapper<T> action) {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        return get(tableName, rowKey, null, null, action);
    }

    @Override
    public <T, ID extends Serializable> T get(String tableName, ID rowKey, RowMapper<T> rowMapper) {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        return get(tableName, rowKey, null, rowMapper);
    }

    @Override
    public <T> T get(String tableName, String rowKey, String columnFamily, RowMapper<T> action) {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        return get(tableName, rowKey, columnFamily, null, action);
    }

    @Override
    public <T, ID extends Serializable> T get(String tableName, ID rowKey, String columnFamily, RowMapper<T> rowMapper) {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        return execute(tableName, new TableCallback<T>() {
            @Override
            public T doInTable(Table table) {
                Get get = new Get(HbaseUtils.toBytes(rowKey));
                if (StrUtil.isNotBlank(columnFamily)) {
                    byte[] family = HbaseUtils.toBytes(columnFamily);
                    get.addFamily(family);
                }
                try {
                    Result result = table.get(get);
                    return rowMapper.mapRow(result, 0);
                } catch (IOException e) {
                    log.error("The 【{}】 table queries the exception according to 【{}】, 【{}】", tableName, rowKey, e);
                }
                return null;
            }
        });
    }

    @Override
    public <T> T get(String tableName, String rowKey, String columnFamily, String qualifier, RowMapper<T> action) {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        return get(tableName, (Serializable) rowKey, columnFamily, qualifier, action);
    }

    @Override
    public <T, ID extends Serializable> T get(String tableName, ID rowKey, String columnFamily, String qualifier, RowMapper<T> rowMapper) {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        return execute(tableName, new TableCallback<T>() {
            @Override
            public T doInTable(Table table) {
                Get get = new Get(HbaseUtils.toBytes(rowKey));
                if (StrUtil.isNotBlank(columnFamily)) {
                    byte[] family = HbaseUtils.toBytes(columnFamily);
                    if (StrUtil.isNotBlank(qualifier)) {
                        get.addColumn(family, HbaseUtils.toBytes(qualifier));
                    } else {
                        get.addFamily(family);
                    }
                }
                try {
                    Result result = table.get(get);
                    return rowMapper.mapRow(result, 0);
                } catch (IOException e) {
                    log.error("The 【{}】 table queries the 【{}】 field according to 【{}】, 【{}】", tableName, qualifier, rowKey, e);
                }
                return null;
            }
        });
    }

    @Override
    public void put(String tableName, String rowKey, String familyName, String qualifier, byte[] data) throws HbaseException {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        Assert.notBlank(rowKey, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "rowKey"));
        Assert.notBlank(familyName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "familyName"));
        Assert.notBlank(qualifier, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "qualifier"));
        Assert.isTrue(ArrayUtil.isNotEmpty(data), HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "data"));

        this.execute(tableName, new TableCallback<Void>() {
            @Override
            public Void doInTable(Table table) {
                Put put = new Put(HbaseUtils.toBytes(rowKey)).addColumn(HbaseUtils.toBytes(familyName), HbaseUtils.toBytes(qualifier), data);
                try {
                    table.put(put);
                } catch (IOException e) {
                    log.error("【{}】 field data in row 【{}】 of table 【{}】 was updated abnormally, 【{}】", qualifier, rowKey, tableName, e);
                    throw new HbaseException(e);
                }
                return null;
            }
        });
    }

    @Override
    public Boolean delete(String tableName, String rowKey, String columnFamily) {
        return this.delete(tableName, rowKey, columnFamily, null);
    }

    @Override
    public <ID extends Serializable> Boolean delete(String tableName, ID rowKey, String columnFamily) {
        return this.delete(tableName, (Serializable) rowKey, columnFamily, null);
    }

    @Override
    public <ID extends Serializable> Boolean delete(String tableName, Set<ID> rowKey, String columnFamily) {
        return this.delete(tableName, rowKey, columnFamily, null);
    }

    @Override
    public Boolean delete(String tableName, String rowKey, String columnFamily, String qualifier) {
        return this.delete(tableName, (Serializable) rowKey, columnFamily, qualifier);
    }

    @Override
    public <ID extends Serializable> Boolean delete(String tableName, ID rowKey, String columnFamily, String qualifier) {
        return this.delete(tableName, Collections.singleton(rowKey), columnFamily, qualifier);
    }

    @Override
    public <ID extends Serializable> Boolean delete(String tableName, Set<ID> rowKey, String columnFamily, String qualifier) {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        Assert.notEmpty(rowKey, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "rowKey"));
        Assert.notNull(columnFamily, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "columnFamily"));

        return this.execute(tableName, new TableCallback<Boolean>() {
            @Override
            public Boolean doInTable(Table table) {
                List<Delete> deletes = rowKey.stream().map(a -> {
                    Delete delete = new Delete(HbaseUtils.toBytes(a));
                    byte[] family = HbaseUtils.toBytes(columnFamily);
                    if (StrUtil.isNotBlank(qualifier)) {
                        delete.addColumn(family, HbaseUtils.toBytes(qualifier));
                    } else {
                        delete.addFamily(family);
                    }
                    return delete;
                }).collect(Collectors.toList());
                try {
                    table.delete(deletes);
                    return Boolean.TRUE;
                } catch (IOException e) {
                    log.error("【{}】 field data of row 【{}】 in table 【{}】 is abnormal, 【{}】", qualifier, rowKey, tableName, e);
                }
                return Boolean.FALSE;
            }
        });
    }

    @Override
    public void saveOrUpdate(String tableName, final Mutation mutation) throws HbaseException {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        saveOrUpdate(tableName, CollectionUtil.newArrayList(mutation));
    }

    @Override
    public void saveOrUpdate(String tableName, final List<Mutation> mutations) throws HbaseException {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        this.execute(tableName, new MutatorCallback<Void>() {
            @Override
            public Void doInMutator(BufferedMutator mutator) {
                try {
                    mutator.mutate(mutations);
                } catch (IOException e) {
                    log.error("【{}】Save or update exceptions, 【{}】",tableName, e);
                    throw new HbaseException(e);
                }
                return null;
            }
        });
    }

    @Override
    public <ID extends Serializable> Boolean exists(String tableName, ID rowKey) {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        Assert.notNull(rowKey, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "rowKey"));
        return this.execute(tableName, new TableCallback<Boolean>() {
            @Override
            public Boolean doInTable(Table table) {
                Get get = new Get(HbaseUtils.toBytes(rowKey));
                get.setCheckExistenceOnly(Boolean.TRUE);
                try {
                    return table.get(get).getExists();
                } catch (IOException e) {
                    log.error("Verify abnormal data on 【{}】 line of 【{}】 table, 【{}】", rowKey, tableName, e);
                }
                return Boolean.FALSE;
            }
        });
    }

    @Override
    public <T, ID extends Serializable> List<T> get(String tableName, Set<ID> rowKey, RowMapper<T> rowMapper) {
        return this.get(tableName, rowKey, null, rowMapper);
    }

    @Override
    public <T, ID extends Serializable> List<T> get(String tableName, Set<ID> rowKey, String columnFamily, RowMapper<T> rowMapper) {
        return this.get(tableName, rowKey, columnFamily, null, rowMapper);
    }

    @Override
    public <T, ID extends Serializable> List<T> get(String tableName, Set<ID> rowKey, String columnFamily, String qualifier, RowMapper<T> rowMapper) {
        Assert.notBlank(tableName, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "tableName"));
        Assert.notEmpty(rowKey, HbaseExceptionEnum.getException(HbaseExceptionEnum.GIVEN_VALUE, "rowKey"));
        return this.execute(tableName, new TableCallback<List<T>>() {
            @Override
            public List<T> doInTable(Table table) {
                List<Get> gets = rowKey.stream().map(a -> {
                    Get get = new Get(HbaseUtils.toBytes(a));
                    if (StrUtil.isNotBlank(columnFamily)) {
                        byte[] family = HbaseUtils.toBytes(columnFamily);
                        if (StrUtil.isNotBlank(qualifier)) {
                            get.addColumn(family, HbaseUtils.toBytes(qualifier));
                        } else {
                            get.addFamily(family);
                        }
                    }
                    return get;
                }).collect(Collectors.toList());

                try {
                    return rowMapper.mapRow(table.get(gets));
                } catch (IOException e) {
                    log.error("The 【{}】 table queries the 【{}】 field according to 【{}】, 【{}】", tableName, qualifier, rowKey, e);
                }
                return Collections.emptyList();
            }
        });
    }

    /**
     * 表信息
     *
     * @param tableNames 表名
     * @return {@link List}<{@link TableInfo}>
     */
    private static List<TableInfo> tableInfos(List<TableName> tableNames) {
        if (CollectionUtil.isEmpty(tableNames)) {
            return Collections.emptyList();
        }
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
    private static TableInfo tableInfo(TableName tableName) {
        if (ObjectUtil.isEmpty(tableName)) {
            return null;
        }
        TableInfo tableInfo = new TableInfo();
        tableInfo.setName(tableName.getNameAsString());
        tableInfo.setNamespace(tableName.getNamespaceAsString());
        tableInfo.setQualifier(tableName.getQualifierAsString());
        tableInfo.setSystemTable(tableName.isSystemTable());
        return tableInfo;
    }


}
