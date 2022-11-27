package cn.darkjrong.hbase;

import cn.darkjrong.hbase.domain.ServerInfo;
import cn.darkjrong.hbase.domain.TableInfo;
import cn.darkjrong.spring.boot.autoconfigure.HbaseFactoryBean;
import cn.darkjrong.spring.boot.autoconfigure.HbaseProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class HbaseTemplateTest {

    private static HbaseTemplate hbaseTemplate;

    @BeforeEach
    void before() throws IOException {
        HbaseProperties hbaseProperties = new HbaseProperties();
        hbaseProperties.setQuorum("127.0.0.1:2181");
        hbaseProperties.setRootDir("hdfs://localhost:8020/hbase");
        hbaseProperties.setNodeParent("/hbase");
        hbaseProperties.setTableSanityChecks(Boolean.TRUE);
        HbaseFactoryBean hbaseFactoryBean = new HbaseFactoryBean(hbaseProperties);
        hbaseFactoryBean.afterPropertiesSet();
        Configuration configuration = hbaseFactoryBean.getObject();
        Connection connection = ConnectionFactory.createConnection(configuration);
        HBaseAdmin hBaseAdmin = (HBaseAdmin) connection.getAdmin();
        hbaseTemplate = new HbaseTemplate(connection, hBaseAdmin);
    }

    @Test
    void tableExists() {

        Boolean stu = hbaseTemplate.tableExists("s1tu");
        System.out.println(stu);

    }

    @Test
    void getRegionsByTable() {

        List<RegionInfo> regions = hbaseTemplate.getRegionsByTable("stu");
        System.out.println(regions.toString());
    }

    @Test
    void getRegionsByServer() {

        List<ServerInfo> regionServers = hbaseTemplate.getRegionServers();

        ServerInfo serverInfo = regionServers.get(0);

        List<RegionInfo> regions = hbaseTemplate.getRegionsByServer(serverInfo.getName());
        System.out.println(regions.toString());
    }

    @Test
    void getTableDescriptors() {
        List<TableDescriptor> tableDescriptors = hbaseTemplate.getTableDescriptors();
        System.out.println(tableDescriptors.toString());
    }

    @Test
    void getTableDescriptors1() {

        Pattern compile = Pattern.compile("[a-z]{1,6}");
        List<TableDescriptor> tableDescriptors = hbaseTemplate.getTableDescriptors(compile);
        System.out.println(tableDescriptors.toString());

    }

    @Test
    void getTableNames() {

        List<TableInfo> tableInfos = hbaseTemplate.getTableNames();
        System.out.println(tableInfos.toString());

    }

    @Test
    void getTableNames1() {
        Pattern compile = Pattern.compile("[a-z]{1,6}");
        List<TableInfo> tableInfos = hbaseTemplate.getTableNames(compile);
        System.out.println(tableInfos.toString());
    }

    @Test
    void getTableNames3() {
        Pattern compile = Pattern.compile("[a-z]{1,6}");
        List<TableInfo> tableInfos = hbaseTemplate.getTableNames(compile, Boolean.TRUE);
        System.out.println(tableInfos.toString());
    }

    @Test
    void getDescriptor() {

        TableDescriptor descriptor = hbaseTemplate.getTableDescriptor("stu");
        System.out.println(descriptor);

    }

    @Test
    void createTable() {

        System.out.println(hbaseTemplate.createTable("person"));

    }

    @Test
    void deleteTable() {
        System.out.println(hbaseTemplate.deleteTable("person"));

    }

    @Test
    void isTableDisabled() {
        System.out.println(hbaseTemplate.isTableDisabled("person"));
    }

    @Test
    void disableTable() {
        System.out.println(hbaseTemplate.disableTable("person"));
    }

    @Test
    void isTableEnabled() {
        System.out.println(hbaseTemplate.isTableEnabled("person"));

    }

    @Test
    void enableTable() {
        System.out.println(hbaseTemplate.enableTable("person"));

    }

    @Test
    void isTableAvailable() {
        System.out.println(hbaseTemplate.isTableAvailable("person"));
    }

    @Test
    void addColumnFamily() {

        System.out.println(hbaseTemplate.addColumnFamily("person", "a"));

    }

    @Test
    void deleteColumnFamily() {
        System.out.println(hbaseTemplate.deleteColumnFamily("person", "a"));

    }

    @Test
    void modifyColumnFamily() {

        System.out.println(hbaseTemplate.modifyColumnFamily("person", "a"));


    }

    @Test
    void flushTable() {
        System.out.println(hbaseTemplate.flushTable("person"));
    }

    @Test
    void getRegionServers() {
        List<ServerInfo> regionServers = hbaseTemplate.getRegionServers();

        System.out.println(regionServers.toString());

    }

    @Test
    void getRegion() {
        RegionInfo region = hbaseTemplate.getRegion("stu,,1668782200719.40da4d6d488a100a0d054ddf5854def2.");
        System.out.println(region);

    }



    @Test
    void assign() {

        System.out.println(hbaseTemplate.assignRegion("192.168.87.1,16020,1668780661566"));

    }

    @Test
    void unassignRegion() {

        System.out.println(hbaseTemplate.unassignRegion("srcReg"));
    }

    @Test
    void createNamespace() {

        System.out.println(hbaseTemplate.createNamespace("testnamespace"));

    }

    @Test
    void modifyNamespace() {

        System.out.println(hbaseTemplate.modifyNamespace("testnamespace", "a", "b"));

    }

    @Test
    void deleteNamespace() {

        System.out.println(hbaseTemplate.deleteNamespace("testnamespace"));

    }

    @Test
    void getNamespaceDescriptor() {

        NamespaceDescriptor testnamespace = hbaseTemplate.getNamespaceDescriptor("testnamespace");
        System.out.println(testnamespace);

    }

    @Test
    void getNamespaces() {

        List<String> namespaces = hbaseTemplate.getNamespaces();
        System.out.println(namespaces.toString());
    }

    @Test
    void getNamespaceDescriptors() {

        List<NamespaceDescriptor> namespaceDescriptors = hbaseTemplate.getNamespaceDescriptors();
        System.out.println(namespaceDescriptors.toString());
    }

    @Test
    void getTableDescriptorsByNamespace() {

        List<TableDescriptor> tableDescriptors = hbaseTemplate.getTableDescriptorsByNamespace("hbase");
        System.out.println(tableDescriptors.toString());
    }

    @Test
    void getTableNamesByNamespace() {

        List<TableName> tableNames = hbaseTemplate.getTableNamesByNamespace("hbase");
        System.out.println(tableNames.toString());
    }

    @Test
    void getTableDescriptors2() {
        List<String> tableNames = new ArrayList<>();
        tableNames.add("stu");
        tableNames.add("person");
        List<TableDescriptor> tableDescriptors = hbaseTemplate.getTableDescriptors(tableNames);
        System.out.println(tableDescriptors.toString());
    }

    @Test
    void getTableDescriptor() {
        TableDescriptor tableDescriptor = hbaseTemplate.getTableDescriptor("stu");
        System.out.println(tableDescriptor.toString());
    }

    @Test
    void getCompactionStateByTable() {

        System.out.println(hbaseTemplate.getCompactionStateByTable("stu"));

    }

    @Test
    void getLastMajorCompactionTimestampByTable(){
        System.out.println(hbaseTemplate.getLastMajorCompactionTimestampByTable("stu"));
    }

    @Test
    void getLastMajorCompactionTimestampByRegion(){
        System.out.println(hbaseTemplate.getLastMajorCompactionTimestampByRegion("stu,,1668782200719.40da4d6d488a100a0d054ddf5854def2."));
    }

    @Test
    void createSnapshot() {

        System.out.println(hbaseTemplate.createSnapshot("aaa", "stu"));

    }

    @Test
    void isSnapshotFinished() {

        SnapshotDescription snapshotDescription = new SnapshotDescription("aaa");
        Boolean snapshotFinished = hbaseTemplate.isSnapshotFinished(snapshotDescription);
        System.out.println(snapshotFinished);
    }

    @Test
    void isSnapshotFinished2() {

        Boolean snapshotFinished = hbaseTemplate.isSnapshotFinished("aaa", "stu");
        System.out.println(snapshotFinished);
    }

    @Test
    void getSnapshots() {
        List<SnapshotDescription> snapshotDescriptions = hbaseTemplate.getSnapshots();
        System.out.println(snapshotDescriptions.toString());
    }

    @Test
    void getSnapshots2() {
        Pattern pattern = Pattern.compile("[a-z]{1,5}");
        List<SnapshotDescription> snapshotDescriptions = hbaseTemplate.getSnapshots(pattern);
        System.out.println(snapshotDescriptions.toString());
    }

    @Test
    void getSnapshot() {
        SnapshotDescription snapshotDescription = hbaseTemplate.getSnapshot("aaa");
        System.out.println(snapshotDescription.toString());
    }

    @Test
    void getTableSnapshots(){
        List<SnapshotDescription> snapshotDescriptions = hbaseTemplate.getTableSnapshots("stu");
        System.out.println(snapshotDescriptions.toString());
    }

    @Test
    void getTable() {
        Table table = hbaseTemplate.getTable("stu");
        System.out.println(table.toString());
    }

    @Test
    void getTableName() {
        TableInfo table = hbaseTemplate.getTableName("stu");
        System.out.println(table.toString());
    }

    @Test
    void getClusterId() {
        System.out.println(hbaseTemplate.getClusterId());
    }



















}
