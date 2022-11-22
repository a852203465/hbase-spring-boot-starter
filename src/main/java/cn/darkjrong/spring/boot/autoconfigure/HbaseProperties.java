package cn.darkjrong.spring.boot.autoconfigure;

import cn.darkjrong.hbase.enums.IdType;
import lombok.Data;
import org.apache.hadoop.hbase.HConstants;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Hbase 配置属性
 *
 * @author Rong.Jia
 * @date 2022/11/19
 */
@Data
@ConfigurationProperties(prefix = "spring.data.hbase")
public class HbaseProperties {

    /**
     * HBASE集群地址
     */
    private String quorum;

    /**
     * hbase持久化的目录,
     *  一般设置为hdfs://namenode.example.org:9000/hbase类似，带全限定名；
     */
    private String rootDir;

    /**
     * znode存放root region的地址
     */
    private String nodeParent = HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT;

    /**
     * 是否检查完整性， 默认：true
     */
    private boolean tableSanityChecks = Boolean.TRUE;

    /**
     * ID类型
     */
    private IdType idType = IdType.ASSIGN_ID;









}
