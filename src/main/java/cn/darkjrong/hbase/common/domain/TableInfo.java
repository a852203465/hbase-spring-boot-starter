package cn.darkjrong.hbase.common.domain;

import lombok.Data;

import java.io.Serializable;

/**
 * 表信息
 *
 * @author Rong.Jia
 * @date 2022/11/19
 */
@Data
public class TableInfo implements Serializable {

    private static final long serialVersionUID = -1697698460721363688L;

    /**
     * 名称
     */
    private String name;

    /**
     * 命名空间
     */
    private String namespace;

    /**
     * 限定符
     */
    private String qualifier;

    /**
     * 是系统表
     */
    private Boolean systemTable;







}
