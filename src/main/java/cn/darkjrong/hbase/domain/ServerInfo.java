package cn.darkjrong.hbase.domain;

import lombok.Data;

import java.io.Serializable;

/**
 * 服务器信息
 *
 * @author Rong.Jia
 * @date 2022/11/19
 */
@Data
public class ServerInfo implements Serializable {

    private static final long serialVersionUID = 8579668225228568216L;

    /**
     * 名称
     */
    private String name;

    /**
     * 编码
     */
    private Long startCode;

    /**
     * host
     */
    private String host;

    /**
     * 端口
     */
    private Integer port;



}
