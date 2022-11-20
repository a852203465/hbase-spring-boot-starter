package cn.darkjrong.hbase.common.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 异常枚举
 *
 * @author Rong.Jia
 * @date 2022/11/19
 */
@Getter
@AllArgsConstructor
public enum ExceptionEnum {

    // 给定的值不能为空
    GIVEN_VALUE("The given 【%s】 must not be null!"),
    SPECIFIED_VALUE("The specified 【%s】 does not exist, please confirm"),
    TABLE_NAME_NOT_FOUND("'tableName' not found in target class"),
    ID_NOT_FOUND("The object 【{}】 did not find the primary key ID field. The object must specify the primary key ID or define a field with the attribute name 'id'"),












    ;

    private final String value;

    /**
     * 获取异常
     *
     * @param exceptionEnum 异常枚举
     * @param args          arg
     * @return {@link String}
     */
    public static String getException(ExceptionEnum exceptionEnum, Object... args) {
        return String.format(exceptionEnum.getValue(), args);
    }














}
