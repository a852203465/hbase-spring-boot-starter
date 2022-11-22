package cn.darkjrong.hbase;

import cn.darkjrong.hbase.annotation.TableName;
import lombok.Data;

@Data
@TableName("student")
public class Student {

    private Long id;
    private String name;
    private String email;
    private Integer age;
    private String sex;

}
