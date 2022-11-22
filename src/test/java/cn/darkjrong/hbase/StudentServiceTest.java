package cn.darkjrong.hbase;

import cn.darkjrong.hbase.service.StudentService;
import cn.darkjrong.hbase.service.impl.StudentServiceImpl;
import cn.hutool.core.util.ReflectUtil;
import com.alibaba.fastjson.JSON;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

public class StudentServiceTest extends TestInit {

    private StudentService studentService;

    @BeforeEach
    public void before() {
        studentService = new StudentServiceImpl();
        ReflectUtil.setFieldValue(studentService, "hbaseTemplate", hbaseTemplate);
        ReflectUtil.setFieldValue(studentService, "objectMappedFactory", objectMappedFactory);
        ReflectUtil.setFieldValue(studentService, "rowKeyGeneratorFactory", rowKeyGeneratorFactory);
    }

    @Test
    void createTable() {

        Boolean student = hbaseTemplate.createTable("student");
        System.out.println(student);

    }

    @Test
    void deleteTable() {

        Boolean student = hbaseTemplate.deleteTable("student");
        System.out.println(student);

    }


    @Test
    void save() {

        for (int i = 0; i <100; i++) {
            Student student = new Student();
            student.setName("贾荣"+i);
            student.setEmail("852203465@qq.com"+i);
            student.setAge(12+i);
            student.setSex("男"+i);

            studentService.save(student);
        }
    }

    @Test
    void findAll() {

        List<Student> students = studentService.findAll();
        System.out.println(JSON.toJSONString(students));
    }









}
