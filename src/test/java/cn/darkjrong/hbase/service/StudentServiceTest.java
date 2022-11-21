package cn.darkjrong.hbase.service;

import cn.darkjrong.hbase.Student;
import cn.darkjrong.hbase.TestInit;
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
        ReflectUtil.setFieldValue(studentService, "objectParser", objectParser);
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

        Student student = new Student();
        student.setId(1);
        student.setName("贾荣");
        student.setEmail("852203465@qq.com");
        student.setAge(28);
        student.setSex("男");

        studentService.save(student);

    }

    @Test
    void findAll() {

        List<Student> students = studentService.findAll();
        System.out.println(JSON.toJSONString(students));

    }









}
