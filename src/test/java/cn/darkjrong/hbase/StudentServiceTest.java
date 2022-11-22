package cn.darkjrong.hbase;

import cn.darkjrong.hbase.service.StudentService;
import cn.darkjrong.hbase.service.impl.StudentServiceImpl;
import cn.hutool.core.util.ReflectUtil;
import com.alibaba.fastjson.JSON;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

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

        Student student = new Student();
        student.setName("贾荣"+1000);
        student.setEmail("852203465@qq.com"+1000);
        student.setAge(12+1000);
        student.setSex("男"+1000);

        System.out.println(studentService.save(student));
    }

    @Test
    void findAll() {

        List<Student> students = studentService.findAll();
        System.out.println(JSON.toJSONString(students));
    }

    @Test
    void findById() {

        Optional<Student> optional = studentService.findById(5L);
        System.out.println(optional.get());

    }









}
