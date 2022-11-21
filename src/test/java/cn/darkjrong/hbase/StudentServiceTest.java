package cn.darkjrong.hbase;

import cn.darkjrong.hbase.repository.SimpleHbaseRepository;
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
        SimpleHbaseRepository<Student, Integer> simpleHbaseRepository
                = new SimpleHbaseRepository<Student, Integer>(hbaseTemplate, objectMappedFactory);
        ReflectUtil.setFieldValue(studentService, "hbaseRepository", simpleHbaseRepository);
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
        student.setId(2);
        student.setName("贾荣2");
        student.setEmail("852203465@qq.com2");
        student.setAge(228);
        student.setSex("男2");

        studentService.save(student);

    }

    @Test
    void findAll() {

        List<Student> students = studentService.findAll();
        System.out.println(JSON.toJSONString(students));
    }









}
