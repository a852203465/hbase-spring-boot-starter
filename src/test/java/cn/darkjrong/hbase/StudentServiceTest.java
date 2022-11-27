package cn.darkjrong.hbase;

import cn.darkjrong.hbase.service.StudentService;
import cn.darkjrong.hbase.service.impl.StudentServiceImpl;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.ReflectUtil;
import com.alibaba.fastjson.JSON;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

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
        student.setName("贾荣1");
        student.setEmail("852203465@qq.com1");
        student.setAge(121);
        student.setSex("男1");
        studentService.save(student);
        System.out.println(student.getId());
    }

    @Test
    void findAll() {

        List<Student> students = studentService.findAll();
        System.out.println(JSON.toJSONString(students));
    }

    @Test
    void findById() {

        Student student = studentService.findById(1596717171369631744L);
        System.out.println(student);

    }

    @Test
    void existsById() {
        Boolean exists = studentService.existsById(1596717171369631744L);
        System.out.println(exists);
    }

    @Test
    void findAllById() {

        Set<Long> ids = CollectionUtil.newHashSet();
        ids.add(1596768913939542016L);
//        ids.add(1596769253082529792L);
//        ids.add(1596717171369631744L);

        List<Student> students = studentService.findAllById(ids);

        System.out.println(JSON.toJSONString(students));


    }






}
