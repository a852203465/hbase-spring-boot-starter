package cn.darkjrong.hbase;

import cn.darkjrong.hbase.service.StudentService;
import cn.darkjrong.hbase.service.impl.StudentServiceImpl;
import cn.hutool.core.util.ReflectUtil;
import com.alibaba.fastjson.JSON;
import org.apache.hadoop.hbase.util.Bytes;
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

        Student student = new Student();
        student.setName("贾荣");
        student.setEmail("852203465@qq.com");
        student.setAge(12);
        student.setSex("男");

        System.out.println(studentService.save(student));
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

    public static void main(String[] args) {

        byte[] bytes = Bytes.toBytes(1595401874058526720L);
        System.out.println();


    }







}
