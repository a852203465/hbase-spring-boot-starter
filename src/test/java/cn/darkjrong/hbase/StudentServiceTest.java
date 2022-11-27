package cn.darkjrong.hbase;

import cn.darkjrong.hbase.service.StudentService;
import cn.darkjrong.hbase.service.impl.StudentServiceImpl;
import cn.hutool.core.util.ReflectUtil;
import com.alibaba.fastjson.JSON;
import org.apache.hadoop.hbase.util.Bytes;
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
        student.setName("贾荣"+10221010);
        student.setEmail("852203465@qq.com"+11001011);
        student.setAge(12+1101100);
        student.setSex("男"+1011001);

        System.out.println(studentService.save(student));
    }

    @Test
    void findAll() {

        List<Student> students = studentService.findAll();
        System.out.println(JSON.toJSONString(students));
    }

    @Test
    void findById() {

        Optional<Student> optional = studentService.findById(1595030848606953472L);
        System.out.println(optional.get());

    }

    public static void main(String[] args) {

        byte[] bytes = Bytes.toBytes(1595401874058526720L);
        System.out.println();


    }







}
