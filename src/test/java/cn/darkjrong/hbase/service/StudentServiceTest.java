package cn.darkjrong.hbase.service;

import cn.darkjrong.hbase.HbaseMappedFactoryTest;
import cn.darkjrong.hbase.HbaseTemplate;
import cn.darkjrong.hbase.Student;
import cn.darkjrong.hbase.service.impl.StudentServiceImpl;
import cn.darkjrong.spring.boot.autoconfigure.HbaseFactoryBean;
import cn.darkjrong.spring.boot.autoconfigure.HbaseProperties;
import cn.hutool.core.util.ReflectUtil;
import com.alibaba.fastjson.JSON;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

public class StudentServiceTest extends HbaseMappedFactoryTest {

    private StudentService studentService;
    private HbaseTemplate hbaseTemplate;

    @BeforeEach
    void before() throws IOException {
        HbaseProperties hbaseProperties = new HbaseProperties();
        hbaseProperties.setQuorum("127.0.0.1:2181");
        hbaseProperties.setRootDir("hdfs://localhost:8020/hbase");
        hbaseProperties.setNodeParent("/hbase");
        hbaseProperties.setTableSanityChecks(Boolean.TRUE);
        HbaseFactoryBean hbaseFactoryBean = new HbaseFactoryBean(hbaseProperties);
        hbaseFactoryBean.afterPropertiesSet();
        Configuration configuration = hbaseFactoryBean.getObject();
        Connection connection = ConnectionFactory.createConnection(configuration);
        HBaseAdmin hBaseAdmin = (HBaseAdmin) connection.getAdmin();
        hbaseTemplate = new HbaseTemplate(connection, hBaseAdmin);

        studentService = new StudentServiceImpl();
        ReflectUtil.setFieldValue(studentService, "hbaseTemplate", hbaseTemplate);
    }

    @Test
    void createTable() {

        Boolean student = hbaseTemplate.createTable("student");
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
