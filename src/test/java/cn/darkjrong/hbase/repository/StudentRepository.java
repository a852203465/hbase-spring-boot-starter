package cn.darkjrong.hbase.repository;

import cn.darkjrong.hbase.Student;
import org.springframework.stereotype.Repository;

@Repository
public interface StudentRepository extends HbaseRepository<Student, Integer> {









}
