package cn.darkjrong.hbase;

import cn.darkjrong.hbase.annotation.ColumnName;
import cn.darkjrong.hbase.annotation.TableId;
import cn.darkjrong.hbase.annotation.TableName;
import cn.darkjrong.hbase.domain.ObjectMappedStatement;
import cn.darkjrong.hbase.domain.ObjectProperty;
import cn.darkjrong.hbase.factory.ObjectMappedFactory;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.util.ClassUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.ReflectUtil;
import cn.hutool.core.util.StrUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.util.Assert;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ObjectMappedFactoryTest {

    protected ObjectMappedFactory objectMappedFactory = new ObjectMappedFactory();

    @BeforeEach
    public void mappedStatements() {

        List<String> classNames = CollectionUtil.newArrayList();
        classNames.add(Student.class.getName());

        for (String className : classNames) {

            Class<?> clazz = ClassUtil.loadClass(className);

            ObjectMappedStatement objectMappedStatement = new ObjectMappedStatement();
            objectMappedStatement.setId(className);
            objectMappedStatement.setTableName(clazz.getAnnotation(TableName.class).value());
            objectMappedStatement.setTableNameBytes(Bytes.toBytes(objectMappedStatement.getTableName()));
            objectMappedStatement.setColumnFamily(clazz.getAnnotation(TableName.class).columnFamily());
            objectMappedStatement.setColumnFamilyBytes(Bytes.toBytes(objectMappedStatement.getColumnFamily()));
            Map<String, ObjectProperty> properties = parseProperties(clazz);
            ObjectProperty objectProperty = properties.values().stream().filter(a -> a.getField().isAnnotationPresent(TableId.class)).findAny().orElse(null);
            if (ObjectUtil.isEmpty(objectProperty)) {
                objectProperty = properties.get(HbaseConstant.ID);
            }
            Assert.notNull(objectProperty, HbaseExceptionEnum.getException(HbaseExceptionEnum.ID_NOT_FOUND, className));
            objectMappedStatement.setColumns(properties);
            objectMappedStatement.setTableId(objectProperty.getField());
            objectMappedFactory.addStatement(className, objectMappedStatement);
        }
    }

    /**
     * 解析属性
     *
     * @param clazz class
     * @return {@link Map}<{@link String}, {@link ObjectProperty}>
     */
    private Map<String, ObjectProperty> parseProperties(Class<?> clazz) {
        if (ObjectUtil.isNotNull(clazz)) {
            List<ObjectProperty> objectProperties = CollectionUtil.newArrayList(ReflectUtil.getFields(clazz))
                    .stream().map(a -> {
                        ObjectProperty objectProperty = new ObjectProperty();
                        objectProperty.setField(a);

                        String column = a.getName();
                        if (a.isAnnotationPresent(ColumnName.class)) {
                            column = a.getAnnotation(ColumnName.class).value();
                        }
                        objectProperty.setColumnBytes(Bytes.toBytes(column));
                        objectProperty.setColumn(column);
                        objectProperty.setType(a.getType());
                        return objectProperty;
                    }).collect(Collectors.toList());

            return objectProperties.stream().collect(Collectors.toMap(ObjectProperty::getColumn, a -> a, (a, b) -> a));
        }
        return Collections.emptyMap();
    }

    /**
     * 获取表名
     *
     * @param attributes 属性
     * @return {@link String}
     */
    private String getTableName(Map<String, Object> attributes) {
        String name = Convert.toStr(attributes.get("name"));
        if (StrUtil.isBlank(name)) {
            name = Convert.toStr(attributes.get("value"));
        }
        return name;
    }

    /**
     * 获取列族名
     *
     * @param attributes 属性
     * @return {@link String}
     */
    private String getColumnFamily(Map<String, Object> attributes) {
        return Convert.toStr(attributes.get("columnFamily"));
    }











}
