package cn.darkjrong.hbase.enums;


import lombok.AllArgsConstructor;

/**
 * ID类型
 *
 * @author Rong.Jia
 * @date 2022/11/22
 */
@AllArgsConstructor
public enum IdType {

	/**
	 * 用户输入ID
	 * 该类型可以通过自己注册自动填充插件进行填充
	 */
	INPUT,

	/**
	 * 分配ID (主键类型为number或string）,
	 */
	ASSIGN_ID,

	/**
	 * 分配UUID (主键类型为 string)
	 */
	ASSIGN_UUID,

	/**
	 * 分配ObjectId
	 */
	ASSIGN_OBJECT_ID



	;

}
