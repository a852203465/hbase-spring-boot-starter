package cn.darkjrong.hbase.common.exceptions;

import cn.hutool.core.exceptions.ExceptionUtil;
import cn.hutool.core.util.StrUtil;

import java.io.Serializable;

/**
 * hbase异常
 *
 * @author Rong.Jia
 * @date 2022/11/19
 */
public class HbaseException extends RuntimeException implements Serializable {

	private static final long serialVersionUID = -5711015629424998843L;

	public HbaseException(Throwable e) {
		super(ExceptionUtil.getMessage(e), e);
	}

	public HbaseException(String message) {
		super(message);
	}

	public HbaseException(String messageTemplate, Object... params) {
		super(StrUtil.format(messageTemplate, params));
	}

	public HbaseException(String message, Throwable throwable) {
		super(message, throwable);
	}

	public HbaseException(String message, Throwable throwable, boolean enableSuppression, boolean writableStackTrace) {
		super(message, throwable, enableSuppression, writableStackTrace);
	}

	public HbaseException(Throwable throwable, String messageTemplate, Object... params) {
		super(StrUtil.format(messageTemplate, params), throwable);
	}



}
