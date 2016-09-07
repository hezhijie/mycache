package net.mycache.core;

import java.lang.reflect.Method;

public interface ReadPolicy {
	Object invokeRead(Method method, Object[] args);
}
