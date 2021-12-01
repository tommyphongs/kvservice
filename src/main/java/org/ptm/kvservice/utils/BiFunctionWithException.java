package org.ptm.kvservice.utils;

@FunctionalInterface
public interface BiFunctionWithException<T, U, K> {

    K apply(T t, U u) throws Exception;

}
