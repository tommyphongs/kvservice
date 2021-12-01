package org.ptm.kvservice.utils;

@FunctionalInterface
public interface FunctionWithException<T, U> {

    U apply(T t ) throws Exception;

}
