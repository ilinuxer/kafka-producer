package ssy.kafka.common;

import java.util.List;
import java.util.Map;

/**
 * author: huangqian
 * date  : 15/9/10
 * time  : 上午10:07
 */
public final class Toolkit {

    public static <T> void ifNull(T t,String errorMessage){
        if(t == null){
            throw new NullPointerException(errorMessage);
        }
    }

    public static <T> boolean isNull(T t){
        return t == null;
    }

    public static <T> boolean isNotNull(T t){
        return !isNull(t);
    }

    public static <T> boolean isNotEmpty(List<T> list){
        return !isEmpty(list);
    }

    public static <K,V> boolean isNotEmpty(Map<K,V> map){
        return !isEmpty(map);
    }

    public static <K,V> boolean isEmpty(Map<K,V> map){
        return isNull(map) || map.isEmpty();
    }

    public static <T> boolean isEmpty(List<T> list){
        return isNull(list) || list.isEmpty();
    }

    public static <T> boolean isEmpty(T[] array){
        return isNull(array) || array.length <= 0;
    }

    public static <T> boolean isNotEmpty(T[] array){
        return !isEmpty(array);
    }

}
