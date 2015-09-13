package ssy.kafka.common;

import java.util.UUID;

/**
 * author: huangqian
 * date  : 15/9/11
 * time  : 上午10:18
 */
public final class UniqueKeyKit {

    public static String newUUID(){
        UUID uuid = UUID.randomUUID();
        return uuid.toString();
    }

}
