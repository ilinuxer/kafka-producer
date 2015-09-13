package ssy.kafka.message;

import java.io.Serializable;

/**
 * author: huangqian
 * date  : 15/9/10
 * time  : 上午10:33
 */
public interface MessageFormatter extends Serializable {


    /***
     * 格式话消息。
     * 对于所有使用kafka-producer发送的消息，必须实现该接口
     * @return 返回规定格式的消息字符串。
     */
    public String format();
}
