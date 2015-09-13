package ssy.kafka.producer;

import org.apache.commons.lang3.StringUtils;
import ssy.kafka.common.Toolkit;

/**
 * author: huangqian
 * date  : 15/9/10
 * time  : 下午6:00
 */
public class KafkaProducerBuilder implements ProducerConstants{

    private int queueBufferingMaxMessages   = QUEUE_BUFFERING_MAX_MESSAGES_100; //异步模式下，producer端允许buffer缓存的消息最大消息数量。
    private long queueBufferingMaxMs        = QUEUE_BUFFERING_MAX_MS_5000;      //异步模式下，缓冲数据的最大时间。
    private long queueEnqueueTimeoutMs      = QUEUE_ENQUEUE_TIMEOUT_MS;         //异步模式下，控制producer沉积阻塞超时时间。
    private int batchNumMessages            = BATCH_NUM_MESSAGES_100;           //异步模式下，每个batch发送的消息数量
    private String brokerList               = null;                             //broker集群访问地址。
    private SendType sendType               = SendType.SYNC;                    //默认为同步方式

    private KafkaProducerBuilder(){

    }

    public static KafkaProducerBuilder getBuilder(){
        return new KafkaProducerBuilder();
    }

    /***
     * 设置异步模式下，producer端允许buffer缓存的消息最大消息数量。
     * 如果消息条数未达到此配置值，producer端将会阻塞。直到达到阀值才会发送。
     * 默认值：queueBufferingMaxMessages＝5000
     * @param queueBufferingMaxMessages producer端允许buffer缓存的消息最大消息数量
     */
    public KafkaProducerBuilder setQueueBufferingMaxMessages(int queueBufferingMaxMessages) {
        this.queueBufferingMaxMessages = queueBufferingMaxMessages;
        return this;
    }

    /***
     * 设置异步模式下，缓冲数据的最大时间。
     * 如果设置queueBufferingMaxMs=100，会每隔100ms把所有的消息批量发送。
     * 这会提高吞吐量，但是会增加消息的到达延时。
     * 默认值：queueBufferingMaxMs=100
     * @param queueBufferingMaxMs 缓冲数据的最大时间（单位：ms）
     */
    public KafkaProducerBuilder setQueueBufferingMaxMs(long queueBufferingMaxMs) {
        this.queueBufferingMaxMs = queueBufferingMaxMs;
        return this;
    }

    /***
     * 设置阻塞超时时间。（建议为默认值－1）
     * 当消息在producer端沉积的条数达到queue.buffering.max.meesages时，
     * 阻塞一定时间后，队列仍然没有enqueue(producer仍然没有发送出任何消息) 。
     * 此时producer可以继续阻塞或者将消息抛弃，此timeout值用于控制阻塞的时间，如果值为-1则无阻塞超时限制，消息不会被抛弃；
     * 如果值为0则立即清空队列，消息被抛弃。
     * 默认值：queueEnqueueTimeoutMs ＝ －1
     * @param queueEnqueueTimeoutMs producer端沉积阻塞超时时间。
     */
    public KafkaProducerBuilder setQueueEnqueueTimeoutMs(long queueEnqueueTimeoutMs) {
        this.queueEnqueueTimeoutMs = queueEnqueueTimeoutMs;
        return this;
    }

    /***
     * 设置异步模式下，一个batch发送的消息数量。
     * producer会等待直到要发送的消息数量达到这个值，之后才会发送。
     * 但如果消息数量不够，达到queue.buffer.max.ms时也会直接发送。
     * 默认值：100
     * @param batchNumMessages 一个batch发送的消息数量
     */
    public KafkaProducerBuilder setBatchNumMessages(int batchNumMessages) {
        this.batchNumMessages = batchNumMessages;
        return this;
    }

    /***
     * 设置Kafka的Broker集群访问地址。必须设置。
     * @param brokerList Kafka的Broker集群访问地址。
     */
    public KafkaProducerBuilder setBrokerList(String brokerList) {
        this.brokerList = brokerList;
        return this;
    }

    /***
     * 设置发送模式
     * @param sendType 发送模式。同步/异步两种
     */
    public KafkaProducerBuilder setSendType(SendType sendType) {
        this.sendType = sendType;
        return this;
    }

    /***
     * 构建一个KafkaProducer对象。
     * @return 返回一个KafkaProducer子类对象
     */
    public  KafkaProducer<String,String> build(){
        if(StringUtils.isBlank(brokerList)){
            throw new IllegalArgumentException("brokerList can't empty, please set brokerList");
        }
        switch (sendType){
            case ASYNC:
                return AsyncProducer.newInstanceAndOpen(brokerList,queueBufferingMaxMessages,queueBufferingMaxMs,
                        queueEnqueueTimeoutMs,batchNumMessages);
            case SYNC:
                return SyncProducer.newInstanceAndOpen(brokerList);
            default:
                return SyncProducer.newInstanceAndOpen(brokerList);

        }
    }


    public <T extends KafkaProducer<String,String>> T build( Class<T> type){
        KafkaProducer<String,String> kafkaProducer = build();
        if(Toolkit.isNotNull(kafkaProducer) && Toolkit.isNotNull(type) && type.isInstance(kafkaProducer)){
            return (T) kafkaProducer;
        }
        throw new RuntimeException("can not cast to "+type+", please you check type ");
    }


}
