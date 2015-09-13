package ssy.kafka.producer;

/**
 * author: huangqian
 * date  : 15/9/10
 * time  : 下午4:18
 */
public interface ProducerConstants {

    /***
     * 用来控制一个produce请求怎样才能算完成，准确的说，是有多少broker必须已经提交数据到log文件，并向leader发送ack，可以设置如下的值：
     * 0：意味着producer永远不会等待一个来自broker的ack，这就是0.7版本的行为。这个选项提供了最低的延迟，
     *   但是持久化的保证是最弱的，当server挂掉的时候会丢失一些数据。
     * 1：意味着在leader replica已经接收到数据后，producer会得到一个ack。这个选项提供了更好的持久性，
     *   因为在server确认请求成功处理后，client才会返回。如果刚写到leader上，还没来得及复制leader就挂了，那么消息才可能会丢失。
     * -1：意味着在所有的ISR都接收到数据后，producer才得到一个ack。这个选项提供了最好的持久性，只要还有一个replica存活，那么数据就不会丢失。
     */
    public static final String REQUEST_REQUIRED_ACKS        = "0";

    /***
     * 请求超时时间
     */
    public static final String REQUEST_TIMEOUT_MS           = "10000";


    /***
     * 决定消息是否应在一个后台线程异步发送。合法的值为sync，表示异步发送；sync表示同步发送。
     * 设置为async则允许批量发送请求，这回带来更高的吞吐量，但是client的机器挂了的话会丢失还没有发送的数据。
     * 异步意味着消息将会在本地buffer，并适时批量发送，推荐使用。
     */
    public interface  ProducerType{
        /***
         * 同步发送方式
         * 决定消息是否应在一个后台线程异步发送。合法的值为sync；sync表示同步发送。
         */
        public static final String SYNC           = "sync";

        /***
         * 异步发送方式
         * 设置为async则允许批量发送请求，这回带来更高的吞吐量，但是client的机器挂了的话会丢失还没有发送的数据。
         * 异步意味着消息将会在本地buffer，并适时批量发送，推荐使用。
         */
        public static final String ASYNC          = "async";
    }


    /***
     * The serializer class for messages. The default encoder takes a byte[] and returns the same byte[].
     *消息的序列化类，默认是的encoder处理一个byte[]，返回一个byte[]。
     */
    public static final String SERIALIZER_CLASS_DEFAULT     = "kafka.serializer.DefaultEncoder";

    /***
     * 用来把消息分到各个partition中，默认行为是对key进行hash。
     */
    public static final String PARTITIONER_CLASS_DEFAULT    = "kafka.producer.DefaultPartitioner";


    /***
     * 消息是否压缩
     */
    public interface CompressionCodec{

        public static final String NONE         = "none";           //不压缩。
        public static final String GZIP         = "gizp";           //gzip压缩
        public static final String SNAPPY       = "snappy";         //snappy压缩

    }


    /***
     * 消息发送最大重连次数
     * 如果producer发送消息失败了会自动重发，本选项指定了重发的次数。
     * 注意如果是非0值，那么可能会导致重复发送，就是说的确发送了消息，但是没有收到ack，那么就还会发一次。
     */
    public static final String MESSAGE_SEND_MAX_RETRIES_10    = "10";   //默认重连10次


    /***
     * 在每次重发之前，producer会刷新相关的topic的元数据，来看看是否选出了一个新leader。
     * 由于选举leader会花一些时间，此选项指定了在刷新元数据前等待的时间。
     */
    public static final String RETRY_BACKOFF_MS_100             = "100";


    /***
     * 当出现错误时(缺失partition，leader不可用等)，producer通常会从broker拉取最新的topic的元数据。
     * 它也会每隔一段时间轮询(默认是每隔10分钟)。如果设置了一个负数，那么只有当发生错误时才会刷新元数据，当然不推荐这样做。
     * 有一个重要的提示：只有在消息被发送后才会刷新，所以如果producer没有发送一个消息的话，则元数据永远不会被刷新。
     */
    public static final long TOPIC_METADATA_REFRESH_INTERVAL_MS = 600 * 1000;

    /***
     * 当使用异步模式时，缓冲数据的最大时间。例如设为100的话，会每隔100毫秒把所有的消息批量发送。
     * 这会提高吞吐量，但是会增加消息的到达延时。
     */
    public static final long QUEUE_BUFFERING_MAX_MS_5000         = 5000;


    /***
     * 在异步模式下，producer端允许buffer的最大消息数量，如果producer无法尽快将消息发送给broker，
     * 从而导致消息在producer端大量沉积，如果消息的条数达到此配置值，将会导致producer端阻塞或者消息被抛弃。
     */
    public static final int  QUEUE_BUFFERING_MAX_MESSAGES_100    = 100;

    /***
     * 当消息在producer端沉积的条数达到queue.buffering.max.meesages时，阻塞一定时间后，队列仍然没有enqueue(producer仍然没有发送出任何消息) 。
     * 此时producer可以继续阻塞或者将消息抛弃，此timeout值用于控制阻塞的时间，如果值为-1则无阻塞超时限制，消息不会被抛弃；
     * 如果值为0则立即清空队列，消息被抛弃。
     */
    public static final long QUEUE_ENQUEUE_TIMEOUT_MS           = -1;

    /***
     * 在异步模式下，一个batch发送的消息数量。producer会等待直到要发送的消息数量达到这个值，之后才会发送。
     * 但如果消息数量不够，达到queue.buffer.max.ms时也会直接发送。
     */
    public static final int BATCH_NUM_MESSAGES_100             = 100;

    /***
     * socket的发送缓存大小。
     */
    public static final String SEND_BUFFER_BYTES_100K           = "102400";     //100k



}
