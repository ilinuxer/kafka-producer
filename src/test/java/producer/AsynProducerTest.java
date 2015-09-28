package producer;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;
import ssy.kafka.common.UniqueKeyKit;
import ssy.kafka.message.MessageFormatter;
import ssy.kafka.producer.AsyncProducer;
import ssy.kafka.producer.KafkaProducer;
import ssy.kafka.producer.KafkaProducerBuilder;
import ssy.kafka.producer.SendType;

import java.util.ArrayList;
import java.util.List;

/**
 * author: huangqian
 * date  : 15/9/10
 * time  : 下午7:39
 */
@RunWith(BlockJUnit4ClassRunner.class)
public class AsynProducerTest {

    private KafkaProducer<String,String> asyncProducer = KafkaProducerBuilder.getBuilder()
            .setBrokerList("127.0.0.1:9092")
            .setSendType(SendType.ASYNC)
            .setBatchNumMessages(10)
            .build();


    @Test
    public void testAsyncProducer(){
        List<MessageFormatter> lst = new ArrayList<MessageFormatter>();
        for(int i = 0; i < 100; i ++){
            lst.add(new TestMessage());
        }
        asyncProducer.send("news_request_stat",lst);
        asyncProducer.close();
    }
    class TestMessage implements MessageFormatter {

        @Override
        public String format() {
            String json = "{ " +
                    "    \"app_id\":86367687," +
                    "    \"time\":1441959488845," +
                    "    \"uuid\":\"840b3d8e-8f27-4e70-8984-5b437323309a\"," +
                    "    \"news_id\":10001," +
                    "    \"dev_id\":\"qwertyuikjhg\"," +
                    "    \"version_code\":101," +
                    "    \"channel\":\"for you\"," +
                    "    \"tag\":[" +
                    "       \"for you\"," +
                    "       \"sport\"" +
                    "    ]," +
                    "    \"source\":\"CNN\"" +
                    "\n" +
                    "}";
            return json;
        }
    }

    @Test
    public void testCastTypeBuilder(){
        AsyncProducer producer = KafkaProducerBuilder.getBuilder()
                .setBrokerList("192.168.30.102:9092")
                .setSendType(SendType.ASYNC)
                .setBatchNumMessages(2)
                .build(AsyncProducer.class);
        List<MessageFormatter> lst = new ArrayList<MessageFormatter>();
        for(int i = 0; i < 10; i ++){
            lst.add(new TestMessage());
        }
        producer.send("news_request_stat",lst);
    }
}
