package com.xy.rocketmq.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.CountDownLatch2;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.TimeUnit;

public class TestProducer {

    public static void main(String[] args) {

    }

    /**
     * 同步发送消息
     */
    private static void syncProducer() throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("test_group");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();
        // 设置异步发送失败重试次数
        producer.setRetryTimesWhenSendAsyncFailed(1);

        int messageCount = 100;
        for (int i = 0; i < messageCount; i++) {
            final int index = i;
            Message message = new Message("TestTopic", "TagA", "OrderId188", "hellow world".getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult sendResult = producer.send(message);
            System.out.println("[sync] Send callback return:" + sendResult);
        }
        producer.shutdown();
    }

    /**
     * 异步发送消息
     */

    private static void asyncProducer() throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("test_group");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();
        // 设置异步发送失败重试次数
        producer.setRetryTimesWhenSendAsyncFailed(1);

        int messageCount = 100;
        final CountDownLatch2 countDownLatch2 = new CountDownLatch2(messageCount);
        for (int i = 0; i < messageCount; i++) {
            final int index = i;
            Message message = new Message("TestTopic", "TagA", "OrderId188", "hellow world".getBytes(RemotingHelper.DEFAULT_CHARSET));
            producer.send(message, new SendCallback() {

                @Override
                public void onSuccess(SendResult sendResult) {
                    countDownLatch2.countDown();
                    System.out.printf("%-10d OK %s %n", index, sendResult.getMsgId());
                }

                @Override
                public void onException(Throwable e) {
                    countDownLatch2.countDown();
                    System.out.printf("%-10d Exception %s %n", index, e);
                    e.printStackTrace();
                }
            });
        }
        countDownLatch2.await(5, TimeUnit.SECONDS);
        producer.shutdown();
    }

    /**
     * 单向发送消息
     */
    private static void oneWayProducer() throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("test_group");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();

        int messageCount = 100;
        for (int i = 0; i < messageCount; i++) {
            final int index = i;
            Message message = new Message("TestTopic", "TagA", "OrderId188", "hellow world".getBytes(RemotingHelper.DEFAULT_CHARSET));
            producer.sendOneway(message);
        }
        producer.shutdown();
    }

}
