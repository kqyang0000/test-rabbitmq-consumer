package com.imocc;

import com.rabbitmq.client.*;

import java.io.IOException;

/**
 * Created by kqyang on 2019/6/7.
 */
public class Consumer02_subscribe_sms {
    private static final String QUEUE_INFORM_SMS = "queue_inform_sms";
    private static final String EXCHANGE_FANOUT_INFORM = "exchange_fanout_inform";

    public static void main(String[] args) {
        // 通过连接工厂创建新的连接
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");
        // 设置虚拟机 一个mq服务可以设置多个虚拟机 每个虚拟机相当于一个独立的mq
        factory.setVirtualHost("/");
        // 建立连接
        Connection connection;

        try {
            // 创建会话通道，生产者和mq服务的所有通信都在channel通道中完成
            connection = factory.newConnection();
            Channel channel = connection.createChannel();
            // 声明队列 如果队列在mq中没有则要创建
            /**
             * String queue:队列名称
             * boolean durable:是否持久化 如果持久化，mq重启后队列还在
             * boolean exclusive:是否独占 如果connect连接关闭队列则自动删除，如果设置成true可用于临时队列的创建
             * boolean autoDelete:自动删除 队列不再使用时是否自动删除此队列，如果设置成true就可以实现临时队列（队列不用了就自动删除）
             * Map<String, Object> arguments:参数 可以设置一个队列的扩展参数，比如可以设置存活时间
             */
            channel.queueDeclare(QUEUE_INFORM_SMS, true, false, false, null);
            channel.exchangeDeclare(EXCHANGE_FANOUT_INFORM, BuiltinExchangeType.FANOUT);
            // 绑定交换机
            channel.queueBind(QUEUE_INFORM_SMS, EXCHANGE_FANOUT_INFORM, "");
            // 实现消费方法
            DefaultConsumer defaultConsumer = new DefaultConsumer(channel) {


                /**
                 *
                 * @param consumerTag 消费者标签 用来标识消费者
                 * @param envelope 信封
                 * @param properties 消息属性
                 * @param body 消息内容
                 * @throws IOException
                 */
                // 当接收到消息以后，此方法将被调用
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    // 交换机
                    /**
                     * 六种模式
                     * 1.work queues-工作模式
                     * ***** 特点：两个消费者消费同一个队列中的消息，消费者监听同一个队列的消息，消费不能被重复消费
                     *            rabbitmq会采用轮询的方法将消息平均发送给消费者
                     * 2.publish/subscribe-发布模式
                     * ***** 特点：一个生产者将消息发送给交换机，交换机绑定多个队列，每个消费者监听自己的队列，生产
                     *            者将消息发送给交换机，由交换机将消息转发到绑定此交换机的每个队列，每个绑定交换机
                     *            的队列将接收到消息，如果消息发送到没有绑定队列的交换机上消息将丢失
                     * 3.routing-路由模式
                     * ***** 特点：一个交换机绑定多个队列，每个队列设置routingkey，并且一个队列可以设置多个routingkey，
                     *            每个消费者监听自己的队列，生产者将消息发给交换机，发送消息时需要指定routingkey
                     *            的值，交换机来判断该routingkey的值和哪个队列的routingkey相等，如果相等则将消
                     *            息转发给队列。
                     * 4.topics-通配符模式
                     * 5.header-转发器模式
                     * 6.RPC-远程过程调用模式
                     */
                    String exchange = envelope.getExchange();
                    // 消息id，mq在channel用用来标识消息的id，可用于确认消息已接收
                    long deliveryTag = envelope.getDeliveryTag();
                    String message = new String(body, "UTF-8");
                    System.out.println("receive sms message:" + message);
                }
            };

            // 监听队列
            /**
             * String queue:队列名称
             * final boolean autoAck:自动回复，当消费者接收到消息后要告诉mq消息已接收，如果将此参数设置为true，表示会自动回复mq,如果设置为false则要通过编程实现回复
             * final Consumer callback:消费方法，当消费者接收到消息要执行的方法
             */
            // 参数：String queue, final boolean autoAck, final Consumer callback
            channel.basicConsume(QUEUE_INFORM_SMS, true, defaultConsumer);
        } catch (Exception e) {

        } finally {

        }
    }
}
