/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.spring.autoconfigure;

import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.MQAdmin;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.SelectorType;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.support.RocketMQMessageConverter;
import org.apache.rocketmq.spring.support.RocketMQUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.AnyNestedCondition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;

@Configuration
@EnableConfigurationProperties(RocketMQProperties.class)
@ConditionalOnClass({MQAdmin.class})
@ConditionalOnProperty(prefix = "rocketmq", value = "name-server", matchIfMissing = true)
@Import({MessageConverterConfiguration.class, ListenerContainerConfiguration.class, ExtProducerResetConfiguration.class, ExtConsumerResetConfiguration.class, RocketMQTransactionConfiguration.class})
@AutoConfigureAfter({MessageConverterConfiguration.class})
@AutoConfigureBefore({RocketMQTransactionConfiguration.class})

public class RocketMQAutoConfiguration implements ApplicationContextAware {

    /*
     * @Import 是 Spring 框架中的一种注解，用于向 Spring 容器中动态地导入其他的配置类或 Bean。<br/>
     *
     * @AutoConfigureAfter 和 @AutoConfigureBefore 是 Spring Boot 中的两个注解，用于控制自动配置类的加载顺序。<br/>
     * @AutoConfigureAfter 注解用于指定当前自动配置类需要在哪些自动配置类之后进行加载；@AutoConfigureBefore` 注解用于指定当前自动配置类需要在哪些自动配置类之前进行加载。<br/>
     *
     * @ConditionalOnClass 是 Spring 框架中的一种条件注解，用于根据类路径中是否存在指定的类来决定是否创建当前 Bean。（其中，`value` 属性用于指定要检查的类；`name` 属性用于指定类的完整名称（覆盖 `value` 属性）。如果指定了多个属性，则满足其中任意一个即可。）<br/>
     * @ConditionalOnBean 是 Spring 框架中的一种条件注解，用于根据容器中是否存在指定类型的 Bean 来决定是否创建当前 Bean。（其中，`value` 属性用于指定要检查的 Bean 类型；`type` 属性用于指定 Bean 的完整名称（覆盖 `value` 属性）；`annotation` 属性用于指定 Bean 上必须存在的注解。如果指定了多个属性，则满足其中任意一个即可。）<br/>
     * @ConditionalOnMissingBean 是 Spring 框架中的一种条件注解，用于指定一个 Bean 只有在容器中不存在时才会被创建。如果容器中已经存在该类型的 Bean，则该注解所标注的 Bean 不会被创建。<br/>
     * （`value` 属性用于指定要检查的 Bean 类型；`type` 属性用于指定 Bean 的完整名称（覆盖 `value` 属性）；`annotation` 属性用于指定 Bean 上必须存在的注解。如果指定了多个属性，则满足其中任意一个即可。
     * 通过使用 `@ConditionalOnMissingBean` 注解，可以很好地控制 Bean 的创建与注入，确保应用能够正确地启动。如果容器中不存在指定类型的 Bean，则会创建当前 Bean；否则跳过创建。）<br/>
     *
     * @ConditionalOnProperty` 是 Spring 框架中的一种条件注解，用于根据配置文件中的属性值来判断是否需要创建 Bean。<br/>
     * （`value` 属性用于指定要检查的属性名；`prefix` 属性用于指定属性的前缀；`name` 属性用于指定属性的完整名称（覆盖 `value` 和 `prefix` 属性）；`havingValue` 属性用于指定属性的期望值；`matchIfMissing` 属性用于指定当属性不存在时，是否匹配成功。）<br/>
     *
     * 这些注解的作用是在 Spring 启动时根据实际情况进行 Bean 的创建与注入，可以很好地控制 Bean 的初始化顺序和依赖关系。在 Spring Boot 等基于 Spring 的应用中，这些注解也被广泛地应用于自动装配和自定义配置的实现中。
     */

    private static final Logger log = LoggerFactory.getLogger(RocketMQAutoConfiguration.class);

    public static final String ROCKETMQ_TEMPLATE_DEFAULT_GLOBAL_NAME =
        "rocketMQTemplate";
    public static final String PRODUCER_BEAN_NAME = "defaultMQProducer";
    public static final String CONSUMER_BEAN_NAME = "defaultLitePullConsumer";

    @Autowired
    private Environment environment;

    private ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @PostConstruct
    public void checkProperties() {
        String nameServer = environment.getProperty("rocketmq.name-server", String.class);
        log.debug("rocketmq.nameServer = {}", nameServer);
        if (nameServer == null) {
            log.warn("The necessary spring property 'rocketmq.name-server' is not defined, all rockertmq beans creation are skipped!");
        }
    }

    @Bean(PRODUCER_BEAN_NAME)
    @ConditionalOnMissingBean(DefaultMQProducer.class)
    @ConditionalOnProperty(prefix = "rocketmq", value = {"name-server", "producer.group"})
    public DefaultMQProducer defaultMQProducer(RocketMQProperties rocketMQProperties) {
        RocketMQProperties.Producer producerConfig = rocketMQProperties.getProducer();
        String nameServer = rocketMQProperties.getNameServer();
        String groupName = producerConfig.getGroup();
        Assert.hasText(nameServer, "[rocketmq.name-server] must not be null");
        Assert.hasText(groupName, "[rocketmq.producer.group] must not be null");

        String accessChannel = rocketMQProperties.getAccessChannel();

        String ak = rocketMQProperties.getProducer().getAccessKey();
        String sk = rocketMQProperties.getProducer().getSecretKey();
        boolean isEnableMsgTrace = rocketMQProperties.getProducer().isEnableMsgTrace();
        String customizedTraceTopic = rocketMQProperties.getProducer().getCustomizedTraceTopic();

        DefaultMQProducer producer = RocketMQUtil.createDefaultMQProducer(groupName, ak, sk, isEnableMsgTrace, customizedTraceTopic);

        producer.setNamesrvAddr(nameServer);
        if (!StringUtils.isEmpty(accessChannel)) {
            producer.setAccessChannel(AccessChannel.valueOf(accessChannel));
        }
        producer.setSendMsgTimeout(producerConfig.getSendMessageTimeout());
        producer.setRetryTimesWhenSendFailed(producerConfig.getRetryTimesWhenSendFailed());
        producer.setRetryTimesWhenSendAsyncFailed(producerConfig.getRetryTimesWhenSendAsyncFailed());
        producer.setMaxMessageSize(producerConfig.getMaxMessageSize());
        producer.setCompressMsgBodyOverHowmuch(producerConfig.getCompressMessageBodyThreshold());
        producer.setRetryAnotherBrokerWhenNotStoreOK(producerConfig.isRetryNextServer());

        return producer;
    }

    @Bean(CONSUMER_BEAN_NAME)
    @ConditionalOnMissingBean(DefaultLitePullConsumer.class)
    @ConditionalOnProperty(prefix = "rocketmq", value = {"name-server", "consumer.group", "consumer.topic"})
    public DefaultLitePullConsumer defaultLitePullConsumer(RocketMQProperties rocketMQProperties)
            throws MQClientException {
        RocketMQProperties.Consumer consumerConfig = rocketMQProperties.getConsumer();
        String nameServer = rocketMQProperties.getNameServer();
        String groupName = consumerConfig.getGroup();
        String topicName = consumerConfig.getTopic();
        Assert.hasText(nameServer, "[rocketmq.name-server] must not be null");
        Assert.hasText(groupName, "[rocketmq.consumer.group] must not be null");
        Assert.hasText(topicName, "[rocketmq.consumer.topic] must not be null");

        String accessChannel = rocketMQProperties.getAccessChannel();
        MessageModel messageModel = MessageModel.valueOf(consumerConfig.getMessageModel());
        SelectorType selectorType = SelectorType.valueOf(consumerConfig.getSelectorType());
        String selectorExpression = consumerConfig.getSelectorExpression();
        String ak = consumerConfig.getAccessKey();
        String sk = consumerConfig.getSecretKey();
        int pullBatchSize = consumerConfig.getPullBatchSize();

        DefaultLitePullConsumer litePullConsumer = RocketMQUtil.createDefaultLitePullConsumer(nameServer, accessChannel,
                groupName, topicName, messageModel, selectorType, selectorExpression, ak, sk, pullBatchSize);
        litePullConsumer.setEnableMsgTrace(consumerConfig.isEnableMsgTrace());
        litePullConsumer.setCustomizedTraceTopic(consumerConfig.getCustomizedTraceTopic());
        return litePullConsumer;
    }

    @Bean(destroyMethod = "destroy")
    @Conditional(ProducerOrConsumerPropertyCondition.class)
    @ConditionalOnMissingBean(name = ROCKETMQ_TEMPLATE_DEFAULT_GLOBAL_NAME)
    public RocketMQTemplate rocketMQTemplate(RocketMQMessageConverter rocketMQMessageConverter) {
        RocketMQTemplate rocketMQTemplate = new RocketMQTemplate();
        if (applicationContext.containsBean(PRODUCER_BEAN_NAME)) {
            rocketMQTemplate.setProducer((DefaultMQProducer) applicationContext.getBean(PRODUCER_BEAN_NAME));
        }
        if (applicationContext.containsBean(CONSUMER_BEAN_NAME)) {
            rocketMQTemplate.setConsumer((DefaultLitePullConsumer) applicationContext.getBean(CONSUMER_BEAN_NAME));
        }
        rocketMQTemplate.setMessageConverter(rocketMQMessageConverter.getMessageConverter());
        return rocketMQTemplate;
    }

    static class ProducerOrConsumerPropertyCondition extends AnyNestedCondition {

        public ProducerOrConsumerPropertyCondition() {
            super(ConfigurationPhase.REGISTER_BEAN);
        }

        @ConditionalOnBean(DefaultMQProducer.class)
        static class DefaultMQProducerExistsCondition {
        }

        @ConditionalOnBean(DefaultLitePullConsumer.class)
        static class DefaultLitePullConsumerExistsCondition {
        }
    }
}
