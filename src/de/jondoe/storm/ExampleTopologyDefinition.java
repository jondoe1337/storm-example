/*
 * Copyright (c) [2021] Anyblock Analytics GmbH
 * This file is part of the eth.events data.
 *
 * The eth.events data is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * The eth.events data is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with the eth.events data. If not, see <http://www.gnu.org/licenses/>.
 */
/**
 *
 */
package de.jondoe.storm;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig.Builder;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import de.jondoe.storm.bolt.KafkaInspectorBolt;
import de.jondoe.storm.bolt.MessageAnalysisBolt;
import de.jondoe.storm.bolt.RedisStorageBolt;
import de.jondoe.storm.bolt.TimestampEnricherBolt;

/**
 * @author mschmidt
 * @since 23-02-2021
 */
public class ExampleTopologyDefinition
{
    private TopologyBuilder topologyBuilder = new TopologyBuilder();

    public StormTopology createTopology()
    {
        addKafkaSpout();

        addKafkaInspectorBolt();

        addMessageAnalysisBolt();

        addTimestampEnricherBolt();

        addRedisStorageBolt();

        return topologyBuilder.createTopology();
    }

    private void addKafkaSpout()
    {
        Builder<Object, Object> builder = new Builder<>("localhost:9092", "storm.exampletopic");
        builder.setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        builder.setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        builder.setRecordTranslator((r) -> new Values(r.topic(), r.partition(), r.offset(), r.key(), r.value()),
                                    new Fields("topic", "partition", "offset", "key", "value"));
        builder.setProp(ConsumerConfig.GROUP_ID_CONFIG, "storm-example-consumer");
        topologyBuilder.setSpout("kafka-spout", new KafkaSpout<>(builder.build()), 1);
    }

    private void addKafkaInspectorBolt()
    {
        topologyBuilder.setBolt("kafka-inspector-bolt", new KafkaInspectorBolt(), 2).shuffleGrouping("kafka-spout");
    }

    private void addMessageAnalysisBolt()
    {
        topologyBuilder.setBolt("message-analysis-bolt", new MessageAnalysisBolt(), 1).shuffleGrouping("kafka-inspector-bolt", "analysis");
    }

    private void addTimestampEnricherBolt()
    {
        topologyBuilder.setBolt("timestamp-enricher-bolt", new TimestampEnricherBolt(), 1).shuffleGrouping("kafka-inspector-bolt", "main");
    }

    private void addRedisStorageBolt()
    {
        JedisPoolConfig config = new JedisPoolConfig.Builder().setHost("localhost").setPort(6379).build();
        topologyBuilder.setBolt("redis-storage-bolt", new RedisStorageBolt(config), 1).shuffleGrouping("timestamp-enricher-bolt");
    }
}
