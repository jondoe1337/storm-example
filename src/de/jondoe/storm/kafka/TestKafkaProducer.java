/*
 * Copyright (c) 2018 Anyblock Analytics GmbH
 * This file is part of the eth.events storm-topology.
 *
 * The eth.events storm-topology is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * The eth.events storm-topology is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with the eth.events storm-topology. If not, see <http://www.gnu.org/licenses/>.
 */
package de.jondoe.storm.kafka;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Testclass to send messages using Kafka
 *
 * @author Max Schmidt <max@eth.events>
 * @since 23-02-2021
 */
public class TestKafkaProducer
{
    public static void main(String[] args) throws Exception
    {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
                BufferedReader in = new BufferedReader(new InputStreamReader(System.in)))
        {
            String msg = null;
            while ((msg = in.readLine()) != null && msg.length() != 0)
            {
                Future<RecordMetadata> result = producer.send(new ProducerRecord<String, String>("storm.exampletopic", null, msg), null);
                result.get();
                System.out.println("Sent");
            }
        }
    }
}
