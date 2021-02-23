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
package de.jondoe.storm.bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseTickTupleAwareRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * @author mschmidt
 * @since 23-02-2021
 */
public class KafkaInspectorBolt extends BaseTickTupleAwareRichBolt
{
    private String componentId;
    private OutputCollector collector;

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
    {
        this.collector = collector;
        this.componentId = context.getThisComponentId() + "-" + context.getThisTaskId();
    }

    @Override
    public void process(Tuple input)
    {
        try
        {
            System.out.printf("%s: %s\n", componentId, input);
            String kafkaRawMessage = input.getStringByField("value");
            if (kafkaRawMessage == null || kafkaRawMessage.isEmpty() || !kafkaRawMessage.trim().startsWith("{"))
            {
                System.out.printf("Kafka message is not JSON formatted: %s\n", kafkaRawMessage);
                // throwing an exception here will make the message loop forever
                return;
            }
            this.collector.emit("analysis", input, new Values(kafkaRawMessage));
            this.collector.emit("main", input, new Values(kafkaRawMessage));
        }
        catch (Exception e)
        {
            e.printStackTrace();
            // show it in the UI: this.collector.reportError(e);
            this.collector.fail(input);
        }
        finally
        {
            this.collector.ack(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declareStream("analysis", new Fields("message"));
        declarer.declareStream("main", new Fields("message"));
    }
}
