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
import org.apache.storm.tuple.Tuple;

/**
 * @author mschmidt
 * @since 23-02-2021
 */
public class MessageAnalysisBolt extends BaseTickTupleAwareRichBolt
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
        String message = input.getStringByField("message");
        System.out.printf("%s: %s\n", componentId, message);
        if (message.matches(".*forbidden.*"))
        {
            System.out.printf("Forbidden message detected: %s\n", message);
            this.collector.fail(input);
        }
        else
        {
            this.collector.ack(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        /* noop */
    }
}
