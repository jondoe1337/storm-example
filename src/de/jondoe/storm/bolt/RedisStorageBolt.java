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

import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import redis.clients.jedis.JedisCommands;

/**
 * @author mschmidt
 * @since 23-02-2021
 */
public class RedisStorageBolt extends AbstractRedisBolt
{
    public RedisStorageBolt(JedisPoolConfig config)
    {
        super(config);
    }

    @Override
    protected void process(Tuple tuple)
    {
        JedisCommands instance = getInstance();
        try
        {
            String s = tuple.getStringByField("message");
            instance.set(String.valueOf(s.hashCode()), s);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            this.collector.fail(tuple);
        }
        finally
        {
            this.collector.ack(tuple);
            if (instance != null)
            {
                returnInstance(instance);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        /* noop */
    }
}
