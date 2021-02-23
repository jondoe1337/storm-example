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

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;

/**
 * @author mschmidt
 * @since 23-02-2021
 */
public class LocalTopologyRunner
{
    public static void main(String[] args)
    {
        LocalCluster cluster = new LocalCluster();
        ExampleTopologyDefinition definition = new ExampleTopologyDefinition();
        cluster.submitTopology("MyStormExampleTopology", createConfig(), definition.createTopology());

        // If submit within a fat jar to a cluster run:
        // StormSubmitter.submitTopology(...)
    }

    private static Config createConfig()
    {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);
        conf.setMaxSpoutPending(10);
        conf.setMessageTimeoutSecs(60);
        conf.setNumWorkers(1);
        conf.setTopologyWorkerMaxHeapSize(1024);
        return conf;
    }
}
