package com.internal.storm;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class WordCounterBolt implements IRichBolt {
  private static final long serialVersionUID = 1L;

  Map<String, Integer> counters;
  private OutputCollector collector;

  @Override public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.counters = new HashMap<String, Integer>();
    this.collector = collector;
  }

  /*
   * Puts word frequency in the counter-map.
   */
  @Override public void execute(Tuple input) {
    String str = input.getString(0);
    if (!counters.containsKey(str)) {
      counters.put(str, 1);
    } else {
      Integer c = counters.get(str) + 1;
      counters.put(str, c);
    }
    collector.ack(input);
  }

  /*
   * This method would be called at the end of the topology and we are using it
   * to print all the words with their frequency
   */
  @Override public void cleanup() {
    for (Map.Entry<String, Integer> entry : counters.entrySet()) {
      System.out.println(entry.getKey() + " : " + entry.getValue());
    }
  }

  /*
   * This method is empty because we dont want to return any tuples for further
   * processing
   */
  @Override public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }

  @Override public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}