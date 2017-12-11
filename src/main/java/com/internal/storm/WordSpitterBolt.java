package com.internal.storm;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordSpitterBolt implements IRichBolt {
  private static final long serialVersionUID = 1L;

  private OutputCollector collector;

  /*
   * (non-Javadoc)
   * 
   * @see backtype.storm.task.IBolt#prepare(java.util.Map,
   * backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
   * 
   * This method is similar to open() method in Spout-Program, it allows you to
   * initialize your code and get access to OutputCollector object for passing
   * output back to Storm
   */
  @Override public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
  }

  /*
   * (non-Javadoc)
   * 
   * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
   * 
   * This is the method where you implement business logic of your bolt, in this
   * case i am splitting the input line into words and passing them back to
   * Storm for further processing.
   */
  @Override public void execute(Tuple input) {
    String sentence = input.getString(0);
    String[] words = sentence.split(" ");
    for (String word : words) {
      word = word.trim();
      if (!word.isEmpty()) {
        word = word.toLowerCase();
        collector.emit(new Values(word));
      }
    }
    collector.ack(input);
  }

  /*
   * (non-Javadoc)
   * 
   * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.
   * topology .OutputFieldsDeclarer)
   * 
   * This method is similar to declareOutputFields() method in Spout-Program, it
   * declares that it is going to return word tuple for further processing
   */
  @Override public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word"));
  }

  @Override public void cleanup() {
  }

  @Override public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}