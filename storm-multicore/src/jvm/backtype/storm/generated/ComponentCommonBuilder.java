package backtype.storm.generated;


import java.awt.*;
import java.util.HashMap;
import java.util.Map;

public class ComponentCommonBuilder implements java.io.Serializable {
  private Map<GlobalStreamId, Grouping> inputs = new HashMap<GlobalStreamId, Grouping>();
  private Map<String, StreamInfo> streams = new HashMap<String, StreamInfo>();
  private Number parallelismHint;
  private Map<String, Object> conf = new HashMap<String, Object>();

  public Map<GlobalStreamId, Grouping> getInputs() {
    return inputs;
  }

  public void setInputs(Map<GlobalStreamId, Grouping> inputs) {
    this.inputs = inputs;
  }

  public Map<String, StreamInfo> getStreams() {
    return streams;
  }

  public void setStreams(Map<String, StreamInfo> streams) {
    this.streams = streams;
  }

  public Number getParallelismHint() {
    return parallelismHint;
  }

  public void setParallelismHint(Number parallelismHint) {
    this.parallelismHint = parallelismHint;
  }

  public Map<String, Object> getConf() {
    return conf;
  }

  public void setConf(Map<String, Object> conf) {
    this.conf = conf;
  }

  public void putToConf(Map<String, Object> newConf) {
    if (newConf != null) {
      this.conf.putAll(newConf);
    }
  }

  public void putToInputs(GlobalStreamId key, Grouping val) {
    this.inputs.put(key, val);
  }

  public ComponentCommon build() {
    return new ComponentCommon(inputs, streams, parallelismHint, conf);
  }

}
