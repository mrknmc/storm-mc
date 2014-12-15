package backtype.storm.generated;

import backtype.storm.topology.IRichSpout;

import java.util.Map;

public class SpoutSpec implements java.io.Serializable, Cloneable {

  private final IRichSpout spoutObject;
  private final ComponentCommon common;

  public IRichSpout getSpoutObject() {
    return spoutObject;
  }

  public ComponentCommon getCommon() {
    return common;
  }

  public SpoutSpec(IRichSpout spoutObject, ComponentCommon common) {
    this.spoutObject = spoutObject;
    this.common = common;
  }

}

