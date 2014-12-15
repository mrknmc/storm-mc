package backtype.storm.generated;

import backtype.storm.Config;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;

public class StormTopology {

  private final ImmutableMap<String, SpoutSpec> spouts;
  private final ImmutableMap<String, Bolt> bolts;

  public Map<String, SpoutSpec> getSpouts() {
    return spouts;
  }

  public Map<String, Bolt> getBolts() {
    return bolts;
  }

  public StormTopology(Map<String, SpoutSpec> spouts, Map<String, Bolt> bolts) {
    this.spouts = ImmutableMap.copyOf(spouts);
    this.bolts = ImmutableMap.copyOf(bolts);
  }

  /**
   *
   * @param topology
   * @param conf
   * @return
   */
  public StormTopology normalize(StormTopology topology, Map conf) {
    Map<String, SpoutSpec> copySpouts = new HashMap<String, SpoutSpec>();
    Map<String, Bolt> copyBolts = new HashMap<String, Bolt>();
    for (Map.Entry<String, SpoutSpec> spoutEntry : spouts.entrySet()) {
      SpoutSpec spout = spoutEntry.getValue();
      copySpouts.put(spoutEntry.getKey(), spoutEntry.getValue());
    }
    for (Map.Entry<String, Bolt> boltEntry : bolts.entrySet()) {
      copyBolts.put(boltEntry.getKey(), boltEntry.getValue());
    }
    return new StormTopology(copySpouts, copyBolts);
  }

  /**
   * Combines all components into one Map.
   * @return Map of all components
   */
  public Map<String, Object> allComponents() {
    Map<String, Object> ret = new HashMap<String, Object>(spouts);
    ret.putAll(bolts);
    return ret;
  }

}
