package backtype.storm.tuple;

import java.util.Deque;
import java.util.LinkedList;
import java.util.logging.Logger;
//import java.util.concurrent.LinkedBlockingDeque;

public class TupleImplPool {

  private final int DEFAULT_POOL_SIZE = 100;

  private final Deque<TupleImpl> idleObjects;
//  private final Logger log = Logger.getLogger("TupleImplPool");
  private final TupleImplFactory factory;

  public TupleImplPool(TupleImplFactory factory) {
    this.factory = factory;
    this.idleObjects = new LinkedList<TupleImpl>();
  }

  public TupleImpl borrowObject() {
    if (idleObjects.isEmpty()) {
//      log.info("Created tuple");
      return factory.create();
    } else {
//      log.info("Reused tuple");
      return idleObjects.pop();
    }
  }

  public void returnObject(TupleImpl tuple) {
    if (idleObjects.size() < DEFAULT_POOL_SIZE) {
      idleObjects.addLast(tuple);
//      log.info("Added tuple, size: " + idleObjects.size());
    }
  }

  public int getNumIdle() {
    return idleObjects.size();
  }

  public void clear() {
    idleObjects.clear();
  }

}
