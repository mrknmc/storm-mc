package backtype.storm.generated;

public class SubmitOptions {
  private TopologyInitialStatus initialStatus;

  public enum TopologyInitialStatus {
    ACTIVE(1),
    INACTIVE(2);

    private final int value;

    private TopologyInitialStatus(int value) {
      this.value = value;
    }

    /**
     * Get the integer value of this enum value, as defined in the Thrift IDL.
     */
    public int getValue() {
      return value;
    }

    /**
     * Find a the enum type by its integer value, as defined in the Thrift IDL.
     * @return null if the value is not found.
     */
    public static TopologyInitialStatus findByValue(int value) {
      switch (value) {
        case 1:
          return ACTIVE;
        case 2:
          return INACTIVE;
        default:
          return null;
      }
    }
  }
}


