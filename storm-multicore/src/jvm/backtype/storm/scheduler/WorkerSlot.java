package backtype.storm.scheduler;

import java.util.UUID;

public class WorkerSlot {

    private final UUID uuid;

    public WorkerSlot() {
        this.uuid = UUID.randomUUID();
    }

    public UUID getUUID() {
        return uuid;
    }

    @Override
    public int hashCode() {
        return uuid.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return uuid == ((WorkerSlot) obj).getUUID();
    }
}
