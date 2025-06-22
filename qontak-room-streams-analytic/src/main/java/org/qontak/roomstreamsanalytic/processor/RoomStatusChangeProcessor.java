package org.qontak.roomstreamsanalytic.processor;

import org.apache.kafka.streams.processor.api.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.qontak.roomstreamsanalytic.model.RoomStatusChange;
import org.qontak.roomstreamsanalytic.model.RoomEvent;

public class RoomStatusChangeProcessor implements FixedKeyProcessor<String, RoomEvent, RoomStatusChange> {

    private FixedKeyProcessorContext<String, RoomStatusChange> context;
    private KeyValueStore<String, String> stateStore;

    @Override
    public void init(FixedKeyProcessorContext<String, RoomStatusChange> context) {
        this.context = context;
        this.stateStore = context.getStateStore("room-status-store");
    }

    @Override
    public void process(FixedKeyRecord<String, RoomEvent> record) {
        String roomId = record.key();
        RoomEvent currentEvent = record.value();

        String previousStatus = stateStore.get(roomId);
        stateStore.put(roomId, currentEvent.getStatus());

        // Skip forwarding if the status hasn't changed
        if (previousStatus != null && previousStatus.equals(currentEvent.getStatus())) {
            return; // No change, skip forwarding
        }

        RoomStatusChange statusChange = new RoomStatusChange(
                previousStatus,
                currentEvent.getStatus(),
                currentEvent.getOrganizationId()
        );

        // Forward the status change to downstream processors
        context.forward(record.withValue(statusChange));
    }

    @Override
    public void close() {}
}
