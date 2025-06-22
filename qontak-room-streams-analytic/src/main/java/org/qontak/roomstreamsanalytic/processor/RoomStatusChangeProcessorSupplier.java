package org.qontak.roomstreamsanalytic.processor;

import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.qontak.roomstreamsanalytic.model.RoomStatusChange;
import org.qontak.roomstreamsanalytic.model.RoomEvent;

public class RoomStatusChangeProcessorSupplier implements FixedKeyProcessorSupplier<String, RoomEvent, RoomStatusChange> {

    @Override
    public FixedKeyProcessor<String, RoomEvent, RoomStatusChange> get() {
        return new RoomStatusChangeProcessor();
    }
}
