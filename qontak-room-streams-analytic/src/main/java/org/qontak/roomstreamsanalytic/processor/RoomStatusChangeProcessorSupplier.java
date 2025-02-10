package org.qontak.roomstreamsanalytic.processor;

import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.qontak.roomstreamsanalytic.model.RoomStatusChange;
import org.qontak.roomstreamsanalytic.model.RoomStatusEvent;

public class RoomStatusChangeProcessorSupplier implements FixedKeyProcessorSupplier<String, RoomStatusEvent, RoomStatusChange> {

    @Override
    public FixedKeyProcessor<String, RoomStatusEvent, RoomStatusChange> get() {
        return new RoomStatusChangeProcessor();
    }
}
