package org.qontak.roomstreamsanalytic.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

public class StoreConfig {

    public static StoreBuilder<KeyValueStore<String, String>> RoomStatusStore() {
        return Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore("room-status-store"),
                Serdes.String(),
                Serdes.String()
        );
    }
}
