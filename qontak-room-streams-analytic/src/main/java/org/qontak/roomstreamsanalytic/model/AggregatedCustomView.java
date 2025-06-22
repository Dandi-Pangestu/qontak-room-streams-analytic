package org.qontak.roomstreamsanalytic.model;

import lombok.Data;

import java.util.HashSet;
import java.util.Set;

@Data
public class AggregatedCustomView {

    private long count;
    private Set<String> roomIds;

    public AggregatedCustomView() {
        this.count = 0;
        this.roomIds = new HashSet<>();
    }

    public AggregatedCustomView addRoom(String roomId) {
        if (!roomIds.contains(roomId)) {
            roomIds.add(roomId);
            count++;
        }
        return this;
    }

    @Override
    public String toString() {
        return "AggregatedCustomView{" +
                "count=" + count +
                ", roomIds=" + roomIds +
                '}';
    }
}
