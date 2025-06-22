package org.qontak.roomstreamsanalytic.model;

import lombok.Data;

@Data
public class EnrichedRoomCustomView {

    private final RoomEvent roomEvent;
    private final CustomView customView;

    public EnrichedRoomCustomView(RoomEvent roomEvent, CustomView customView) {
        this.roomEvent = roomEvent;
        this.customView = customView;
    }

    public boolean matchesCriteria() {
        for (CustomViewFilter filter : customView.getFilters()) {
            if (filter.getField().equals("status")) {
                if (filter.getOperator().equals("is")) {
                    if (!roomEvent.getStatus().equalsIgnoreCase(filter.getValue())) {
                        return false;
                    }
                }
            }

            if (filter.getField().equals("division_id")) {
                if (filter.getOperator().equals("has_any_value")) {
                    if (roomEvent.getDivisionId() == null || roomEvent.getDivisionId().isEmpty()) {
                        return false;
                    }
                }
            }
        }

        return true;
    }

    @Override
    public String toString() {
        return "EnrichedRoomCustomView{" +
                "roomEvent=" + roomEvent +
                ", customView=" + customView +
                '}';
    }
}
