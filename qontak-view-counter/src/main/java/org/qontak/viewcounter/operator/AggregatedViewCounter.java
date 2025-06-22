package org.qontak.viewcounter.operator;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.qontak.viewcounter.model.AggregatedViewCount;
import org.qontak.viewcounter.model.CustomViewEvent;
import org.qontak.viewcounter.model.IncrementalViewUpdate;
import org.qontak.viewcounter.model.RoomEvent;
import org.qontak.viewcounter.util.ClickhouseEnv;
import org.qontak.viewcounter.util.ClickhouseQuery;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.*;

public class AggregatedViewCounter extends KeyedCoProcessFunction<String, RoomEvent, CustomViewEvent, AggregatedViewCount> {

    // Efficient room → view lookup (roomId as key)
    private transient MapState<String, List<String>> roomToViewState;

    // Store all active view filters orgId → (viewId → CustomView)
    private transient MapState<String, Map<String, CustomViewEvent>> viewState;

    private transient Connection clickhouseConnection;

    // Stores which rooms are linked to a view
    private transient MapState<String, List<String>> viewToRoomState;

    // Stores aggregated view count (viewId as key)
    private transient MapState<String, Long> aggregatedViewState;

    // State to store pending CustomViewEvents
    private transient MapState<String, CustomViewEvent> pendingViewUpdates;

    @Override
    public void open(OpenContext openContext) throws Exception {
        roomToViewState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>(
                        "roomToViewState",
                        Types.STRING,
                        Types.LIST(Types.STRING)
                )
        );

        viewState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>(
                        "viewState",
                        Types.STRING,
                        Types.MAP(Types.STRING, TypeInformation.of(CustomViewEvent.class))
                )
        );

        clickhouseConnection = DriverManager.getConnection(ClickhouseEnv.URL(), ClickhouseEnv.Username(), ClickhouseEnv.Password());

        viewToRoomState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>(
                        "viewToRoomState",
                        Types.STRING,
                        Types.LIST(Types.STRING)
                )
        );

        aggregatedViewState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>(
                        "aggregatedViewState",
                        Types.STRING,
                        Types.LONG
                )
        );

        pendingViewUpdates = getRuntimeContext().getMapState(
                new MapStateDescriptor<>(
                        "pendingViewUpdates",
                        Types.STRING,
                        TypeInformation.of(CustomViewEvent.class)
                )
        );
    }

    @Override
    public void processElement1(RoomEvent roomEvent, KeyedCoProcessFunction<String, RoomEvent, CustomViewEvent, AggregatedViewCount>.Context context, Collector<AggregatedViewCount> out) throws Exception {
        String roomId = roomEvent.getId();
        String organizationId = roomEvent.getOrganizationId();

        synchronized (organizationId.intern()) {
            List<String> previousViewList = roomToViewState.get(roomId);
            Set<String> previousViews = previousViewList == null ? Collections.emptySet() : new HashSet<>(previousViewList);
            Set<String> currentViews = new HashSet<>();
            List<IncrementalViewUpdate> incrementalViewUpdates = new ArrayList<>();

            Map<String, CustomViewEvent> orgViews = viewState.get(organizationId);
            if (orgViews != null) {
                for (Map.Entry<String, CustomViewEvent> entry : orgViews.entrySet()) {
                    if (entry.getValue().matches(roomEvent)) {
                        currentViews.add(entry.getKey());
                    }
                }
            }

            // Handle View Removals
            for (String viewId : previousViews) {
                if (!currentViews.contains(viewId)) {
                    incrementalViewUpdates.add(new IncrementalViewUpdate(viewId, organizationId, -1));

                    // Update viewToRoomState (detach room)
                    List<String> rooms = viewToRoomState.get(viewId);
                    if (rooms != null) {
                        rooms.remove(roomId);
                        if (rooms.isEmpty()) {
                            viewToRoomState.remove(viewId);
                        } else {
                            viewToRoomState.put(viewId, new ArrayList<>(rooms));
                        }
                    }
                }
            }

            // Handle View Additions
            for (String viewId : currentViews) {
                if (!previousViews.contains(viewId)) {
                    incrementalViewUpdates.add(new IncrementalViewUpdate(viewId, organizationId, 1));

                    // Update viewToRoomState
                    List<String> rooms = viewToRoomState.get(viewId);
                    if (rooms == null) {
                        rooms = new ArrayList<>();
                    }
                    if (!rooms.contains(roomId)) {
                        rooms.add(roomId);
                        viewToRoomState.put(viewId, rooms);
                    }
                }
            }

            if (!previousViews.equals(currentViews)) {
                if (!currentViews.isEmpty()) {
                    roomToViewState.put(roomId, new ArrayList<>(currentViews));
                } else {
                    roomToViewState.remove(roomId);
                }
            }

            // Updating aggregated view count with incremental value
            Set<String> updatedViews = new HashSet<>();
            for (IncrementalViewUpdate incrementalViewUpdate : incrementalViewUpdates) {
                Long currentCount = aggregatedViewState.get(incrementalViewUpdate.getViewId());
                if (currentCount == null) {
                    currentCount = 0L;
                }
                Long newCount = currentCount + incrementalViewUpdate.getCount();
                aggregatedViewState.put(incrementalViewUpdate.getViewId(), newCount);
                updatedViews.add(incrementalViewUpdate.getViewId());
            }

            for (String viewId : updatedViews) {
                out.collect(new AggregatedViewCount(
                        viewId,
                        organizationId,
                        aggregatedViewState.get(viewId),
                        LocalDateTime.now(ZoneId.of("UTC")).toInstant(ZoneOffset.UTC).toEpochMilli())
                );
            }
        }
    }

    @Override
    public void processElement2(CustomViewEvent event, KeyedCoProcessFunction<String, RoomEvent, CustomViewEvent, AggregatedViewCount>.Context context, Collector<AggregatedViewCount> out) throws Exception {
        String organizationId = event.getOrganizationId();
        String viewId = event.getId();

        synchronized (organizationId.intern()) {
            pendingViewUpdates.put(viewId, event);

            // Register a timer for 5 seconds
            context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + 5000);
        }
    }

    @Override
    public void onTimer(long timestamp, KeyedCoProcessFunction<String, RoomEvent, CustomViewEvent, AggregatedViewCount>.OnTimerContext ctx, Collector<AggregatedViewCount> out) throws Exception {
        Iterator<Map.Entry<String, CustomViewEvent>> iterator = pendingViewUpdates.entries().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, CustomViewEvent> entry = iterator.next();
            CustomViewEvent event = entry.getValue();

            // Remove the pending event from state
            iterator.remove();

            // Process the event (execute ClickHouse query)
            processViewUpdate(event, out);
        }
    }

    private void processViewUpdate(CustomViewEvent event, Collector<AggregatedViewCount> out) throws Exception {
        String organizationId = event.getOrganizationId();
        String viewId = event.getId();

        Map<String, CustomViewEvent> orgViews = viewState.get(organizationId);
        if (orgViews == null) {
            orgViews = new HashMap<>();
        }

        // Skip processing if view criteria haven't changed
        CustomViewEvent previousView = orgViews.get(viewId);
        if (previousView != null && previousView.equals(event)) {
            return; // No need to recompute!
        }

        orgViews.put(viewId, event);
        viewState.put(organizationId, orgViews);

        // Get affected rooms using viewToRoomState
        List<String> affectedRooms = viewToRoomState.get(viewId);
        if (affectedRooms != null) {
            for (String roomId : affectedRooms) {
                List<String> views = roomToViewState.get(roomId);
                if (views != null) {
                    views.remove(viewId);
                    if (views.isEmpty()) {
                        roomToViewState.remove(roomId);
                    } else {
                        roomToViewState.put(roomId, views);
                    }
                }
            }
            viewToRoomState.remove(viewId);
        }

        // Fetch new (room → view) mappings from Clickhouse
        Map<String, String> updatedRoomtoView = new HashMap<>();
        try (PreparedStatement statement = clickhouseConnection.prepareStatement(ClickhouseQuery.FetchRoomViewMapping)) {
            statement.setString(1, organizationId);
            statement.setString(2, organizationId);
            statement.setString(3, viewId);
            try (ResultSet rs = ClickhouseQuery.executeQueryWithRetry(statement)) {
                if (rs != null) {
                    while (rs.next()) {
                        String roomId = rs.getString("room_id");
                        updatedRoomtoView.put(roomId, viewId);
                    }
                }
            }
        }

        // Update roomToViewState and viewToRoomState
        for (Map.Entry<String, String> entry : updatedRoomtoView.entrySet()) {
            String roomId = entry.getKey();

            List<String> existingViews = roomToViewState.get(roomId);
            if (existingViews == null) {
                existingViews = new ArrayList<>();
            }

            if (!existingViews.contains(viewId)) {
                existingViews.add(viewId);
                roomToViewState.put(roomId, existingViews);
            }

            List<String> rooms = viewToRoomState.get(viewId);
            if (rooms == null) {
                rooms = new ArrayList<>();
            }

            if (!rooms.contains(roomId)) {
                rooms.add(roomId);
                viewToRoomState.put(viewId, rooms);
            }
        }

        // Fetch new aggregated view count only for the affected view
        Long newCount = 0L;
        try (PreparedStatement statement = clickhouseConnection.prepareStatement(ClickhouseQuery.FetchAggregatedViewCount)) {
            statement.setString(1, organizationId);
            statement.setString(2, organizationId);
            statement.setString(3, viewId);
            try (ResultSet rs = ClickhouseQuery.executeQueryWithRetry(statement)) {
                if (rs != null) {
                    while (rs.next()) {
                        newCount = rs.getLong("count");
                    }
                }
            }
        }

        aggregatedViewState.put(viewId, newCount);
        out.collect(new AggregatedViewCount(
                viewId,
                organizationId,
                newCount,
                LocalDateTime.now(ZoneId.of("UTC")).toInstant(ZoneOffset.UTC).toEpochMilli())
        );
    }

    @Override
    public void close() throws Exception {
        if (clickhouseConnection != null) {
            clickhouseConnection.close();
        }
    }
}
