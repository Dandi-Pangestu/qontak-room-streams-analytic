package org.qontak.viewcounter.util;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class ClickhouseQuery {

    public static String FetchAggregatedViewCount = """
            SELECT
                cv.id AS view_id,
                cv.organization_id AS organization_id,
                count() AS count
            FROM (
                SELECT
                    id,
                    organization_id,
                    argMax(name, version_updated) AS name,
                    argMax(status, version_updated) AS status,
                    argMax(type, version_updated) AS type,
                    argMax(channel_integration_id, version_updated) AS channel_integration_id,
                    argMax(account_uniq_id, version_updated) AS account_uniq_id,
                    argMax(division_id, version_updated) AS division_id,
                    argMax(is_blocked, version_updated) AS is_blocked
                FROM room_events
                WHERE organization_id = ?
                GROUP BY organization_id, id
            ) AS re
            JOIN (
            	SELECT
            		id,
            		organization_id,
            		argMax(filters, version_updated) AS filters
            	FROM custom_view_events
            	WHERE organization_id = ? AND id = ?
            	GROUP BY organization_id, id
            ) AS cv
            USING (organization_id)
            WHERE
                arrayAll(
                    f -> (
                        -- Equality check (field = value)
                        (JSONExtractString(f, 'operator') = 'is'
                            AND (
                                (JSONExtractString(f, 'field') = 'status' AND toString(re.status)  = JSONExtractString(f, 'value')) OR
                                (JSONExtractString(f, 'field') = 'channel_integration_id' AND toString(re.channel_integration_id)  = JSONExtractString(f, 'value')) OR
                                (JSONExtractString(f, 'field') = 'division_id' AND toString(re.division_id)  = JSONExtractString(f, 'value'))
                            )
                        )
                      
                        -- Not equal (field != value)
                        OR (JSONExtractString(f, 'operator') = 'is_not'
                            AND (
                                (JSONExtractString(f, 'field') = 'status' AND toString(re.status)  != JSONExtractString(f, 'value')) OR
                                (JSONExtractString(f, 'field') = 'channel_integration_id' AND toString(re.channel_integration_id)  != JSONExtractString(f, 'value')) OR
                                (JSONExtractString(f, 'field') = 'division_id' AND toString(re.division_id)  != JSONExtractString(f, 'value'))
                            )
                        )
                            
                        -- "IN" check (array contains value)
                        OR (JSONExtractString(f, 'operator') = 'in'
                            AND (
                                (JSONExtractString(f, 'field') = 'status'
                                    AND has(
            							    arrayMap(x -> trim(BOTH '"' FROM x), JSONExtractArrayRaw(f, 'value_array')),
            							    toString(re.status)
            							)
                                )
                                OR (JSONExtractString(f, 'field') = 'channel_integration_id'
                                    AND has(
            							    arrayMap(x -> trim(BOTH '"' FROM x), JSONExtractArrayRaw(f, 'value_array')),
            							    toString(re.channel_integration_id)
            							)
                                )
                                OR (JSONExtractString(f, 'field') = 'division_id'
                                    AND has(
            							    arrayMap(x -> trim(BOTH '"' FROM x), JSONExtractArrayRaw(f, 'value_array')),
            							    toString(re.division_id)
            							)
                                )
                            )
                        )
                    ),
                    JSONExtractArrayRaw(cv.filters)
                )
            GROUP BY cv.organization_id, cv.id
            """;

    public static String FetchRoomViewMapping = """
            SELECT
                cv.id AS view_id,
                cv.organization_id AS organization_id,
                re.id AS room_id
            FROM (
                SELECT
                    id,
                    organization_id,
                    argMax(name, version_updated) AS name,
                    argMax(status, version_updated) AS status,
                    argMax(type, version_updated) AS type,
                    argMax(channel_integration_id, version_updated) AS channel_integration_id,
                    argMax(account_uniq_id, version_updated) AS account_uniq_id,
                    argMax(division_id, version_updated) AS division_id,
                    argMax(is_blocked, version_updated) AS is_blocked
                FROM room_events
                WHERE organization_id = ?
                GROUP BY organization_id, id
            ) AS re
            JOIN (
            	SELECT
            		id,
            		organization_id,
            		argMax(filters, version_updated) AS filters
            	FROM custom_view_events
            	WHERE organization_id = ? AND id = ?
            	GROUP BY organization_id, id
            ) AS cv
            USING (organization_id)
            WHERE
                arrayAll(
                    f -> (
                        -- Equality check (field = value)
                        (JSONExtractString(f, 'operator') = 'is'
                            AND (
                                (JSONExtractString(f, 'field') = 'status' AND toString(re.status)  = JSONExtractString(f, 'value')) OR
                                (JSONExtractString(f, 'field') = 'channel_integration_id' AND toString(re.channel_integration_id)  = JSONExtractString(f, 'value')) OR
                                (JSONExtractString(f, 'field') = 'division_id' AND toString(re.division_id)  = JSONExtractString(f, 'value'))
                            )
                        )
                      
                        -- Not equal (field != value)
                        OR (JSONExtractString(f, 'operator') = 'is_not'
                            AND (
                                (JSONExtractString(f, 'field') = 'status' AND toString(re.status)  != JSONExtractString(f, 'value')) OR
                                (JSONExtractString(f, 'field') = 'channel_integration_id' AND toString(re.channel_integration_id)  != JSONExtractString(f, 'value')) OR
                                (JSONExtractString(f, 'field') = 'division_id' AND toString(re.division_id)  != JSONExtractString(f, 'value'))
                            )
                        )
                        
                        -- "IN" check (array contains value)
                        OR (JSONExtractString(f, 'operator') = 'in'
                            AND (
                                (JSONExtractString(f, 'field') = 'status'
                                    AND has(
            							    arrayMap(x -> trim(BOTH '"' FROM x), JSONExtractArrayRaw(f, 'value_array')),
            							    toString(re.status)
            							)
                                )
                                OR (JSONExtractString(f, 'field') = 'channel_integration_id'
                                    AND has(
            							    arrayMap(x -> trim(BOTH '"' FROM x), JSONExtractArrayRaw(f, 'value_array')),
            							    toString(re.channel_integration_id)
            							)
                                )
                                OR (JSONExtractString(f, 'field') = 'division_id'
                                    AND has(
            							    arrayMap(x -> trim(BOTH '"' FROM x), JSONExtractArrayRaw(f, 'value_array')),
            							    toString(re.division_id)
            							)
                                )
                            )
                        )
                    ),
                    JSONExtractArrayRaw(cv.filters)
                )
            """;

    public static ResultSet executeQueryWithRetry(PreparedStatement statement) throws Exception {
        int retries = 3;
        while (retries > 0) {
            try {
                return statement.executeQuery();
            } catch (Exception e) {
                retries--;
                if (retries == 0) {
                    throw e;  // If all retries fail, throw exception.
                }
                Thread.sleep(500); // Wait before retrying.
            }
        }
        return null;
    }
}
