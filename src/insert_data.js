import { getCoordinatesQueryText, getStationsQueryText, getRidesQueryText } from "./query_texts.js";

export async function insertData(batch, pool) {
    
    const client = await pool.connect();

    try {
        await client.query("BEGIN");

        const parseCoordinate = (value) => value === "" ? null : value;

        const coordinatesValues = batch.flatMap(row => [
            [parseCoordinate(row.start_lat), parseCoordinate(row.start_lng)], [parseCoordinate(row.end_lat), parseCoordinate(row.end_lng)]
        ]);

        const coordinates_placeholders = coordinatesValues.map((_, i) => `($${i * 2 + 1}, $${i * 2 + 2})`).join(", ");


        await client.query(
            getCoordinatesQueryText(coordinates_placeholders),
            coordinatesValues.flat()
        );


        const stationsValues = batch.flatMap(row => [
            [row.start_station_id, row.start_station_name, parseCoordinate(row.start_lat), parseCoordinate(row.start_lng)],
            [row.end_station_id, row.end_station_name, parseCoordinate(row.end_lat), parseCoordinate(row.end_lng)]
        ]);

        const stations_placeholders = stationsValues.map((_, i) => `($${i * 4 + 1}, $${i * 4 + 2}, $${i * 4 + 3}, $${i * 4 + 4})`).join(", ");


        await client.query(
            getStationsQueryText(stations_placeholders),
            stationsValues.flat()
        );

        
        const ridesValues = batch.map((row) => [
            row.ride_id, row.rideable_type, row.started_at, row.ended_at, row.start_station_id, row.end_station_id, row.member_casual
        ]);

        const rides_placeholders = ridesValues.map((_, i) => `($${i * 7 + 1}, $${i * 7 + 2}, $${i * 7 + 3}, $${i * 7 + 4}, $${i * 7 + 5}, $${i * 7 + 6}, $${i * 7 + 7})`);


        await client.query(
            getRidesQueryText(rides_placeholders),
            ridesValues.flat()
        );

        await client.query("COMMIT");
        console.log(`Inserted ${batch.length} records successfully.`);
    } catch (error) {
        await client.query("ROLLBACK");
        console.error("Error inserting data:", error);
        throw error;
    } finally {
        client.release();
    }
}
