import { getCoordinatesQueryText, getStationsQueryText, getRidesQueryText } from "./query_texts.js";


export async function insertData(batch, pool) {

    const client = await pool.connect();

    try {
        await client.query("BEGIN");

        const coordinatesValues = getCoordinateValues(batch);
        await client.query(
            getCoordinatesQueryText(getCoordinatesIndexes(coordinatesValues)),
            coordinatesValues.flat()
        );

        const stationsValues = getStationValues(batch);
        await client.query(
            getStationsQueryText(getStationsIndexes(stationsValues)),
            stationsValues.flat()
        );

        const ridesValues = getRideValues(batch);
        await client.query(
            getRidesQueryText(getRidesIndexes(ridesValues)),
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

function parseCoordinate(value) {
    return value === "" ? null : value;
};

function getCoordinatesIndexes(coordinatesValues) {
    return coordinatesValues.map((_, i) => `($${i * 2 + 1}, $${i * 2 + 2})`).join(", ");
}

function getStationsIndexes(stationsValues) {
    return stationsValues.map((_, i) => `($${i * 4 + 1}, $${i * 4 + 2}, $${i * 4 + 3}, $${i * 4 + 4})`).join(", ");
}

function getRidesIndexes(ridesValues) {
    return ridesValues.map((_, i) => `($${i * 7 + 1}, $${i * 7 + 2}, $${i * 7 + 3}, $${i * 7 + 4}, $${i * 7 + 5}, $${i * 7 + 6}, $${i * 7 + 7})`);
}

function getCoordinateValues(batch) {
    return batch.flatMap(row => [
        [parseCoordinate(row.start_lat), parseCoordinate(row.start_lng)], [parseCoordinate(row.end_lat), parseCoordinate(row.end_lng)]
    ]);
}

function getStationValues(batch) {
    return batch.flatMap(row => [
        [row.start_station_id, row.start_station_name, parseCoordinate(row.start_lat), parseCoordinate(row.start_lng)],
        [row.end_station_id, row.end_station_name, parseCoordinate(row.end_lat), parseCoordinate(row.end_lng)]
    ]);    
}

function getRideValues(batch) {
    return batch.map((row) => [
        row.ride_id, row.rideable_type, row.started_at, row.ended_at, row.start_station_id, row.end_station_id, row.member_casual
    ]);
}


