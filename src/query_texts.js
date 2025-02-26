export function getCoordinatesQueryText(coordinates_placeholders) {
    return `INSERT INTO Coordinates (lat, lng)
            SELECT DISTINCT v.lat::DOUBLE PRECISION, v.lng::DOUBLE PRECISION
            FROM (VALUES ${coordinates_placeholders}) AS v(lat, lng)
            ON CONFLICT (lat, lng) DO NOTHING;`
}
//     `WITH new_coords AS (
//         INSERT INTO Coordinates (lat, lng)
//         SELECT DISTINCT v.lat::DOUBLE PRECISION, v.lng::DOUBLE PRECISION
//         FROM (VALUES ${coordinates_placeholders}) AS v(lat, lng)
//         ON CONFLICT (lat, lng) DO NOTHING
//         RETURNING coordinate_id
//     )
//     SELECT setval('coordinates_coordinate_id_seq', (coordinate_id + 1), true) FROM Coordinates;`,

export function getStationsQueryText(stations_placeholders) {
    return `INSERT INTO Stations (station_id, name, coordinate_id)
                 SELECT s.station_id, s.name, c.coordinate_id
                 FROM (VALUES ${stations_placeholders}) AS s(station_id, name, lat, lng)
                 JOIN Coordinates c 
                 ON s.lat::DOUBLE PRECISION = c.lat 
                 AND s.lng::DOUBLE PRECISION = c.lng
                 ON CONFLICT (station_id) DO NOTHING`
}

export function getRidesQueryText(rides_placeholderrs) {
    return `INSERT INTO Rides (ride_id, rideable_type, started_at, ended_at, start_station_id, end_station_id, member_casual) 
                VALUES ${rides_placeholderrs}
                ON CONFLICT (ride_id) DO NOTHING`
}



