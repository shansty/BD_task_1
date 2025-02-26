CREATE TYPE user_type AS ENUM ('member', 'casual');
CREATE TYPE rideable_type AS ENUM ('electric_bike', 'classic_bike');

CREATE TABLE Coordinates (
    coordinate_id SERIAL PRIMARY KEY,
    lat DOUBLE PRECISION,
    lng DOUBLE PRECISION,
    UNIQUE (lat, lng)
);

CREATE TABLE Stations (
    station_id VARCHAR(10) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    coordinate_id INT NOT NULL,
    FOREIGN KEY (coordinate_id) REFERENCES Coordinates(coordinate_id) ON DELETE CASCADE
);

CREATE TABLE Rides (
    ride_id VARCHAR(20) PRIMARY KEY,
    rideable_type rideable_type NOT NULL, 
    started_at TIMESTAMP NOT NULL, 
    ended_at TIMESTAMP NOT NULL, 
    start_station_id VARCHAR(10), 
    FOREIGN KEY (start_station_id) REFERENCES Stations(station_id) ON DELETE SET NULL,
    end_station_id VARCHAR(10), 
    FOREIGN KEY (end_station_id) REFERENCES Stations(station_id) ON DELETE SET NULL,
    member_casual user_type NOT NULL
);
