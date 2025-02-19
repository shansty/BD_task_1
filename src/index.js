import express from "express";
import { createServer } from 'node:http';
import multer from "multer";
import fs from "fs";
import csv from "csv-parser";
import dotenv from "dotenv";
import pkg from "pg";

dotenv.config();

const app = express();
const server = createServer(app);
const PORT = 3001;

const { Pool } = pkg;
const UPLOADS_FOLDER = process.env.UPLOADS_FOLDER || "./uploads";

const storage = multer.diskStorage({
    destination: (req, file, cb) => {
        cb(null, UPLOADS_FOLDER);
    },
    filename: (req, file, cb) => {
        cb(null, `${file.originalname}-${Date.now()}`);
    },
});
const upload = multer({ storage })
const BATCH_SIZE = 1000;

app.post("/import-csv", upload.single("file"), async (req, res) => {
    if (!req.file) {
        return res.status(400).json({ error: "No file uploaded" });
    }
    try {
        let batch = [];
        let count = 0;

        const stream = fs.createReadStream(req.file.path).pipe(csv());
        const pool = new Pool({
            connectionString: process.env.DATABASE_URL,
        });

        stream.on("data", async (row) => {
            try {
                batch.push(row);
                count++;

                if (count % BATCH_SIZE === 0) {
                    stream.pause();
                    console.log(`Processing batch ${count / BATCH_SIZE}...`);
                    await insertData(batch, pool);
                    batch = [];
                    stream.resume();
                }
            } catch (err) {
                console.error("Error inserting batch:", err);
                stream.destroy();
                res.status(500).json({ error: "Error processing CSV data" });
            }
        });

        stream.on("end", async () => {
            try {
                if (batch.length > 0) {
                    console.log("Processing last batch...");
                    await insertData(batch, pool);
                }
                console.log("CSV processing completed.");
                pool.end();
                res.status(200).json({ message: "CSV data imported successfully" });
            } catch (err) {
                console.error("Error inserting batch:", err);
                res.status(500).json({ error: "Error processing CSV data" });
            }
        });

        stream.on("error", (error) => {
            console.error("Stream error:", error);
            res.status(500).json({ error: "Error reading CSV file" });
        });

    } catch (error) {
        console.error("Error inserting data:", error);
    }
});


async function insertData(batch, pool) {
    const client = await pool.connect();
    try {
        await client.query("BEGIN");

        const parseCoordinate = (value) => value === "" || value === null ? 0 : value;

        const coordinatesValues = batch.flatMap(row => [
            [parseCoordinate(row.start_lat), parseCoordinate(row.start_lng)], [parseCoordinate(row.end_lat), parseCoordinate(row.end_lng)]
        ]);

        const coordinates_placeholders = coordinatesValues.map((_, i) => `($${i * 2 + 1}, $${i * 2 + 2})`).join(", ");

        await client.query(
            `INSERT INTO Coordinates (lat, lng) 
             VALUES ${coordinates_placeholders}
             ON CONFLICT (lat, lng) DO NOTHING`,
             coordinatesValues.flat()
        );


        const stationsValues = batch.flatMap(row => [
                [row.start_station_id, row.start_station_name, parseCoordinate(row.start_lat), parseCoordinate(row.start_lng)],
                [row.end_station_id, row.end_station_name, parseCoordinate(row.end_lat), parseCoordinate(row.end_lng)]
            ]);

        const stations_placeholders = stationsValues.map((_, i) => `($${i * 4 + 1}, $${i * 4 + 2}, $${i * 4 + 3}, $${i * 4 + 4})`).join(", ");

        await client.query(
            `INSERT INTO Stations (station_id, name, coordinate_id)
                 SELECT s.station_id, s.name, c.coordinate_id
                 FROM (VALUES ${stations_placeholders}) AS s(station_id, name, lat, lng)
                 JOIN Coordinates c 
                 ON s.lat::DOUBLE PRECISION = c.lat 
                 AND s.lng::DOUBLE PRECISION = c.lng
                 ON CONFLICT (station_id) DO NOTHING`,
            stationsValues.flat()
        );


        const ridesValues = batch.map((row) => [
            row.ride_id, row.rideable_type, row.started_at, row.ended_at, row.start_station_id, row.end_station_id, row.member_casual
        ]);

        const ride_placeholderrs = ridesValues.map((_, i) => `($${i * 7 + 1}, $${i * 7 + 2}, $${i * 7 + 3}, $${i * 7 + 4}, $${i * 7 + 5}, $${i * 7 + 6}, $${i * 7 + 7})`);

        await client.query(
            `INSERT INTO Rides (ride_id, rideable_type, started_at, ended_at, start_station_id, end_station_id, member_casual) 
                VALUES ${ride_placeholderrs}
                ON CONFLICT (ride_id) DO NOTHING`,
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

server.listen(PORT, (error) => {
    if (error) {
        console.error("Error starting server:", error);
    } else {
        console.log(`Server is running at http://localhost:${PORT}`);
    }
});