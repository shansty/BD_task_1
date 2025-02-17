import express from "express";
import { createServer } from 'node:http';
import multer from "multer";
import path from "path";
import fs from "fs";
import csv from "csv-parser";
import { fileURLToPath } from "url";
import dotenv from "dotenv";
import pkg from "pg";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const server = createServer(app);
const PORT = 3001;

const { Pool } = pkg;
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
});
const UPLOADS_FOLDER = process.env.UPLOADS_FOLDER || "./uploads";


// app.use(express.json());
// app.use('/uploads', express.static(path.join(__dirname, '../uploads')));

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

        stream.on("data", async (row) => {
            batch.push(row);
            count++;

            if (count % BATCH_SIZE === 0) {
                stream.pause();
                console.log(`Processing batch ${count / BATCH_SIZE}...`);
                try {
                    await insertData(batch);
                } catch {
                    stream.destroy();
                    return;
                }
                batch = [];
                stream.resume();
            }
        });
        stream.on("end", async () => {
            if (batch.length > 0) {
                console.log("Processing last batch...");
                await insertData(batch);
            }
            console.log("CSV processing completed.");
            pool.end();
        });

        res.status(200).json({ message: "CSV data imported successfully" });
    } catch (error) {
        console.error("Error inserting data:", error);
    }
});


async function insertData(batch) {
    const client = await pool.connect();
    try {
        await client.query("BEGIN"); // Start transaction

        // 1️⃣ Insert Coordinates (Avoid duplicates using ON CONFLICT)
        const coordinatesValues = batch
            .flatMap(row => [[row.start_lat, row.start_lng], [row.end_lat, row.end_lng]]);
            
        const coordinates_placeholders = coordinatesValues
            .map((_, i) => `($${i * 2 + 1}, $${i * 2 + 2})`)
            .join(", ");

        console.log(coordinates_placeholders)
        await client.query(
            `INSERT INTO Coordinates (lat, lng) 
             VALUES ${coordinates_placeholders}
             ON CONFLICT DO NOTHING`,
            coordinatesValues.flat()
        );
        console.log(`coord + ${coordinatesValues}`)

        // 2️⃣ Insert Stations (Avoid duplicates)
        const stationsValues = batch
            .flatMap(row => [
                [row.start_station_id, row.start_station_name, row.start_lat, row.start_lng],
                [row.end_station_id, row.end_station_name, row.end_lat, row.end_lng]
            ]);

        const stations_placeholders = stationsValues
            .map((_, i) => `($${i * 4 + 1}, $${i * 4 + 2}, $${i * 4 + 3}, $${i * 4 + 4})`)
            .join(", ");


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

        // 3️⃣ Insert Ride Sessions (Bulk Insert)
        const rideSessionsValues = batch.map(row => [row.started_at, row.ended_at]);

        const rideSessions_placeholders = rideSessionsValues
            .map((_, i) => `($${i * 2 + 1}, $${i * 2 + 2})`)
            .join(", ");

        const rideSessionRes = await client.query(
            `INSERT INTO Ride_sessions (started_at, ended_at) 
                 VALUES ${rideSessions_placeholders}
                 RETURNING ride_session_id`,
            rideSessionsValues.flat()
        );
        const rideSessionIds = rideSessionRes.rows.map(row => row.ride_session_id);

        // 4️⃣ Insert Rides (Bulk Insert)
        const ridesValues = batch.map((row, index) => [
            row.ride_id, row.rideable_type, rideSessionIds[index], row.start_station_id, row.end_station_id, row.member_casual
        ]);

        const ride_placeholderrs = ridesValues
            .map((_, i) => `($${i * 6 + 1}, $${i * 6 + 2}, $${i * 6 + 3}, $${i * 6 + 4}, $${i * 6 + 5}, $${i * 6 + 6})`)

        await client.query(
            `INSERT INTO Rides (ride_id, rideable_type, ride_session_id, start_station_id, end_station_id, member_casual) 
     VALUES ${ride_placeholderrs}`,
            ridesValues.flat()
        );

        await client.query("COMMIT"); // Commit all queries at once
        console.log(`Inserted ${batch.length} records successfully.`);
    } catch (error) {
        await client.query("ROLLBACK");
        console.error("Error inserting data:", error);
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