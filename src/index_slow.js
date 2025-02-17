import express from "express";
import multer from "multer";
import path from "path";
import fs from "fs";
import csv from "csv-parser";
import pkg from "pg";
import { fileURLToPath } from "url";
import dotenv from "dotenv";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const { Pool } = pkg;
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

const PORT = process.env.PORT || 3001;
const UPLOADS_FOLDER = process.env.UPLOADS_FOLDER || "./uploads";

// Multer setup for file uploads
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, UPLOADS_FOLDER);
  },
  filename: (req, file, cb) => {
    cb(null, `${Date.now()}-${file.originalname}`);
  },
});

const upload = multer({ storage });

// API Endpoint to upload and process CSV file
app.post("/import-csv", upload.single("file"), async (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: "No file uploaded" });
  }

  const filePath = path.join(UPLOADS_FOLDER, req.file.filename);
  const results = [];

  fs.createReadStream(filePath)
    .pipe(csv())
    .on("data", (row) => {
      results.push(row);
    })
    .on("end", async () => {
      const client = await pool.connect();
      try {
        await client.query("BEGIN");

        for (const row of results) {
          console.dir({ row: row })
          debugger;

          // Function to parse coordinates safely
          const parseCoordinate = (value) => {
            return value === "" || value === null ? 0 : value;
          };

          let startLat = parseCoordinate(row.start_lat);
          let startLng = parseCoordinate(row.start_lng);
          let endLat = parseCoordinate(row.start_lat);
          let endLng = parseCoordinate(row.start_lng);

          console.log("Parsed Coordinates:", { startLat, startLng });

          // Ensure start_station_id exists with a valid coordinate_id
          let startCoordRes = await client.query(
            "SELECT coordinate_id FROM Coordinates WHERE lat = $1 AND lng = $2",
            [startLat, startLng]
          ); //is empty
          console.dir({ startCoordRes: startCoordRes })
          debugger;

          let startCoordinateId;

          debugger;

          if (startCoordRes.rows.length === 0) {
            debugger;
            const newCoordRes = await client.query(
              "INSERT INTO Coordinates (lat, lng) VALUES ($1, $2) RETURNING coordinate_id",
              [startLat, startLng]
            );
            console.dir({ newCoordRes: newCoordRes })

            debugger;
            startCoordinateId = newCoordRes.rows[0].coordinate_id;
            console.dir({ startCoordinateId: startCoordinateId })

          } else {
            startCoordinateId = startCoordRes.rows[0].coordinate_id;
          }
          debugger;



          let endCoordinateId;
          let endCoordRes = await client.query(
            "SELECT coordinate_id FROM Coordinates WHERE lat = $1 AND lng= $2",
            [endLat, endLng]
          );

          if (endCoordRes.rows.length === 0) {
            const newCoordRes = await client.query(
              "INSERT INTO Coordinates (lat, lng) VALUES ($1, $2) RETURNING coordinate_id",
              [endLat, endLng]
            );
            endCoordinateId = newCoordRes.rows[0].coordinate_id;
          } else {
            endCoordinateId = endCoordRes.rows[0].coordinate_id;
          }

          // Now insert into Stations with a valid coordinate_id
          let startStationRes = await client.query(
            "SELECT station_id FROM Stations WHERE station_id = $1",
            [row.start_station_id]
          );
          debugger;

          if (startStationRes.rows.length === 0) {
            await client.query(
              "INSERT INTO Stations (station_id, names, coordinate_id) VALUES ($1, $2, $3)",
              [row.start_station_id, row.start_station_name, startCoordinateId]
            );
          }
          debugger;


          let endStationRes = await client.query(
            "SELECT station_id FROM Stations WHERE station_id = $1",
            [row.end_station_id]
          );
          debugger;

          if (endStationRes.rows.length === 0) {
            await client.query(
              "INSERT INTO Stations (station_id, names, coordinate_id) VALUES ($1, $2, $3)",
              [row.end_station_id, row.end_station_name, endCoordinateId]
            );
          }
          debugger;

          // Insert into Ride_sessions table
          const rideSessionRes = await client.query(
            "INSERT INTO Ride_sessions (started_at, ended_at) VALUES ($1, $2) RETURNING ride_session_id",
            [row.started_at, row.ended_at]
          );
          debugger;
          const ride_session_id = rideSessionRes.rows[0].ride_session_id;
          debugger;
          // Insert into Users table (or get existing user ID)
          let userId;
          const userRes = await client.query(
            "SELECT user_id FROM Users WHERE type = $1",
            [row.member_casual]
          );
          debugger;
          if (userRes.rows.length > 0) {
            userId = userRes.rows[0].user_id;
          } else {
            const newUserRes = await client.query(
              "INSERT INTO Users (type) VALUES ($1) RETURNING user_id",
              [row.member_casual]
            );
            userId = newUserRes.rows[0].user_id;
          }
          debugger;
          console.dir({ row: row })
          console.log("ride")

          // Insert into Rides table
          await client.query(
            "INSERT INTO Rides (ride_id, rideable_type, ride_session_id, start_station_id, end_station_id, user_id) VALUES ($1, $2, $3, $4, $5, $6)",
            [
              row.ride_id,
              row.rideable_type,
              ride_session_id,
              row.start_station_id,
              row.end_station_id,
              userId,
            ]
          );
        }

        console.log("ride")


        await client.query("COMMIT");
        res.json({ message: "CSV data imported successfully" });
      } catch (error) {
        await client.query("ROLLBACK");
        console.error("Error inserting data:", error);
        res.status(500).json({ error: "Error inserting data" });
      } finally {
        client.release();
        fs.unlinkSync(filePath); // Delete file after processing
      }
    });
});

app.listen(PORT, () => {
  console.log(`Server is running on http://localhost:${PORT}`);
});
