import fs from "fs";
import csv from "csv-parser";
import dotenv from "dotenv";
import pool from "./db_pool.js";
import { server, app } from "./server.js";
import { upload } from "./multer.js";
import { getCoordinatesQueryText, getStationsQueryText, getRidesQueryText } from "./query_texts.js";
import('./server.js');


dotenv.config();

app.post("/import-csv", upload.single("file"), async (req, res) => {
    if (!req.file) {
        return res.status(400).json({ error: "No file uploaded" });
    }
    try {
        let batch = [];
        let count = 0;

        const stream = fs.createReadStream(req.file.path).pipe(csv());

        stream.on("data", async (row) => {
            try {
                batch.push(row);
                count++;

                if (count % process.env.BATCH_SIZE === 0) {
                    stream.pause();
                    console.log(`Processing batch ${count / process.env.BATCH_SIZE}...`);
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

        const rides_placeholderrs = ridesValues.map((_, i) => `($${i * 7 + 1}, $${i * 7 + 2}, $${i * 7 + 3}, $${i * 7 + 4}, $${i * 7 + 5}, $${i * 7 + 6}, $${i * 7 + 7})`);

        await client.query(
            getRidesQueryText(rides_placeholderrs),
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

server.listen(process.env.PORT, (error) => {
    if (error) {
        console.error("Error starting server:", error);
    } else {
        console.log(`Server is running at http://localhost:${process.env.PORT}`);
    }
});
