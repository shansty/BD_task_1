import fs from "fs";
import csv from "csv-parser";
import pool from "./db_pool.js";


export function processCsvFile(filePath, BATCH_SIZE, callback) {
    return new Promise((res, rej) => {
        const stream = fs.createReadStream(filePath).pipe(csv());
        let rows = [];
        stream.on('data', async (row) => {
                rows.push(row);
                if (rows.length >= BATCH_SIZE) {
                    stream.pause();
                    await callback(rows, pool);
                    rows = [];
                    stream.resume();
                }
            })
            .on('end', async () => {
                if (rows.length > 0) {
                    await callback(rows, pool);
                    res();
                }
            })
            .on('error', () => {
                console.log(`Error`)
                rej()
            });
    });
}

