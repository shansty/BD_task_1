import dotenv from "dotenv";
import { server, app } from "./server.js";
import { upload } from "./multer.js";
import { processCsvFile } from "./file_parcer.js";
import { insertData } from "./insert_data.js";
import('./server.js');

dotenv.config();

app.post("/import-csv", upload.single("file"), async (req, res) => {
    if (!req.file) {
        return res.status(400).json({ error: "No file uploaded" });
    }
    try {
        await processCsvFile(req.file.path, 1000, insertData)
        res.status(200).json({ message: "CSV data imported successfully" });
    } catch (error) {
        console.error("Error inserting batch:", error);
        res.status(500).json({ error: "Error processing CSV data" });
    }
});

//отдельный лог для пустых данных
//структура файла


server.listen(process.env.PORT, (error) => {
    if (error) {
        console.error("Error starting server:", error);
    } else {
        console.log(`Server is running at http://localhost:${process.env.PORT}`);
    }
});
