import multer from 'multer';

const UPLOADS_FOLDER = process.env.UPLOADS_FOLDER || "./uploads";

const storage = multer.diskStorage({
    destination: (req, file, cb) => {
        cb(null, UPLOADS_FOLDER);
    },
    filename: (req, file, cb) => {
        cb(null, `${file.originalname}-${Date.now()}`);
    },
});


export const upload = multer({ storage });