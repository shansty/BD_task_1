import { createServer } from 'node:http';
import express from 'express';
import pool from './db_pool.js';

export const app = express();
export const server = createServer(app);

const gracefulShutdown = async (signal) => {
    console.log(`Received ${signal}, shutting down...`);
    server.close(() => {
      console.log('Server closed.');
      pool.end().then(() => {
        console.log('Database pool closed.');
        process.exit(0);
      });
      console.log('Database pool closed.');
    });
  };


process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
