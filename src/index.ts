import 'dotenv/config';
import express from 'express';
import { addUUIDExtension } from './models/db.config';
import * as models from './models';
const PORT = parseInt(process.env.PORT || '3500', 10);
const app = express();
addUUIDExtension();

app.get('/', (req, res) => {
  res.send('Hello World!');
});

app.listen(PORT, () => {
  console.log(`Server Listing on http://localhost:${PORT}`);
});
