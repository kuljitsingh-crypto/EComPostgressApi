import 'dotenv/config';
import express from 'express';
import { addUUIDExtension } from './models/db.config';
import * as test from './models/test.model';
const PORT = parseInt(process.env.PORT || '3500', 10);
const app = express();
addUUIDExtension();

test.run();
app.get('/', (req, res) => {
  res.send('Hello World!');
});

app.listen(PORT, () => {
  console.log(`Server Listing on http://localhost:${PORT}`);
});
