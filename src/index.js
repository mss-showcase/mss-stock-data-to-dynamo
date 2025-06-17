// index.js
import { S3Client, GetObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3';
import { DynamoDBClient, GetItemCommand, PutItemCommand, BatchWriteItemCommand } from '@aws-sdk/client-dynamodb';
import { marshall } from '@aws-sdk/util-dynamodb';
import zlib from 'zlib';

const s3 = new S3Client();
const dynamodb = new DynamoDBClient();

const FILES_TABLE = process.env.FILES_TABLE;
const TICKS_TABLE = process.env.TICKS_TABLE;
const FUNDAMENTALS_TABLE = process.env.FUNDAMENTALS_TABLE;

export const handler = async (event) => {
  for (const record of event.Records) {
    const bucket = record.s3.bucket.name;
    const key = decodeURIComponent(record.s3.object.key.replace(/\+/g, ' '));

    if (/^magnificent7-ticks-.*\.json\.gz$/.test(key)) {
      await processTicksFile(bucket, key);
    } else if (/^magnificent7-fundamentals-.*\.json\.gz$/.test(key)) {
      await processFundamentalsFile(bucket, key);
    }
    // else: ignore other files
  }
  return { statusCode: 200 };
};

async function processFundamentalsFile(bucket, key) {
  // Download and decompress
  const s3obj = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
  const body = await streamToBuffer(s3obj.Body);
  const jsonStr = zlib.gunzipSync(body).toString('utf-8');
  const data = JSON.parse(jsonStr);

  // Extract as_of timestamp from filename (ISO format)
  // Example: magnificent7-fundamentals-2025-06-17T21_36_09.840Z.json.gz
  const match = key.match(/^magnificent7-fundamentals-(.+)\.json\.gz$/);
  const as_of = match ? match[1].replace(/_/g, ':').replace(/T(\d{2}):(\d{2}):(\d{2})\.(\d{3})Z/, (m, h, m2, s, ms) => `T${h}:${m2}:${s}.${ms}Z`) : new Date().toISOString();

  const writes = [];
  for (const symbol of Object.keys(data)) {
    const fundamentals = data[symbol];
    // Compose item with symbol, as_of, all fields, and 30 days TTL
    writes.push({
      PutRequest: {
        Item: marshall({
          symbol,
          as_of,
          ...fundamentals,
          ttl: Math.floor(Date.now() / 1000) + 30 * 24 * 60 * 60 // 30 days from now
        })
      }
    });
  }

  // Batch write (25 items per batch)
  let i = 0;
  while (i < writes.length) {
    const batch = writes.slice(i, i + 25);
    await dynamodb.send(new BatchWriteItemCommand({
      RequestItems: { [FUNDAMENTALS_TABLE]: batch }
    }));
    i += 25;
  }
}


async function processTicksFile(bucket, key) {
  // Check if file already imported
  const fileCheck = await dynamodb.send(new GetItemCommand({
    TableName: FILES_TABLE,
    Key: marshall({ file_name: key })
  }));
  if (fileCheck.Item) return;

  // Download and decompress
  const s3obj = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
  const body = await streamToBuffer(s3obj.Body);
  const jsonStr = zlib.gunzipSync(body).toString('utf-8');
  const data = JSON.parse(jsonStr);

  const tickWrites = [];
  for (const symbol of Object.keys(data)) {
    // Find the time series key dynamically
    const tsKey = Object.keys(data[symbol]).find(k => k.startsWith('Time Series'));
    if (!tsKey) continue;
    const timeSeries = data[symbol][tsKey];

    // Calculate interval in minutes if possible
    let interval = null;
    const timestamps = Object.keys(timeSeries).sort();
    if (timestamps.length >= 2) {
      const t1 = new Date(timestamps[0]);
      const t2 = new Date(timestamps[1]);
      interval = Math.abs((t2 - t1) / (1000 * 60)); // in minutes
    }

    for (const timestamp of Object.keys(timeSeries)) {
      // Check if tick already exists
      const tickCheck = await dynamodb.send(new GetItemCommand({
        TableName: TICKS_TABLE,
        Key: marshall({ symbol, timestamp })
      }));
      if (tickCheck.Item) continue;

      const tick = timeSeries[timestamp];
      tickWrites.push({
        PutRequest: {
          Item: marshall({
            symbol,
            timestamp,
            open: tick['1. open'] ? Number(tick['1. open']) : null,
            high: tick['2. high'] ? Number(tick['2. high']) : null,
            low: tick['3. low'] ? Number(tick['3. low']) : null,
            close: tick['4. close'] ? Number(tick['4. close']) : null,
            volume: tick['5. volume'] ? Number(tick['5. volume']) : null,
            interval,
            file_name: key,
            ttl: Math.floor(Date.now() / 1000) + 30 * 24 * 60 * 60 // 30 days from now
          })
        }
      });
    }
  }

  // Batch write (25 items per batch)
  while (tickWrites.length) {
    const batch = tickWrites.splice(0, 25);
    await dynamodb.send(new BatchWriteItemCommand({
      RequestItems: { [TICKS_TABLE]: batch }
    }));
  }

  // Mark file as imported
  await dynamodb.send(new PutItemCommand({
    TableName: FILES_TABLE,
    Item: marshall({
      file_name: key,
      imported_at: new Date().toISOString(),
      ttl: Math.floor(Date.now() / 1000) + 30 * 24 * 60 * 60 // 30 days from now
    })
  }));

  // Delete file from S3
  await s3.send(new DeleteObjectCommand({
    Bucket: bucket,
    Key: key
  }));
}

// Helper to convert stream to buffer
function streamToBuffer(stream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    stream.on('data', (chunk) => chunks.push(chunk));
    stream.on('end', () => resolve(Buffer.concat(chunks)));
    stream.on('error', reject);
  });
}

