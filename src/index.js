// index.js
import { S3Client, GetObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3';
import { DynamoDBClient, GetItemCommand, PutItemCommand, BatchWriteItemCommand } from '@aws-sdk/client-dynamodb';
import { unmarshall, marshall } from '@aws-sdk/util-dynamodb';
import zlib from 'zlib';

const s3 = new S3Client();
const dynamodb = new DynamoDBClient();

const FILES_TABLE = process.env.FILES_TABLE;
const TICKS_TABLE = process.env.TICKS_TABLE;

export const handler = async (event) => {
  for (const record of event.Records) {
    const bucket = record.s3.bucket.name;
    const key = decodeURIComponent(record.s3.object.key.replace(/\+/g, ' '));
    if (!/^magnificent7-.*\.json\.gz$/.test(key)) continue;

    // Check if file already imported
    const fileCheck = await dynamodb.send(new GetItemCommand({
      TableName: FILES_TABLE,
      Key: marshall({ file_name: key })
    }));
    if (fileCheck.Item) continue;

    // Download and decompress
    const s3obj = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
    const body = await streamToBuffer(s3obj.Body);
    const jsonStr = zlib.gunzipSync(body).toString('utf-8');
    const data = JSON.parse(jsonStr);

    const tickWrites = [];
    for (const symbol of Object.keys(data)) {
      const meta = data[symbol]['Meta Data'];
      const interval = meta['4. Interval'];
      const timeSeries = data[symbol][`Time Series (${interval})`];
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
              open: Number(tick['1. open']),
              high: Number(tick['2. high']),
              low: Number(tick['3. low']),
              close: Number(tick['4. close']),
              volume: Number(tick['5. volume']),
              interval,
              file_name: key
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
      Item: marshall({ file_name: key, imported_at: new Date().toISOString() })
    }));

    // Delete file from S3
    await s3.send(new DeleteObjectCommand({
      Bucket: bucket,
      Key: key
    }));
  }
  return { statusCode: 200 };
};

// Helper to convert stream to buffer
function streamToBuffer(stream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    stream.on('data', (chunk) => chunks.push(chunk));
    stream.on('end', () => resolve(Buffer.concat(chunks)));
    stream.on('error', reject);
  });
}

