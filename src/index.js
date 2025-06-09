// index.js
import AWS from 'aws-sdk';
import zlib from 'zlib';

const s3 = new AWS.S3();
const dynamodb = new AWS.DynamoDB.DocumentClient();

const FILES_TABLE = process.env.FILES_TABLE;
const TICKS_TABLE = process.env.TICKS_TABLE;

export const handler = async (event) => {
  for (const record of event.Records) {
    const bucket = record.s3.bucket.name;
    const key = decodeURIComponent(record.s3.object.key.replace(/\+/g, ' '));
    if (!/^magnificent7-.*\.json\.gz$/.test(key)) continue;

    // Check if file already imported
    const fileCheck = await dynamodb.get({
      TableName: FILES_TABLE,
      Key: { file_name: key }
    }).promise();
    if (fileCheck.Item) continue;

    // Download and decompress
    const s3obj = await s3.getObject({ Bucket: bucket, Key: key }).promise();
    const jsonStr = zlib.gunzipSync(s3obj.Body).toString('utf-8');
    const data = JSON.parse(jsonStr);

    const tickWrites = [];
    for (const symbol of Object.keys(data)) {
      const meta = data[symbol]['Meta Data'];
      const interval = meta['4. Interval'];
      const timeSeries = data[symbol][`Time Series (${interval})`];
      for (const timestamp of Object.keys(timeSeries)) {
        // Check if tick already exists
        const tickCheck = await dynamodb.get({
          TableName: TICKS_TABLE,
          Key: { symbol, timestamp }
        }).promise();
        if (tickCheck.Item) continue;

        const tick = timeSeries[timestamp];
        tickWrites.push({
          PutRequest: {
            Item: {
              symbol,
              timestamp,
              open: Number(tick['1. open']),
              high: Number(tick['2. high']),
              low: Number(tick['3. low']),
              close: Number(tick['4. close']),
              volume: Number(tick['5. volume']),
              interval,
              file_name: key
            }
          }
        });
      }
    }

    // Batch write (25 items per batch)
    while (tickWrites.length) {
      const batch = tickWrites.splice(0, 25);
      await dynamodb.batchWrite({
        RequestItems: { [TICKS_TABLE]: batch }
      }).promise();
    }

    // Mark file as imported
    await dynamodb.put({
      TableName: FILES_TABLE,
      Item: { file_name: key, imported_at: new Date().toISOString() }
    }).promise();

    // Delete file from S3
    await s3.deleteObject({
      Bucket: bucket,
      Key: key
    }).promise();
  }
  return { statusCode: 200 };
};

