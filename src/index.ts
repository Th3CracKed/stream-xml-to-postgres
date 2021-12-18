import { Pool, PoolClient } from 'pg';
import { CONFIG } from './config';
const pool = new Pool({
    host: CONFIG.db.host,
    port: parseInt(<string>CONFIG.db.port, 10),
    user: CONFIG.db.user,
    password: CONFIG.db.password,
    database: CONFIG.db.database,
});
import fs from 'fs';
import { from as copyFrom } from 'pg-copy-streams';

import { createStream } from 'sax';

async function streamData({ table, fields }: { table: string, fields: string[] }) {
    const startTime = process.hrtime();
    pool.connect((err: Error, client: PoolClient, release: (release?: any) => void) => {
        const fieldsStringified = fields.reduce((acc, field, curentIndex) => acc + field.toLowerCase() + (curentIndex !== fields.length - 1 ? ',' : ''), '');
        const query = `COPY ${table} (${fieldsStringified}) FROM STDIN WITH DELIMITER ',' NULL AS ''`;
        // fs.writeFile(`query.txt`, `${query}`, (err) => {
        //     if (err) throw err;
        // });
        const dbStream = client.query(copyFrom(query))
            .on('error', error => {
                console.log('error', error);
                release();
                fs.appendFile(`stream_error_${table}.json`, `saxStream error | nbOfProcessedRows = ${nbOfProcessedRows} | ${JSON.stringify({ error })}`, (err) => {
                    if (err) throw err;
                });
            })
            .on('finish', () => {
                console.log('finished');
                release();
                const endTime = process.hrtime(startTime);
                console.log('Data transfer to database is done!');
                console.log('Execution time: %ds %dms', endTime[0], endTime[1] / 1000000);
            });

        const saxStream = createStream(true, {});
        let nbErrors: number = 0;
        let nbOfProcessedRows = 0;
        const readStream = fs.createReadStream(`data/${table}.xml`);
        saxStream.on('end', () => dbStream.end());
        saxStream.on("error", (error: Error) => console.log(error));
        dbStream.on("drain", () => {
            // console.log('drained');
            readStream.resume();
        })
        saxStream.on("opentag", async function (node) {
            nbOfProcessedRows++;
            if (node.name !== 'row') {
                return;
            }
            if (nbOfProcessedRows % 10000 === 0) {
                console.log(`Processing the ${nbOfProcessedRows} row, nb of nbErrors ${nbErrors}/${nbOfProcessedRows}`);
            }
            if (!node.attributes) {
                nbErrors++;
                fs.appendFile(`errors_${table}.json`, `${{ nbOfProcessedRows }} | Attributes undefined | ${JSON.stringify({ node })}`, (err) => {
                    if (err) throw err;
                });
                return;
            }
            const regexDelimiter = new RegExp(',', 'g');

            const row = fields.reduce((acc, field, currentIndex) => {
                const isLastField = currentIndex === fields.length - 1;
                const delimiter = isLastField ? '' : ',';
                const attributeValue = node.attributes[field];
                // if(!attributeValue && isLastField){
                //     return acc + ' '+ delimiter;
                // }else 
                if (!attributeValue) {
                    return acc + delimiter;
                }
                return acc + encodeURI((<string>attributeValue).replace(regexDelimiter, ' ')) + delimiter;
            }, '');
            // console.log("saxStream", node);
            try {
                // readStream.pause();
                // fs.appendFile(`written_row_${table}.txt`, `${row}\n----------------------------\n`, (err) => {
                //     if (err) throw err;
                // });
                const inserted = dbStream.write(`${row}\n`);
                if (!inserted) {
                    readStream.pause();
                    // console.log(`saxStream paused id = ${Id}`);
                    // console.log(`user inserted ${inserted}`, { Id, UserId, Name, Date, Class, TagBased });
                } else {
                    readStream.resume();
                }
            } catch (err) {
                fs.appendFile(`errors_${table}.json`, `${{ nbOfProcessedRows }} | Error while inserting user ${JSON.stringify({ err })} ${JSON.stringify({ node })}`, (err) => {
                    if (err) throw err;
                });
                nbErrors++;
                // console.log('user insertion error', JSON.stringify({ err }));   
            }
        });
        console.log(`Parsing ${table}.xml as a steam`);
        readStream.pipe(saxStream, { end: true });
    });
}

streamData({ table: 'Comments', fields: ['Id', 'PostId', 'Score', 'Text', 'CreationDate', 'UserId', 'ContentLicense', 'UserDisplayName'] })
    .catch((e) => {
        throw e
    })
    .finally(async () => {
        // disconnect pg
    })

streamData({ table: 'Votes', fields: ['Id', 'PostId', 'VoteTypeId', 'CreationDate'] })
    .catch((e) => {
        throw e
    })
    .finally(async () => {
        // disconnect pg
    })