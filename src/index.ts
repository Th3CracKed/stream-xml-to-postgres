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

async function streamData({ table, fields }: { table: string, fields: { value: string, mandatory?: true, bool?: true, defaultValue?: number | string }[] }) {
    const startTime = process.hrtime();
    pool.connect((err: Error, client: PoolClient, release: (release?: any) => void) => {
        const fieldsStringified = fields.reduce((acc, field, currentIndex) => acc + field.value.toLowerCase() + (currentIndex !== fields.length - 1 ? ',' : ''), '');
        const query = `COPY ${table} (${fieldsStringified}) FROM STDIN NULL AS ''`;
        // fs.writeFile(`query.txt`, `${query}`, (err) => {
        //     if (err) throw err;
        // });
        const dbStream = client.query(copyFrom(query))
            .on('error', (error: any) => {
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
        let attributes: { [key: string]: string } = {};
        const readStream = fs.createReadStream(`data/${table}.xml`);
        saxStream.on('end', () => dbStream.end());
        saxStream.on("error", (error: Error) => console.log(error));
        dbStream.on("drain", () => {
            // console.log('drained');
            readStream.resume();
        });
        saxStream.on('attribute', (attribute) => {
            attributes[attribute.name] = attribute.value;
        });
        saxStream.on('closetag', () => {
            // console.log({ attributes })
            nbOfProcessedRows++;
            if (nbOfProcessedRows % 10000 === 0) {
                console.log(`Processing the ${nbOfProcessedRows} row, nb of nbErrors ${nbErrors}/${nbOfProcessedRows}`);
            }
            if (!attributes) {
                nbErrors++;
                fs.appendFile(`errors_${table}.json`, `${{ nbOfProcessedRows }} | Attributes undefined `, (err) => {
                    if (err) throw err;
                });
                return;
            }
            const regexDelimiter = new RegExp(',', 'g');
            let ignoreRow = false;
            const row = fields.reduce((acc, field, currentIndex) => {
                const isLastField = currentIndex === fields.length - 1;
                const delimiter = isLastField ? '' : '\t';
                if (field.bool) {
                    const attributeValue = attributes[field.value] === 'False' ? 0 : 1;
                    if (!attributeValue && field.mandatory) {
                        ignoreRow = true;
                    }
                    if (!attributeValue) {
                        return acc + field.defaultValue + delimiter;
                    }
                    return acc + attributeValue + delimiter;
                } else {
                    const attributeValue = attributes[field.value];
                    // if(!attributeValue && isLastField){
                    //     return acc + ' '+ delimiter;
                    // }else 
                    if (!attributeValue && field.mandatory) {
                        ignoreRow = true;
                    }
                    if (!attributeValue) {
                        return acc + field.defaultValue + delimiter;
                    }
                    return acc + encodeURI(attributeValue) + delimiter;
                }

            }, '');
            if (ignoreRow) {
                fs.appendFile(`errors_${table}.json`, `${{ nbOfProcessedRows }} | Attributes undefined `, (err) => {
                    if (err) throw err;
                });
                // console.log('Ignore row');
                return;
            }
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
                fs.appendFile(`errors_${table}.json`, `${{ nbOfProcessedRows }} | Error while inserting user ${JSON.stringify({ err })}`, (err) => {
                    if (err) throw err;
                });
                nbErrors++;
                // console.log('user insertion error', JSON.stringify({ err }));   
            }
            attributes = {};
            // console.log('closetag');
        });
        console.log(`Parsing ${table}.xml as a steam`);
        readStream.pipe(saxStream, { end: true });
    });
}
(async function () {
    // streamData({ table: 'Comments', fields: [{ value: 'Id', mandatory: true }, { value: 'PostId', }, { value: 'Score', }, { value: 'Text', }, { value: 'CreationDate', }, { value: 'UserId', }, { value: 'ContentLicense', }, { value: 'UserDisplayName' }] })
    //     .catch((e) => {
    //         fs.appendFile(`Comments_logs.txt`, `${JSON.stringify({ err: e })}`, (err) => {
    //             if (err) throw err;
    //         });
    //     })
    //     .finally(async () => {
    //         // disconnect pg
    //     });

    // streamData({ table: 'PostLinks', fields: [{ value: 'Id', mandatory: true }, { value: 'CreationDate' }, { value: 'PostId' }, { value: 'LinkTypeId' }] })
    //     .catch((e) => {
    //         fs.appendFile(`PostLinks_logs.txt`, `${JSON.stringify({ err: e })}`, (err) => {
    //             if (err) throw err;
    //         });
    //     })
    //     .finally(async () => {
    //         // disconnect pg
    //     })
    // streamData({ table: 'Badges', fields: [{ value: 'Id', mandatory: true }, { value: 'UserId' }, { value: 'Name' }, { value: 'Date' }, { value: 'Class' }, { value: 'TagBased', bool: true }] })
    //     .catch((e) => {
    //         fs.appendFile(`Badges_logs.txt`, `${JSON.stringify({ err: e })}`, (err) => {
    //             if (err) throw err;
    //         });
    //     })
    //     .finally(async () => {
    //         // disconnect pg
    //     })
    // streamData({
    //     table: 'Users',
    //     fields: [
    //         { value: 'Id', mandatory: true }, { value: 'Reputation', defaultValue: 0 }, { value: 'CreationDate' }, { value: 'DisplayName', defaultValue: '' }, { value: 'Views', defaultValue: 0 }, { value: 'WebsiteUrl', defaultValue: '' },
    //         { value: 'Location', defaultValue: '' },
    //         { value: 'AboutMe', defaultValue: '' }, { value: 'Age', defaultValue: 0 }, { value: 'UpVotes', defaultValue: 0 }, { value: 'DownVotes', defaultValue: 0 }, { value: 'EmailHash', defaultValue: '' }, { value: 'AccountId', defaultValue: '' }, { value: 'ProfileImageUrl', defaultValue: '' }
    //     ]
    // })
    //     .catch((e) => {
    //         fs.appendFile(`Users_logs.txt`, `${JSON.stringify({ err: e })}`, (err) => {
    //             if (err) throw err;
    //         });
    //     })
    //     .finally(async () => {
    //         // disconnect pg
    //     })
    streamData({
        table: 'Posts', fields: [{ value: 'Id', mandatory: true }, { value: 'PostTypeId', defaultValue: 0 }, { value: 'AcceptedAnswerId', defaultValue: 0 }, { value: 'ParentId', defaultValue: 0 }, { value: 'Score', defaultValue: 0 }, { value: 'ViewCount', defaultValue: 0 },
        { value: 'Body', defaultValue: '' }, { value: 'OwnerUserId', defaultValue: 0 }, { value: 'LastEditorUserId', defaultValue: 0 }, { value: 'LastEditDate', defaultValue: '' }, { value: 'LastActivityDate', defaultValue: '' }, { value: 'Title', defaultValue: '' }, { value: 'Tags', defaultValue: '' }, { value: 'AnswerCount', defaultValue: 0 },
        { value: 'CommentCount', defaultValue: 0 }, { value: 'FavoriteCount', defaultValue: 0 }, { value: 'CreationDate', defaultValue: '' }, { value: 'CommunityOwnedDate', defaultValue: '' },
        { value: 'ContentLicense', defaultValue: '' }, { value: 'LastEditorDisplayName', defaultValue: '' }, { value: 'OwnerDisplayName', defaultValue: '' }
        ]
    })
        .catch((e) => {
            fs.appendFile(`Users_logs.txt`, `${JSON.stringify({ err: e })}`, (err) => {
                if (err) throw err;
            });
        })
        .finally(async () => {
            // disconnect pg
        })
    // streamData({
    //     table: 'Votes', fields: [{ value: 'Id', mandatory: true }, { value: 'PostId' }, { value: 'VoteTypeId' }, { value: 'CreationDate' }]
    // })
    //     .catch((e) => {
    //         fs.appendFile(`Users_logs.txt`, `${JSON.stringify({ err: e })}`, (err) => {
    //             if (err) throw err;
    //         });
    //     })
    //     .finally(async () => {
    //         // disconnect pg
    //     })
}())

// streamData({ table: 'Votes', fields: ['Id', 'PostId', 'VoteTypeId', 'CreationDate'] })
//     .catch((e) => {
//         throw e
//     })
//     .finally(async () => {
//         // disconnect pg
//     })