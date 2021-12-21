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

async function streamData({ table, fields }: { table: string, fields: { value: string, mandatory?: true, bool?: true }[] }) {
    const startTime = process.hrtime();
    pool.connect((err: Error, client: PoolClient, release: (release?: any) => void) => {
        const fieldsStringified = fields.reduce((acc, field, currentIndex) => acc + field.value.toLowerCase() + (currentIndex !== fields.length - 1 ? ',' : ''), '');
        const query = `COPY ${table} (${fieldsStringified}) FROM STDIN WITH DELIMITER ',' NULL AS ''`;
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
                const delimiter = isLastField ? '' : ',';
                if (field.bool) {
                    const attributeValue = attributes[field.value] === 'False' ? 0 : 1;
                    if (!attributeValue && field.mandatory) {
                        ignoreRow = true;
                    }
                    if (!attributeValue) {
                        return acc + delimiter;
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
                        return acc + delimiter;
                    }
                    return acc + encodeURI((<string>attributeValue).replace(regexDelimiter, ' ')) + delimiter;
                }

            }, '');
            // console.log({ row });
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
    //     table: 'Users', fields: [{ value: 'Id', mandatory: true }, { value: 'Reputation' }, { value: 'CreationDate' }, { value: 'DisplayName' }, { value: 'Views' }, { value: 'WebsiteUrl' },
    //     { value: 'Location' }, { value: 'AboutMe' }, { value: 'Age' }, { value: 'UpVotes' }, { value: 'DownVotes' }, { value: 'EmailHash' }, { value: 'AccountId' }, { value: 'ProfileImageUrl' }
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
    // streamData({
    //     table: 'Posts', fields: [{ value: 'Id', mandatory: true }, { value: 'PostTypeId' }, { value: 'AcceptedAnswerId' }, { value: 'ParentId' }, { value: 'Score' }, { value: 'ViewCount' },
    //     { value: 'Body' }, { value: 'OwnerUserId' }, { value: 'LastEditorUserId' }, { value: 'LastEditDate' }, { value: 'LastActivityDate' }, { value: 'Title' }, { value: 'Tags' }, { value: 'AnswerCount' },
    //     { value: 'CommentCount' }, { value: 'FavoriteCount' }, { value: 'CreationDate' }, { value: 'CommunityOwnedDate' },
    //     { value: 'ContentLicense' }, { value: 'LastEditorDisplayName' }, { value: 'OwnerDisplayName' }
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
        table: 'Votes', fields: [{ value: 'Id', mandatory: true }, { value: 'PostId' }, { value: 'VoteTypeId' }, { value: 'CreationDate' }]
    })
        .catch((e) => {
            fs.appendFile(`Users_logs.txt`, `${JSON.stringify({ err: e })}`, (err) => {
                if (err) throw err;
            });
        })
        .finally(async () => {
            // disconnect pg
        })
}())

// streamData({ table: 'Votes', fields: ['Id', 'PostId', 'VoteTypeId', 'CreationDate'] })
//     .catch((e) => {
//         throw e
//     })
//     .finally(async () => {
//         // disconnect pg
//     })