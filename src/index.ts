import { Pool, PoolClient, QueryArrayConfig } from 'pg';
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

async function main() {
    const startTime = process.hrtime();
    pool.connect((err: Error, client: PoolClient, release: (release?: any) => void) => {
        const table = 'users';
        const dbStream = client.query(copyFrom(`COPY ${table} (id,reputation,creationdate,displayname,lastaccessdate,websiteurl,location,accountid,aboutme,views,upvotes,downvotes) FROM STDIN WITH DELIMITER ',' NULL AS ''`))
            .on('error', error => {
                console.log('error', error);
                release();
                errors.push(`saxStream error | ${{ nbOfProcessedRows }} | ${JSON.stringify({ error })}`);
            })
            .on('finish', () => {
                console.log('finished');
                release();
                const endTime = process.hrtime(startTime);
                console.log('Data transfer to database is done!');
                console.log('Execution time: %ds %dms', endTime[0], endTime[1] / 1000000)
                if (errors.length) {
                    fs.writeFileSync(`errors_${table}.json`, JSON.stringify(errors));
                }
            });

        const saxStream = createStream(true, {});
        const errors: string[] = [];
        let nbOfProcessedRows = 0;
        saxStream.on('end', () => dbStream.end());
        saxStream.on("error", (error: Error) => console.log(error));
        saxStream.on("opentag", async function (node) {
            nbOfProcessedRows++;
            if (node.name !== 'row') {
                return;
            }
            if (nbOfProcessedRows % 50 === 0) {
                console.log(`Processing the ${nbOfProcessedRows} row, nb of errors ${errors.length}/${nbOfProcessedRows}`);
            }
            if (!node.attributes) {
                errors.push(`${{ nbOfProcessedRows }} | Attributes undefined | ${JSON.stringify({ node })}`);
                return;
            }
            const { Id, Reputation, CreationDate, DisplayName, LastAccessDate, WebsiteUrl, Location, AboutMe, Views, UpVotes, DownVotes, AccountId } = node.attributes;
            if (isNaN(parseInt(<string>Id, 10)) || isNaN(parseInt(<string>Reputation, 10))) {
                errors.push(`${{ nbOfProcessedRows }} | Id or Reputation undefined | Id = ${Id} | Reputation = ${Reputation}`);
                return;
            }
            // console.log("saxStream", node);
            try {
                // console.log({ Id, Reputation, CreationDate, DisplayName, LastAccessDate, WebsiteUrl, Location, AboutMe, Views, UpVotes, DownVotes, AccountId });
                const inserted = dbStream.write(`${Id},${Reputation},${CreationDate},${escape(<string>DisplayName)},${LastAccessDate},${escape(<string>WebsiteUrl)},${escape(<string>Location)},${AccountId},${escape(<string>AboutMe)},${Views},${UpVotes},${DownVotes}\n`)
                if (!inserted) {
                    console.log(`user inserted ${inserted}`, { Id, Reputation, CreationDate, DisplayName, LastAccessDate, WebsiteUrl, Location, AboutMe, Views, UpVotes, DownVotes, AccountId });
                    errors.push(`${{ nbOfProcessedRows }} | Failed to insert user ${JSON.stringify({ err })} ${JSON.stringify({ node })}`);
                }
            } catch (err) {
                console.log('user insertion error', JSON.stringify({ err }));
                errors.push(`${{ nbOfProcessedRows }} | Error while inserting user ${JSON.stringify({ err })} ${JSON.stringify({ node })}`);
            }
        });
        console.log(`Parsing ${table}.xml as a steam`);
        fs.createReadStream(`data/${table}.xml`).pipe(saxStream, { end: true });
    });
}


main()
    .catch((e) => {
        throw e
    })
    .finally(async () => {
        // disconnect pg
    })