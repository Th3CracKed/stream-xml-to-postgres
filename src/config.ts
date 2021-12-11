require('dotenv').config()
const env = process.env;

export const CONFIG = {
    db: { /* do not put password or any sensitive info here, done only for demo */
        host: env.DB_HOST,
        port: env.DB_PORT,
        user: env.DB_USER,
        password: env.DB_PASSWORD,
        database: env.DB_NAME,
    }
};
