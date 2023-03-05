const { DATABASE_SCHEMA, DATABASE_URL, SHOW_PG_MONITOR } = require('./config');
const massive = require('massive');
const monitor = require('pg-monitor');
const axios = require('axios');
const urlApi = 'https://datausa.io/api/data?drilldowns=Nation&measures=Population';

// Call start
(async () => {
    console.log('main.js: before start');

    const db = await massive({
        connectionString: DATABASE_URL,
        ssl: { rejectUnauthorized: false },
    }, {
        // Massive Configuration
        scripts: process.cwd() + '/migration',
        allowedSchemas: [DATABASE_SCHEMA],
        whitelist: [`${DATABASE_SCHEMA}.%`],
        excludeFunctions: true,
    }, {
        // Driver Configuration
        noWarnings: true,
        error: function (err, client) {
            console.log(err);
            //process.emit('uncaughtException', err);
            //throw err;
        }
    });

    if (!monitor.isAttached() && SHOW_PG_MONITOR === 'true') {
        monitor.attach(db.driverConfig);
    }

    const execFileSql = async (schema, type) => {
        return new Promise(async resolve => {
            const objects = db['user'][type];

            if (objects) {
                for (const [key, func] of Object.entries(objects)) {
                    console.log(`executing ${schema} ${type} ${key}...`);
                    await func({
                        schema: DATABASE_SCHEMA,
                    });
                }
            }

            resolve();
        });
    };

    //public
    const migrationUp = async () => {
        return new Promise(async resolve => {
            await execFileSql(DATABASE_SCHEMA, 'schema');

            //cria as estruturas necessarias no db (schema)
            await execFileSql(DATABASE_SCHEMA, 'table');
            await execFileSql(DATABASE_SCHEMA, 'view');

            console.log(`reload schemas ...`)
            await db.reload();

            resolve();
        });
    };

    try {
        await migrationUp();

        // const requestData = async () => {
        //     const { data: { data } } = await axios.get(urlApi);
        //     console.log(data);
        //     return data;
        // };

        // const data = await requestData();

        const data = [{"ID Nation":"01000US","Nation":"United States","ID Year":2020,"Year":"2020","Population":326569308,"Slug Nation":"united-states"},
        {"ID Nation":"01000US","Nation":"United States","ID Year":2019,"Year":"2019","Population":324697795,"Slug Nation":"united-states"},
        {"ID Nation":"01000US","Nation":"United States","ID Year":2018,"Year":"2018","Population":322903030,"Slug Nation":"united-states"},
        {"ID Nation":"01000US","Nation":"United States","ID Year":2017,"Year":"2017","Population":321004407,"Slug Nation":"united-states"},
        {"ID Nation":"01000US","Nation":"United States","ID Year":2016,"Year":"2016","Population":318558162,"Slug Nation":"united-states"},
        {"ID Nation":"01000US","Nation":"United States","ID Year":2015,"Year":"2015","Population":316515021,"Slug Nation":"united-states"},
        {"ID Nation":"01000US","Nation":"United States","ID Year":2014,"Year":"2014","Population":314107084,"Slug Nation":"united-states"},
        {"ID Nation":"01000US","Nation":"United States","ID Year":2013,"Year":"2013","Population":311536594,"Slug Nation":"united-states"}];

        //exemplo de insert
        data.map(async (d) => {
            await db[DATABASE_SCHEMA].api_data.insert({
                doc_record: d,
            });
        });

        //exemplo select
        const sumPopulation = () => {
            let totalPopulation = 0;
            data.map((d) => {
                if (d.Year >= 2018 && d.Year <= 2020) {
                    totalPopulation += d.Population;
                };
            });

            return totalPopulation.toLocaleString('pt-BR');
        }

        const sumPopulationSql = async () => {
            const [{ sum_population }] = await db.query(`
            SELECT SUM((doc_record->> 'Population')::int) as sum_population
            FROM italomoraess.api_data
            WHERE doc_record->>'Year' IN ('2018', '2019', '2020')
            `);

            return Number(sum_population).toLocaleString('pt-BR');
        }
        
        console.log("Total Population JS =>", await sumPopulation())
        console.log("Total Population SQL =>", await sumPopulationSql())

        //await db[DATABASE_SCHEMA].api_data.destroy({});

    } catch (e) {
        console.log(e.message)
    } finally {
        console.log('finally');
    }
    console.log('main.js: after start');
})();