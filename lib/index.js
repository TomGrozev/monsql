(function () {

    'use strict';
    const mysql = require('mysql');

    let con;

    // con.connect((err) => {
    //     if(err){
    //         console.log('Error connecting to DB');
    //         console.log(err);
    //         return;
    //     }
    //     console.log('Connection established to DB');
    // });
    //
    // con.end((err) => {
    //     // The connection is terminated gracefully
    //     // Ensures all previously enqueued queries are still
    //     // before sending a COM_QUIT packet to the MySQL server.
    // });

    function createSelectQuery(from, query, select, orderBy, join = {}) {
        let options = [];

        let sql = 'SELECT ';
        if (select && select.length > 0 && select !== '*') {
            let selects = [];
            for (let item of select) {
                if (typeof item === 'object' && Object.keys(item).length > 0) {
                    const key = Object.keys(item)[0];
                    selects.push('?? AS ?');
                    options.push(key, item[key]);
                } else {
                    selects.push('??');
                    options.push(item);
                }
            }
            sql += selects.join(', ');
        } else {
            sql += '*';
        }

        sql += ' FROM ??';
        options.push(from);

        if (Object.keys(join).length === 1) {
            let type = Object.keys(join)[0];
            join = join[type];
            if (Object.keys(join).length > 0) {
                for (let key in join) {
                    if (join.hasOwnProperty(key) && typeof join[key] !== 'undefined') {

                        if (Object.keys(join[key]).length > 0) {
                            sql += ' '+type+' JOIN ?? ON';
                            options.push(key);
                        }
                        let joins = [];
                        for (let oKey in join[key]) {
                            if (join[key].hasOwnProperty(oKey)) {
                                let val = join[key][oKey];
                                if (oKey.substr(0, 2) === '**') {
                                    oKey = oKey.substr(2);
                                    joins.push(' ?? = ?');
                                } else {
                                    joins.push(' ?? = ??');
                                }

                                options.push(oKey, val);
                            }
                        }
                        sql += joins.join(' AND');
                    }
                }
            }
        }

        if (Object.keys(query).length > 0) {
            sql += ' WHERE';

            let wheres = [];
            for (let key in query) {
                if (query.hasOwnProperty(key) && typeof query[key] !== 'undefined') {
                    const lastTwo = key.slice(key.length - 2, key.length);
                    const lastOne = key.slice(key.length - 1, key.length);

                    let finalKey = key;
                    switch (lastTwo) {
                        case '>=':
                            wheres.push(' ?? >= ?');
                            finalKey = key.splice(0, key.length-2);
                            break;

                        case '<=':
                            wheres.push(' ?? <= ?');
                            finalKey = key.splice(0, key.length-2);
                            break;

                        default:
                            switch (lastOne) {
                                case '>':
                                    wheres.push(' ?? > ?');
                                    finalKey = key.splice(0, key.length-1);
                                    break;

                                case '<':
                                    wheres.push(' ?? < ?');
                                    finalKey = key.splice(0, key.length-1);
                                    break;

                                case '=':
                                    wheres.push(' ?? = ?');
                                    finalKey = key.splice(0, key.length-1);
                                    break;

                                default:
                                    wheres.push(' ?? = ?');
                                    break;
                            }
                    }
                    options.push(key, query[key]);
                }
            }
            sql += wheres.join(' AND');
        }
        if (orderBy && Object.keys(orderBy).length === 1) {
            const key = Object.keys(orderBy)[0];
            if (orderBy[key] !== 'DESC') {
                orderBy[key] = 'ASC';
            }
            sql += ' ORDER BY ?? ' + orderBy[key];
            options.push(key);
        }
        return {sql: sql, options: options};
    }

    function find(from, query, select, orderBy, callback) {
        if (from.length === 0 || select.length === 0) {
            return;
        }

        const sqlQuery = createSelectQuery(from, query, select, orderBy);

        let options = sqlQuery.options;
        let sql = sqlQuery.sql;

        return con.query(sql, options, (err,rows) => {
            if(err) callback(err, null);

            callback(null, rows);
        });
    }

    function findOne(from, query, select, orderBy, callback) {
        if (from.length === 0 || select.length === 0) {
            return;
        }

        const sqlQuery = createSelectQuery(from, query, select, orderBy);

        let options = sqlQuery.options;
        let sql = sqlQuery.sql;
        sql += ' LIMIT 1';

        return con.query(sql, options, (err,rows) => {
            if(err || rows.length !== 1) {
                callback(err, null);
            } else {
                callback(null, rows[0]);
            }
        });
    }

    function findInnerJoin(from, query, select, join, orderBy, callback) {
        if (from.length === 0 || select.length === 0 || join.length === 0) {
            return;
        }

        const sqlQuery = createSelectQuery(from, query, select, orderBy, {'INNER': join});

        let options = sqlQuery.options;
        let sql = sqlQuery.sql;

        return con.query(sql, options, (err,rows) => {
            if(err || rows.length <= 0) {
                callback(err, null);
            } else {
                callback(null, rows);
            }
        });
    }

    function findLeftJoin(from, query, select, join, orderBy, callback) {
        if (from.length === 0 || select.length === 0 || join.length === 0) {
            return;
        }

        const sqlQuery = createSelectQuery(from, query, select, orderBy, {'LEFT': join});

        let options = sqlQuery.options;
        let sql = sqlQuery.sql;

        return con.query(sql, options, (err,rows) => {
            if(err || rows.length <= 0) {
                callback(err, null);
            } else {
                callback(null, rows);
            }
        });
    }


    exports.config = function (config, existing=false) {
        return function (req, res, next) {
            con = existing ? config : mysql.createPool(config);

            next();
        }
    };

    exports.db = class {


        constructor() {
            if (!con) {
                throw "MySQL Config needs to be set first."
            }
        }

// FIND //

        // FROM, SELECT, CALLBACK
        static find(from, select, callback) {
            find(from, {}, select, {}, callback);
        }

        // FROM, SELECT, WHERE, CALLBACK
        static findWhere(from, select, where, callback) {
            find(from, where, select, {}, callback);
        }

        // FROM, SELECT, ORDERBY, CALLBACK
        static findOrder(from, select, orderby, callback) {
            find(from, {}, select, orderby, callback);
        }

        // FROM, SELECT, WHERE, ORDERBY, CALLBACK
        static findWhereOrder(from, select, where, orderby, callback) {
            find(from, where, select, orderby, callback);
        }

        /////////



        // FIND ONE //

        // FROM, SELECT, CALLBACK
        static findOne(from, select, callback) {
            findOne(from, {}, select, {}, callback);
        }

        // FROM, SELECT, WHERE, CALLBACK
        static findOneWhere(from, select, where, callback) {
            findOne(from, where, select, {}, callback);
        }

        // FROM, SELECT, ORDERBY, CALLBACK
        static findOneOrder(from, select, orderBy, callback) {
            findOne(from, {}, select, orderBy, callback);
        }

        // FROM, SELECT, WHERE, ORDERBY, CALLBACK
        static findOneWhereOrder(from, select, where, orderBy, callback) {
            findOne(from, where, select, orderBy, callback);
        }

        //////////////



        // FIND INNER JOIN //

        // FROM, SELECT, JOIN, CALLBACK
        static findInnerJoin(from, select, join, callback) {
            findInnerJoin(from, {}, select, join, {}, callback);
        }

        // FROM, SELECT, WHERE, JOIN, CALLBACK
        static findInnerJoinWhere(from, select, where, join, callback) {
            findInnerJoin(from, where, select, join, {}, callback);
        }

        // FROM, SELECT, JOIN, ORDERBY, CALLBACK
        static findInnerJoinOrder(from, select, join, orderby, callback) {
            findInnerJoin(from, {}, select, join, orderby, callback);
        }

        // FROM, SELECT, WHERE, JOIN, ORDERBY, CALLBACK
        static findInnerJoinWhereOrder(from, select, where, join, orderby, callback) {
            findInnerJoin(from, where, select, join, orderby, callback);
        }

        ////////////////////



        // FIND LEFT JOIN //

        // FROM, SELECT, JOIN, CALLBACK
        static findLeftJoin(from, select, join, callback) {
            findLeftJoin(from, {}, select, join, {}, callback);
        }

        // FROM, SELECT, WHERE, JOIN, CALLBACK
        static findLeftJoinWhere(from, select, where, join, callback) {
            findLeftJoin(from, where, select, join, {}, callback);
        }

        // FROM, SELECT, JOIN, ORDERBY, CALLBACK
        static findLeftJoinOrder(from, select, join, orderby, callback) {
            findLeftJoin(from, {}, select, join, orderby, callback);
        }

        // FROM, SELECT, WHERE, JOIN, ORDERBY, CALLBACK
        static findLeftJoinWhereOrder(from, select, where, join, orderby, callback) {
            findLeftJoin(from, where, select, join, orderby, callback);
        }

        ////////////////////


        // UPDATE //

        // TABLE, UPDATES, WHERE, CALLBACK
        static update(table, updates, query, callback) {
            if (table.length === 0 || updates.length === 0 || query.length === 0) {
                return;
            }

            let options = [table];

            let sql = 'UPDATE ?? SET ';
            let sets = [];
            for (let key in updates) {
                if (updates.hasOwnProperty(key) && typeof updates[key] !== 'undefined') {
                    sets.push(' ?? = ?');
                    options.push(key, updates[key]);
                }
            }
            sql += sets.join(',');

            if (Object.keys(query).length > 0) {
                sql += ' WHERE';
            }
            for (let key in query) {
                if (query.hasOwnProperty(key) && typeof query[key] !== 'undefined') {
                    sql += ' ?? = ?';
                    options.push(key, query[key]);
                }
            }

            return con.query(sql, options, (err,rows) => {
                if(err || rows.length > 0) {
                    callback(err, null);
                } else {
                    callback(null, rows);
                }
            });
        }

        ////////////



        // CREATE //

        // TABLE, DATA, CALLBACK
        static create(table, data, callback) {
            if (table.length === 0 || data.length === 0) {
                return;
            }

            let options = [table];

            let sql = 'INSERT INTO ?? (';

            let sets = [];
            let setValues = [];

            if (!Array.isArray(data)) {
                data = [data];
            }

            let allValueKeys = [];
            let allValues = [];

            for (let row of data) {
                let values = [];

                for (let key in row) {
                    if (row.hasOwnProperty(key) && typeof row[key] !== 'undefined') {
                        if (setValues.indexOf(key) === -1) {
                            sets.push('??');
                            setValues.push(key);
                        }

                        values.push('?');
                        allValues.push(row[key]);
                    }
                }

                allValueKeys.push(values);
            }
            sql += sets.join(', ');
            sql += ') VALUES ';
            let vals = [];
            for (let row of allValueKeys) {
                vals.push('(' + row.join(', ') + ')');
            }
            sql += vals.join(', ');

            options = options.concat(setValues);
            options = options.concat(allValues);

            return con.query(sql, options, (err,rows) => {
                if(err || rows.length > 0) {
                    callback(err, null);
                } else {
                    callback(null, rows);
                }
            });
        }

        // CREATE UPDATE //

        // TABLE, DATA, CALLBACK
        static createUpdate(table, data, callback) {
            if (table.length === 0 || data.length === 0) {
                return;
            }

            let options = [table];

            let sql = 'INSERT INTO ?? (';

            let sets = [];
            let setValues = [];

            if (!Array.isArray(data)) {
                data = [data];
            }

            let allValueKeys = [];
            let allValues = [];
            let updatesVals = [];

            for (let row of data) {
                let values = [];

                for (let key in row) {
                    if (row.hasOwnProperty(key) && typeof row[key] !== 'undefined') {
                        if (setValues.indexOf(key) === -1) {
                            sets.push('??');
                            setValues.push(key);
                        }
                        updatesVals.push(key);

                        values.push('?');
                        allValues.push(row[key]);
                    }
                }

                allValueKeys.push(values);
            }
            sql += sets.join(', ');
            sql += ') VALUES ';
            let vals = [];
            for (let row of allValueKeys) {
                vals.push('(' + row.join(', ') + ')');
            }
            sql += vals.join(', ');

            options = options.concat(setValues);
            options = options.concat(allValues);

            if (updatesVals.length > 0) {
                sql += ' ON DUPLICATE KEY UPDATE ';

                let updates = [];
                for (let key of updatesVals) {
                    updates.push('?? = VALUES(??)');
                    options.push(key, key);
                }
                sql += updates.join(', ');
            }

            return con.query(sql, options, (err,rows) => {
                if(err || rows.length > 0) {
                    callback(err, null);
                } else {
                    callback(null, rows);
                }
            });
        }

        //////////////


        // REMOVE //

        // TABLE, WHERE, CALLBACK
        static remove(table, query, callback) {
            if (table.length === 0 || query.length === 0) {
                return;
            }

            let options = [table];

            var sql = 'DELETE FROM ?? ';

            if (Object.keys(query).length > 0) {
                sql += ' WHERE';
            }
            for (let key in query) {
                if (query.hasOwnProperty(key) && typeof query[key] !== 'undefined') {
                    sql += ' ?? = ?';
                    options.push(key, query[key]);
                }
            }

            return con.query(sql, options, (err,rows) => {
                if(err || rows.length > 0) {
                    callback(err, null);
                } else {
                    callback(null, rows);
                }
            });
        }

        //////////////
    }

}());

