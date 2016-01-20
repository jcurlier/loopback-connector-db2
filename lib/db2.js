
var ibmdb = require('ibm_db');

var SqlConnector = require('loopback-connector').SqlConnector;
var ParameterizedSQL = SqlConnector.ParameterizedSQL;

var debug = require('debug')('loopback:connector:db2');

/**
 * Initialize the DB2 connector against the given data source
 *
 * @param {DataSource} dataSource The loopback-datasource-juggler dataSource
 * @param {Function} [callback] The callback function
 */
exports.initialize = function initializeDataSource(dataSource, callback) {
  var s = dataSource.settings;
  var db2_settings = {
    host: s.host || s.hostname || 'localhost',
    port: s.port || 50000,
    database: s.database || 'SAMPLE',
    schema: s.schema || 'DB2ADMIN',
    user: s.username || s.user,
    password: s.password
  };
    
  dataSource.connector = new db2(ibmdb, db2_settings);
  dataSource.connector.dataSource = dataSource;
    
  callback && callback();
};

/**
* Constructor for DB2 connector
* @param {Driver} driver The IBM driver
* @param {Object} settings The settings object
* instance
* @constructor
*/
function db2(ibmdb, settings) {
  var self = this;
    
  SqlConnector.call(this, 'db2', settings);

  this.connectionString = "DRIVER={DB2};DATABASE=" + settings.database +        
    ";CurrentSchema=" + settings.schema + ";HOSTNAME=" + settings.host + 
    ";UID=" + settings.user + ";PWD=" + settings.password + 
    ";PORT=" + settings.port + ";PROTOCOL=TCPIP";

  var Pool = ibmdb.Pool;
  this.pool = new Pool();
    
  if (debug.enabled) {
    debug('Settings %j', settings);
  }
}

require('util').inherits(db2, SqlConnector);

/**
 * Execute the sql statement
 *
 * @param {String} sql The SQL statement
 * @param {Function} [callback] The callback after the SQL statement is executed
 */
db2.prototype.executeSQL = function(sql, params, options, callback) {
  var self = this;
    
  if (typeof callback !== 'function') {
    throw new Error('callback should be a function');
  }
    
  this.pool.open(this.connectionString, function (err, connection) {
      if (err) {
        callback && callback(err, {});
      } else {
      
        console.log('SQL: %s, params: %j', sql, params);
    
        connection.query(sql, function (err, data) {
          connection.close(function (err) {
              callback && callback(err, data);
          });
        });
      }
    }); 
};

db2.prototype.escapeName = function (name) {
  return name;
};

db2.prototype.fromColumnValue = function(prop, val) {
  if (val == null) {
    return val;
  }
  if (prop) {
    switch (prop.type.name) {
      case 'Number':
        val = Number(val);
        break;
      case 'String':
        val = String(val);
        break;
      case 'Date':
        val = new Date(val.toString().replace(/GMT.*$/, 'GMT'));
        break;
      case 'Boolean':
        val = Boolean(val);
        break;
      default:
        if (!Array.isArray(prop.type) && !prop.type.modelName) {
          // Do not convert array and model types
          val = prop.type(val);
        }
        break;
    }
  }
  return val;
};

db2.prototype.ping = function (cb) {
  this.execute('SELECT 1 FROM sysibm.sysdummy1', [], cb);
};