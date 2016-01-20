
var ibmdb = require('ibm_db');

var SqlConnector = require('loopback-connector').SqlConnector;
var ParameterizedSQL = SqlConnector.ParameterizedSQL;

var debug = require('debug')('loopback:connector:db2');
var debugConnection = require('debug')('loopback:connector:db2:connection');

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

  if (debug.enabled) {
    debug('Settings %j', settings);
  }
  ibmdb.debug(debugConnection.enabled);

  this.connectionString = "DRIVER={DB2};DATABASE=" + settings.database +        
    ";CurrentSchema=" + settings.schema + ";HOSTNAME=" + settings.host + 
    ";UID=" + settings.user + ";PWD=" + settings.password + 
    ";PORT=" + settings.port + ";PROTOCOL=TCPIP";

  var Pool = ibmdb.Pool;
  this.pool = new Pool();
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

  if (debug.enabled) {
    if (params && params.length > 0) {
      debug('SQL: %s \nParameters: %j', sql, params);
    } else {
      debug('SQL: %s', sql);
    }
  }
    
  if (typeof callback !== 'function') {
    throw new Error('callback should be a function');
  }
    
  self.pool.open(this.connectionString, function (err, connection) {
      
    function handleResponse(connection, err, data) {
      connection.close(function (err) {
        callback && callback(err, data);
      });
    }
      
    if (err) {
      callback && callback(err, {});
    } else {
    
      if (params && params.length > 0) {
          connection.query(sql, params, function (err, data) {
            handleResponse(connection, err, data);
          });
      } else {
        connection.query(sql, function (err, data) {
          handleResponse(connection, err, data);
        });
      }
    }
  }); 
};

db2.prototype.applyPagination = function(model, stmt, filter) {
  var offset = filter.offset || filter.skip || 0;
    
  var paginatedSQL = 'SELECT * FROM ( SELECT ROW_NUMBER() OVER() AS rownum, tmp.* FROM (' + stmt.sql + ' ' + ') AS tmp ) ' 
    + ' WHERE rownum > ' + offset;

  if (filter.limit !== -1) {
    paginatedSQL += ' AND rownum <= ' + (offset + filter.limit);
  }

  stmt.sql = paginatedSQL + ' ';
    
  return stmt;
};

db2.prototype.escapeName = function (name) {
  return name;
};

db2.prototype.getPlaceholderForValue = function(key) {
  return '?';
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

db2.prototype.toColumnValue = function(prop, val) {
  if (val == null) {
      return null;
  }
  if (prop.type === String) {
    return String(val);
  }
  if (prop.type === Number) {
    if (isNaN(val)) {
      return val;
    }
    return val;
  }

  if (prop.type === Date || prop.type.name === 'Timestamp') {
    return dateToDB2(val, prop.type === Date);
  }

  // DB2 support char(1) Y/N
  if (prop.type === Boolean) {
    if (val) {
      return 'Y';
    } else {
      return 'N';
    }
  }

  return this.serializeObject(val);
};

db2.prototype.ping = function (cb) {
  this.execute('SELECT 1 FROM sysibm.sysdummy1', [], cb);
};

function dateToDB2(val, dateOnly) {
  function fz(v) {
    return v < 10 ? '0' + v : v;
  }

  function ms(v) {
    if (v < 10) {
      return '00' + v;
    } else if (v < 100) {
      return '0' + v;
    } else {
      return '' + v;
    }
  }

  var dateStr = [
    val.getUTCFullYear(),
    fz(val.getUTCMonth() + 1),
    fz(val.getUTCDate())
  ].join('-') + ' ' + [
    fz(val.getUTCHours()),
    fz(val.getUTCMinutes()),
    fz(val.getUTCSeconds())
  ].join(':');

  if (!dateOnly) {
    dateStr += '.' + ms(val.getMilliseconds());
  }

  if (dateOnly) {
    return new ParameterizedSQL(
      "to_date(?,'yyyy-mm-dd hh24:mi:ss')", [dateStr]);
  } else {
    return new ParameterizedSQL(
      "to_timestamp(?,'yyyy-mm-dd hh24:mi:ss.ff3')", [dateStr]);
  }
}