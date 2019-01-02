// Copyright IBM Corp. 2015,2018. All Rights Reserved.
// Node module: loopback-connector-mssql
// This file is licensed under the MIT License.
// License text available at https://opensource.org/licenses/MIT

'use strict';
var g = require('strong-globalize')();

var async = require('async');

module.exports = mixinMigration;

function mixinMigration(MsSQL) {
  MsSQL.prototype.showFields = function (model, cb) {
    var sql = 'select [COLUMN_NAME] as [Field], ' +
      ' [IS_NULLABLE] as [Null], [DATA_TYPE] as [Type],' +
      ' [CHARACTER_MAXIMUM_LENGTH] as [Length],' +
      ' [NUMERIC_PRECISION] as [Precision], NUMERIC_SCALE as [Scale]' +
      ' from INFORMATION_SCHEMA.COLUMNS' +
      ' where [TABLE_SCHEMA] = \'' + this.schema(model) + '\'' +
      ' and [TABLE_NAME] = \'' + this.table(model) + '\'' +
      ' order by [ORDINAL_POSITION]';
    this.execute(sql, function (err, fields) {
      if (err) {
        return cb && cb(err);
      } else {
        if (Array.isArray(fields)) {
          fields.forEach(function (f) {
            if (f.Length) {
              f.Type = f.Type + '(' + f.Length + ')';
            } else if (f.Precision) {
              f.Type = f.Type + '(' + f.Precision, +',' + f.Scale + ')';
            }
          });
        }
        cb && cb(err, fields);
      }
    });
  };

  MsSQL.prototype.showIndexes = function (model, cb) {
    var schema = "'" + this.schema(model) + "'";
    var table = "'" + this.table(model) + "'";
    var sql = 'SELECT OBJECT_SCHEMA_NAME(T.[object_id],DB_ID()) AS [table_schema],' +
      ' T.[name] AS [Table], I.[name] AS [Key_name], AC.[name] AS [Column_name],' +
      ' I.[type_desc], I.[is_unique], I.[data_space_id], I.[ignore_dup_key], I.[is_primary_key],' +
      ' I.[is_unique_constraint], I.[fill_factor], I.[is_padded], I.[is_disabled], I.[is_hypothetical],' +
      ' I.[allow_row_locks], I.[allow_page_locks], IC.[is_descending_key], IC.[is_included_column], IC.[key_ordinal] AS [Seq_in_index]' +
      ' FROM sys.[tables] AS T' +
      ' INNER JOIN sys.[indexes] I ON T.[object_id] = I.[object_id]' +
      ' INNER JOIN sys.[index_columns] IC ON I.[object_id] = IC.[object_id] AND IC.[index_id] = I.[index_id]' +
      ' INNER JOIN sys.[columns] AC ON T.[object_id] = AC.[object_id] AND IC.[column_id] = AC.[column_id]' +
      ' WHERE T.[is_ms_shipped] = 0 AND I.[type_desc] <> \'HEAP\'' +
      ' AND OBJECT_SCHEMA_NAME(T.[object_id],DB_ID()) = ' + schema + ' AND T.[name] = ' + table +
      ' ORDER BY T.[name], I.[index_id], IC.[key_ordinal]';

    this.execute(sql, function (err, fields) {
      cb && cb(err, fields);
    });
  };

  MsSQL.prototype.buildQueryForeignKeys = function (owner, table) {
    var sql =
      `SELECT
      FK.TABLE_SCHEMA AS "fkOwner", FK.CONSTRAINT_NAME AS "fkName", FK.TABLE_NAME AS "fkTableName",
      CU.COLUMN_NAME AS "fkColumnName", CU.ORDINAL_POSITION AS "keySeq",
      PK.TABLE_SCHEMA AS "pkOwner", 'PK' AS "pkName", 
      PK.TABLE_NAME AS "pkTableName", PT.COLUMN_NAME AS "pkColumnName"
    FROM
        INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS C
    INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS FK
        ON C.CONSTRAINT_NAME = FK.CONSTRAINT_NAME
    INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS PK
        ON C.UNIQUE_CONSTRAINT_NAME = PK.CONSTRAINT_NAME
    INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE CU
        ON C.CONSTRAINT_NAME = CU.CONSTRAINT_NAME
    INNER JOIN (
                SELECT
                    i1.TABLE_NAME,
                    i2.COLUMN_NAME
                FROM
                    INFORMATION_SCHEMA.TABLE_CONSTRAINTS i1
                INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE i2
                    ON i1.CONSTRAINT_NAME = i2.CONSTRAINT_NAME
                WHERE
                    i1.CONSTRAINT_TYPE = 'PRIMARY KEY'
               ) PT
        ON PT.TABLE_NAME = PK.TABLE_NAME
    WHERE FK.CONSTRAINT_TYPE = 'FOREIGN KEY'`;
    if (owner) {
      sql += ' AND FK.TABLE_SCHEMA=\'' + owner + '\'';
    }
    if (table) {
      sql += ' AND FK.TABLE_NAME=\'' + table + '\'';
    }
    return sql;
  }


  MsSQL.prototype.isActual = function (models, cb) {
    var ok = false;
    var self = this;

    if ((!cb) && ('function' === typeof models)) {
      cb = models;
      models = undefined;
    }
    // First argument is a model name
    if ('string' === typeof models) {
      models = [models];
    }

    models = models || Object.keys(this._models);
    async.each(models, function (model, done) {
      self.getTableStatus(model, function (err, fields, indexes) {
        self.discoverForeignKeys(self.table(model), {}, function (err, foreignKeys) {

          if (err) console.log('Failed to discover "' + self.table(model) +
            '" foreign keys', err);

          self.alterTable(model, fields, indexes, foreignKeys, function (err, needAlter) {
            if (err) {
              return done(err);
            } else {
              ok = ok || needAlter;
              done(err);
            }
          }, true);
        });
      });
    }, function (err) {
      if (err) {
        return err;
      }
      cb(null, !ok);
    });
  };

  MsSQL.prototype.getColumnsToAdd = function (model, actualFields) {
    var self = this;
    var m = self._models[model];
    var propNames = Object.keys(m.properties).filter(function (name) {
      return !!m.properties[name];
    });
    var idName = this.idName(model);

    var statements = [];
    var columnsToAdd = [];
    var columnsToAlter = [];

    // change/add new fields
    propNames.forEach(function (propName) {
      if (propName === idName) return;
      var found;
      var colName = expectedColNameForModel(propName, m);
      if (actualFields) {
        actualFields.forEach(function (f) {
          if (f.Field === colName) {
            found = f;
          }
        });
      }

      if (found) {
        actualize(propName, found);
      } else {
        columnsToAdd.push(self.columnEscaped(model, propName) +
          ' ' + self.propertySettingsSQL(model, propName));
      }
    });

    if (columnsToAdd.length) {
      statements.push('ADD ' + columnsToAdd.join(',' + MsSQL.newline));
    }

    if (columnsToAlter.length) {
      // SQL Server doesn't allow multiple columns to be altered in one statement
      columnsToAlter.forEach(function (c) {
        statements.push('ALTER COLUMN ' + c);
      });
    }

    function actualize(propName, oldSettings) {
      var newSettings = m.properties[propName];
      if (propName == 'label') {
        console.log('newSettings', newSettings.mssql);
        console.log('oldsettings', oldSettings);
      }
      if (newSettings && changed(newSettings, oldSettings)) {
        columnsToAlter.push(self.columnEscaped(model, propName) + ' ' +
          self.propertySettingsSQL(model, propName));
      }
    }

    function changed(newSettings, oldSettings) {
      if (oldSettings.Null === 'YES' &&
        (newSettings.required || newSettings.allowNull === false || newSettings.null === false || (newSettings.mssql && newSettings.mssql.nullable === 'N'))) {
        console.log('oldSettings.Null === YES')
        return true;
      }
      if (oldSettings.Null === 'NO' && !(newSettings.required || newSettings.allowNull === false || newSettings.null === false || (newSettings.mssql && newSettings.mssql.nullable === 'N'))) {
        console.log('oldSettings.Null === NO')
        return true;
      }
      if ((newSettings.mssql && (newSettings.mssql.dataLength && oldSettings.Length) && newSettings.mssql.dataLength !== oldSettings.Length)) {
        if (!(newSettings.mssql.dataLength === 'max' && oldSettings.Length === -1)) {
          console.log('newSettings.mssql.dataLength', newSettings.mssql.dataLength)
          console.log('oldSettings.Length', oldSettings.Length)
          return true;
        }
      }
      if ((oldSettings.Type.toUpperCase() !== datatype(newSettings)) && (newSettings.mssql && (oldSettings.Type.split('(')[0].toUpperCase() !== newSettings.mssql.dataType))) {
        return true;
      }
      return false;
    }
    return statements;
  };

  MsSQL.prototype.getColumnsToDrop = function (model, actualFields) {
    var self = this;
    var m = this._models[model];
    var propNames = Object.keys(m.properties).filter(function (name) {
      return !!m.properties[name];
    });
    var idName = this.idName(model);

    var statements = [];
    var columnsToDrop = [];

    if (actualFields) {
      // drop columns
      actualFields.forEach(function (f) {
        var colNames = propNames.map(function expectedColName(propName) {
          return expectedColNameForModel(propName, m);
        });
        var index = colNames.indexOf(f.Field);
        var propName = index >= 0 ? propNames[index] : f.Field;
        var notFound = !~index;

        if (m.properties[propName] && f.Field === idName) return;
        if (notFound || !m.properties[propName]) {
          columnsToDrop.push(self.columnEscaped(model, f.Field));
        }
      });

      if (columnsToDrop.length) {
        statements.push('DROP COLUMN' + columnsToDrop.join(',' + MsSQL.newline));
      }
    };
    return statements;
  };

  MsSQL.prototype.addIndexes = function (model, actualIndexes) {
    var self = this;
    var m = this._models[model];
    var idName = this.idName(model);
    var indexNames = m.settings.indexes ? Object.keys(m.settings.indexes).filter(function (name) {
      return !!m.settings.indexes[name];
    }) : [];
    var propNames = Object.keys(m.properties).filter(function (name) {
      return !!m.properties[name];
    });
    var ai = {};
    var sql = [];

    if (actualIndexes) {

      actualIndexes.forEach(function (i) {
        var name = i.Key_name;
        if (!ai[name]) {
          ai[name] = {
            info: i,
            columns: [],
          };
        }
        ai[name].columns[i.Seq_in_index - 1] = i.Column_name;
      });
    }

    var aiNames = Object.keys(ai);

    // remove indexes
    aiNames.forEach(function (indexName) {
      if (indexName.substr(0, 3) === 'PK_') {
        return;
      }

      if (indexNames.indexOf(indexName) === -1 && !m.properties[indexName] ||
        m.properties[indexName] && !m.properties[indexName].index) {
        sql.push('DROP INDEX ' + indexName + ' ON ' + self.tableEscaped(model));
      } else {
        // first: check single (only type and kind)
        if (m.properties[indexName] && !m.properties[indexName].index) {
          // TODO
          return;
        }
        // second: check multiple indexes
        var orderMatched = true;
        if (indexNames.indexOf(indexName) !== -1) {
          if (m.settings.indexes[indexName].columns) {
            // check if indexes are configured as "columns"
            m.settings.indexes[indexName].columns.split(/,\s*/).forEach(function (columnName, i) {
              if (ai[indexName].columns[i] !== columnName) {
                orderMatched = false;
              }
            });
          } else if (m.settings.indexes[indexName].keys) {
            // if indexes are configured as "keys"
            var index = 0;
            for (var key in m.settings.indexes[indexName].keys) {
              var sortOrder = m.settings.indexes[indexName].keys[key];
              if (ai[indexName].columns[index] !== key) {
                orderMatched = false;
                break;
              }
              index++;
            }
            // if number of columns differ between new and old index
            if (index !== ai[indexName].columns.length) {
              orderMatched = false;
            }
          }
        }
        if (!orderMatched) {
          sql.push('DROP INDEX ' + self.columnEscaped(model, indexName) + ' ON ' + self.tableEscaped(model));
          delete ai[indexName];
        }
      }
    });

    // add single-column indexes
    propNames.forEach(function (propName) {
      var found = ai[propName] && ai[propName].info;
      if (!found) {
        var tblName = self.tableEscaped(model);
        var i = m.properties[propName].index;
        if (!i) {
          return;
        }
        var type = 'ASC';
        var kind = 'NONCLUSTERED';
        var unique = false;
        if (i.type) {
          type = i.type;
        }
        if (i.kind) {
          kind = i.kind;
        }
        if (i.unique) {
          unique = true;
        }
        var colName = expectedColNameForModel(propName, m);
        // var pName = self.client.escapeId(colName);
        //var name = colName + '_' + kind + '_' + type + '_idx';
        var name = 'idx_' + colName;
        if (i.name) {
          name = i.name;
        }
        self._idxNames[model].push(name);
        var cmd = 'CREATE ' + (unique ? 'UNIQUE ' : '') + kind + ' INDEX [' + name + '] ON ' +
          tblName + MsSQL.newline;
        cmd += '(' + MsSQL.newline;
        cmd += '    [' + propName + '] ' + type;
        cmd += MsSQL.newline + ') WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE = OFF,' +
          ' SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ' +
          'ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON);' +
          MsSQL.newline;
        sql.push(cmd);
      }
    });

    // add multi-column indexes
    indexNames.forEach(function (indexName) {
      var found = ai[indexName] && ai[indexName].info;
      if (!found) {
        var tblName = self.tableEscaped(model);
        var i = m.settings.indexes[indexName];
        var type = 'ASC';
        var kind = 'NONCLUSTERED';
        var unique = false;
        if (i.type) {
          type = i.type;
        }
        if (i.kind) {
          kind = i.kind;
        }
        if (i.unique) {
          unique = true;
        }
        var splitcolumns = [];
        var columns = [];
        var name = '';

        // if indexes are configured as "keys"
        if (i.keys) {
          for (var key in i.keys) {
            splitcolumns.push(key);
          }
        } else if (i.columns) {
          splitcolumns = i.columns.split(',');
        }

        splitcolumns.forEach(function (elem, ind) {
          var trimmed = elem.trim();
          name += trimmed + '_';
          trimmed = '[' + trimmed + '] ' + type;
          columns.push(trimmed);
        });


        name = name.charAt(name.length - 1) === '_' ? name.slice(0, -1) : name;
        // name += kind + '_' + type + '_idx';

        var table = columns.length > 1 ? self.table(model) + '_' : '';

        name = 'idx_' + table + name;


        self._idxNames[model].push(name);

        var cmd = 'CREATE ' + (unique ? 'UNIQUE ' : '') + kind + ' INDEX [' + name + '] ON ' +
          tblName + MsSQL.newline;
        cmd += '(' + MsSQL.newline;
        cmd += columns.join(',' + MsSQL.newline);
        cmd += MsSQL.newline + ') WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE = OFF, ' +
          'SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ' +
          'ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON);' +
          MsSQL.newline;
        sql.push(cmd);
      }
    });
    return sql;
  };

  MsSQL.prototype.alterTable = function (model, actualFields, actualIndexes, actualFks, done, checkOnly) {
    if ('function' == typeof actualFks && typeof done !== 'function') {
      checkOnly = done || false;
      done = actualFks;
    }
    var self = this;
    var statements = [];
    async.series([
      function (cb) {
        statements = self.getAddModifyColumns(model, actualFields);
        cb();
      },
      function (cb) {
        statements = statements.concat(self.getDropColumns(model, actualFields));
        cb();
      },
      function (cb) {
        statements = statements.concat(self.addIndexes(model, actualIndexes));
        cb();
      },
      function (cb) {
        statements = statements.concat(self.dropForeignKeys(model, actualFks));
        cb();
      },
      function (cb) {
        // get foreign keys to add, but when we are checking only
        if (checkOnly) {
          statements = statements.concat(self.getForeignKeySQL(model, self.getModelDefinition(model).settings.foreignKeys, actualFks));
        }
        cb();
      }
    ], function (err, result) {
      if (err) done(err);

      // determine if there are column, index, or foreign keys changes (all require update)
      if (statements.length) {
        // get the required alter statements
        var alterStmt = self.getAlterStatement(model, statements);
        var stmtList = [alterStmt];

        // set up an object to pass back all changes, changes that have been run,
        // and foreign key statements that haven't been run
        var retValues = {
          statements: stmtList,
          query: stmtList.join('; '),
        };

        // if we're running in read only mode OR if the only changes are foreign keys additions,
        // then just return the object directly
        if (checkOnly) {
          done(null, true, retValues);
        } else {
          async.eachSeries(statements, function (query, fn) {
            self.applySqlChanges(model, [query], fn);
          }, function (err, results) {
            done(err, true, retValues);
          });
        }
      } else {
        done();
      }
    });
  };

  MsSQL.prototype.getAlterStatement = function (model, statements) {
    return statements.length ?
      'ALTER TABLE ' + this.tableEscaped(model) + ' ' + statements.join(',\n') :
      '';
  };


  MsSQL.prototype.propertiesSQL = function (model) {
    // debugger;
    var self = this;
    var objModel = this._models[model];
    var modelPKID = this.idName(model);

    var sql = [];
    var props = Object.keys(objModel.properties);
    for (var i = 0, n = props.length; i < n; i++) {
      var prop = props[i];
      if (prop === modelPKID) {
        var idProp = objModel.properties[modelPKID];
        if (idProp.type === Number) {
          if (idProp.generated !== false) {
            sql.push(self.columnEscaped(model, modelPKID) +
              ' ' + self.columnDataType(model, modelPKID) + ' IDENTITY(1,1) NOT NULL');
          } else {
            sql.push(self.columnEscaped(model, modelPKID) +
              ' ' + self.columnDataType(model, modelPKID) + ' NOT NULL');
          }
          continue;
        } else if (idProp.type === String) {
          if (idProp.generated !== false) {
            sql.push(self.columnEscaped(model, modelPKID) +
              ' [uniqueidentifier] DEFAULT newid() NOT NULL');
          } else {
            sql.push(self.columnEscaped(model, modelPKID) + ' ' +
              self.propertySettingsSQL(model, prop) + ' DEFAULT newid()');
          }
          continue;
        }
      }
      sql.push(self.columnEscaped(model, prop) + ' ' + self.propertySettingsSQL(model, prop));
    }
    var joinedSql = sql.join(',' + MsSQL.newline + '    ');
    var cmd = '';
    if (modelPKID) {
      cmd = 'PRIMARY KEY CLUSTERED' + MsSQL.newline + '(' + MsSQL.newline;
      cmd += ' ' + self.columnEscaped(model, modelPKID) + ' ASC' + MsSQL.newline;
      cmd += ') WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, ' +
        'IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON)';
    }

    joinedSql += ',' + MsSQL.newline + cmd;

    return joinedSql;
  };

  MsSQL.prototype.singleIndexSettingsSQL = function (model, prop, add) {
    // Recycled from alterTable single indexes above, more or less.
    var tblName = this.tableEscaped(model);
    var i = this._models[model].properties[prop].index;
    var type = 'ASC';
    var kind = 'NONCLUSTERED';
    var unique = false;
    if (i.type) {
      type = i.type;
    }
    if (i.kind) {
      kind = i.kind;
    }
    if (i.unique) {
      unique = true;
    }
    // var name = prop + '_' + kind + '_' + type + '_idx';
    var name = 'idx_' + prop;

    if (i.name) {
      name = i.name;
    }
    this._idxNames[model].push(name);
    var cmd = 'CREATE ' + (unique ? 'UNIQUE ' : '') + kind + ' INDEX [' + name + '] ON ' +
      tblName + MsSQL.newline;
    cmd += '(' + MsSQL.newline;
    cmd += '    [' + prop + '] ' + type;
    cmd += MsSQL.newline + ') WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE = OFF,' +
      ' SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ' +
      'ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON);' +
      MsSQL.newline;
    return cmd;
  };

  MsSQL.prototype.indexSettingsSQL = function (model, prop) {
    // Recycled from alterTable multi-column indexes above, more or less.
    var tblName = this.tableEscaped(model);
    var i = this._models[model].settings.indexes[prop];
    var type = 'ASC';
    var kind = 'NONCLUSTERED';
    var unique = false;
    if (i.type) {
      type = i.type;
    }
    if (i.kind) {
      kind = i.kind;
    }
    if (i.unique) {
      unique = true;
    }

    var splitcolumns = [];
    var columns = [];
    var name = '';
    // if indexes are configured as "keys"
    if (i.keys) {
      for (var key in i.keys) {
        splitcolumns.push(key);
      }
    } else if (i.columns) {
      splitcolumns = i.columns.split(',');
    }

    splitcolumns.forEach(function (elem, ind) {
      var trimmed = elem.trim();
      name += trimmed + '_';
      trimmed = '[' + trimmed + '] ' + type;
      columns.push(trimmed);
    });

    name = name.charAt(name.length - 1) === '_' ? name.slice(0, -1) : name;

    // name += kind + '_' + type + '_idx';

    var table = splitcolumns.length > 1 ? this.table(model) + '_' : '';

    name = 'idx_' + table + name;

    this._idxNames[model].push(name);

    var cmd = 'CREATE ' + (unique ? 'UNIQUE ' : '') + kind + ' INDEX [' + name + '] ON ' +
      tblName + MsSQL.newline;
    cmd += '(' + MsSQL.newline;
    cmd += columns.join(',' + MsSQL.newline);
    cmd += MsSQL.newline + ') WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE = OFF, ' +
      'SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ' +
      'ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON);' +
      MsSQL.newline;
    return cmd;
  };

  function isNullable(p) {
    if (p.mssql && p.mssql.columnName == 'identifier') {
    }
    return !(p.required || p.id || p.nullable === false ||
      p.allowNull === false || p['null'] === false || (p.mssql && p.mssql.nullable === 'N'));
  }

  MsSQL.prototype.propertySettingsSQL = function (model, prop) {
    var p = this._models[model].properties[prop];
    return this.columnDataType(model, prop) + ' ' +
      (isNullable(p) ? 'NULL' : 'NOT NULL');
  };


  /**
    * Perform autoupdate for the given models
    * @param {String[]} [models] A model name or an array of model names.
    * If not present, apply to all models
    * @param {Function} [cb] The callback function
    */
  MsSQL.prototype.autoupdate = function (models, cb) {
    var self = this;
    var foreignKeyStatements = [];

    if ((!cb) && ('function' === typeof models)) {
      cb = models;
      models = undefined;
    }
    // First argument is a model name
    if ('string' === typeof models) {
      models = [models];
    }

    models = models || Object.keys(this._models);

    async.eachSeries(models, function (model, done) {
      if (!(model in self._models)) {
        return process.nextTick(function () {
          done(new Error(g.f('Model not found: %s', model)));
        });
      }

      self.getTableStatus(model, function (err, fields, indexes) {
        self.discoverForeignKeys(self.table(model), {}, function (err, foreignKeys) {
          if (err) console.log('Failed to discover "' + self.table(model) +
            '" foreign keys', err);

          if (!err && fields && fields.length) {
            // if we already have a definition, update this table
            self.alterTable(model, fields, indexes, foreignKeys, function (err, result) {
              done(err);
            });
          } else {
            // if there is not yet a definition, create this table
            self.createTable(model, function (err) {
              done(err);
            });
          }
        });
      });
    }, function (err) {
      return cb(err);
    });
  };


  /**
    * Perform createForeignKeys for the given models
    * @param {String[]} [models] A model name or an array of model names.
    * If not present, apply to all models
    * @param {Function} [cb] The callback function
    */
  MsSQL.prototype.createForeignKeys = function (models, cb) {
    var self = this;

    if ((!cb) && ('function' === typeof models)) {
      cb = models;
      models = undefined;
    }
    // First argument is a model name
    if ('string' === typeof models) {
      models = [models];
    }

    models = models || Object.keys(this._models);

    async.eachSeries(models, function (model, done) {
      if (!(model in self._models)) {
        return process.nextTick(function () {
          done(new Error(g.f('Model not found: %s', model)));
        });
      }
      self.getTableStatus(model, function (err, fields, indexes) {
        self.discoverForeignKeys(self.table(model), {}, function (err, foreignKeys) {
          if (err) console.log('Failed to discover "' + self.table(model) +
            '" foreign keys', err);

          if (!err && fields && fields.length) {
            var fkSQL = self.getForeignKeySQL(model,
              self.getModelDefinition(model).settings.foreignKeys,
              foreignKeys);

            self.addForeignKeys(model, fkSQL, function (err, result) {
              done(err);
            });
          } else {
            done(err);
          }
        });
      });
    }, function (err) {
      return cb(err);
    });
  };


  MsSQL.prototype.automigrate = function (models, cb) {
    var self = this;
    if ((!cb) && ('function' === typeof models)) {
      cb = models;
      models = undefined;
    }
    // First argument is a model name
    if ('string' === typeof models) {
      models = [models];
    }

    models = models || Object.keys(this._models);
    async.each(models, function (model, done) {
      if (!(model in self._models)) {
        return process.nextTick(function () {
          done(new Error(g.f('Model not found: %s', model)));
        });
      }
      self.dropTable(model, function (err) {
        if (err) {
          return done(err);
        }
        self.createTable(model, done);
      });
    }, function (err) {
      cb && cb(err);
    });
  };

  MsSQL.prototype.dropTable = function (model, cb) {
    var tblName = this.tableEscaped(model);
    var cmd = "IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'" +
      tblName + "') AND type in (N'U'))";
    cmd += MsSQL.newline + 'BEGIN' + MsSQL.newline;
    cmd += '    DROP TABLE ' + tblName;
    cmd += MsSQL.newline + 'END';
    this.execute(cmd, cb);
  };

  MsSQL.prototype.createTable = function (model, cb) {
    var tblName = this.tableEscaped(model);
    var cmd = 'SET ANSI_NULLS ON;' + MsSQL.newline + 'SET QUOTED_IDENTIFIER ON;' +
      MsSQL.newline + 'SET ANSI_PADDING ON;' + MsSQL.newline;
    cmd += "IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'" +
      tblName + "') AND type in (N'U'))" + MsSQL.newline + 'BEGIN' + MsSQL.newline;
    cmd += 'CREATE TABLE ' + this.tableEscaped(model) + ' (';
    cmd += MsSQL.newline + '    ' + this.propertiesSQL(model) + MsSQL.newline;
    cmd += ')' + MsSQL.newline + 'END;' + MsSQL.newline;
    cmd += this.createIndexes(model);
    this.execute(cmd, cb);
  };

  MsSQL.prototype.createIndexes = function (model) {
    var self = this;
    var sql = [];
    // Declared in model index property indexes.
    Object.keys(this._models[model].properties).forEach(function (prop) {
      var i = self._models[model].properties[prop].index;
      if (i) {
        sql.push(self.singleIndexSettingsSQL(model, prop));
      }
    });

    // Settings might not have an indexes property.
    var dxs = this._models[model].settings.indexes;
    if (dxs) {
      Object.keys(this._models[model].settings.indexes).forEach(function (prop) {
        sql.push(self.indexSettingsSQL(model, prop));
      });
    }

    return sql.join(MsSQL.newline);
  };

  MsSQL.prototype.columnDataType = function (model, property) {
    var columnMetadata = this.columnMetadata(model, property);
    var colType = columnMetadata && columnMetadata.dataType;
    if (colType) {
      colType = colType.toUpperCase();
    }
    var prop = this._models[model].properties[property];
    if (!prop) {
      return null;
    }
    var colLength = columnMetadata && columnMetadata.dataLength || prop.length;
    if (colType) {
      var dataPrecision = columnMetadata.dataPrecision;
      var dataScale = columnMetadata.dataScale;
      if (dataPrecision && dataScale) {
        return colType + '(' + dataPrecision + ', ' + dataScale + ')';
      }
      return colType + (colLength ? '(' + colLength + ')' : '');
    }
    return datatype(prop);
  };

  function datatype(p) {
    var dt = '';
    switch (p.type.name) {
      default:
      case 'String':
      case 'JSON':
        dt = '[nvarchar](' + (p.length || p.limit || 255) + ')';
        break;
      case 'Text':
        dt = '[text]';
        break;
      case 'Number':
        dt = '[int]';
        break;
      case 'Date':
        dt = '[datetime]';
        break;
      case 'Boolean':
        dt = '[bit]';
        break;
      case 'Point':
        dt = '[float]';
        break;
      case 'Decimal':
        dt = '[decimal]';
        break;
    }
    return dt;
  }

  MsSQL.prototype.getForeignKeySQL = function (model, actualFks, existingFks) {
    var self = this;
    var m = this.getModelDefinition(model);
    var addFksSql = [];
    existingFks = existingFks || [];

    if (actualFks) {
      var keys = Object.keys(actualFks);
      for (var i = 0; i < keys.length; i++) {
        // all existing fks are already checked in MySQL.prototype.dropForeignKeys
        // so we need check only names - skip if found
        if (existingFks.filter(function (fk) {
          return fk.fkName === keys[i];
        }).length > 0) continue;
        var constraint = self.buildForeignKeyDefinition(model, keys[i]);
        if (constraint) {
          addFksSql.push('ADD ' + constraint);
        }
      }
    }
    return addFksSql;
  };

  MsSQL.prototype.buildForeignKeyDefinition = function (model, keyName) {
    var definition = this.getModelDefinition(model);
    var fk = definition.settings.foreignKeys[keyName];
    if (fk) {
      // get the definition of the referenced object
      var fkEntityName = (typeof fk.entity === 'object') ? fk.entity.name : fk.entity;

      // verify that the other model in the same DB
      if (this._models[fkEntityName]) {
        return ' CONSTRAINT ' + fk.name +
          ' FOREIGN KEY (' + expectedColNameForModel(fk.foreignKey, definition) + ')' +
          ' REFERENCES ' + this.tableEscaped(fkEntityName) +
          '(' + fk.entityKey + ')';
      }
    }
    return '';
  };

  MsSQL.prototype.addForeignKeys = function (model, fkSQL, cb) {
    var self = this;
    var m = this.getModelDefinition(model);

    if ((!cb) && ('function' === typeof fkSQL)) {
      cb = fkSQL;
      fkSQL = undefined;
    }

    if (!fkSQL) {
      var newFks = m.settings.foreignKeys;
      if (newFks)
        fkSQL = self.getForeignKeySQL(model, newFks);
    }
    if (fkSQL && fkSQL.length) {
      self.applySqlChangesforFKs(model, [fkSQL.toString()], function (err, result) {
        if (err) {
          cb(err);
        }
        else {
          cb(null, result);
        }
      });
    } else cb(null, {});
  };

  MsSQL.prototype.applySqlChangesforFKs = function (model, pendingChanges, cb) {
    var self = this;
    if (pendingChanges.length) {
      var alterTable = (pendingChanges[0].substring(0, 10) !== 'DROP INDEX' && pendingChanges[0].substring(0, 6) !== 'CREATE');
      var ranOnce = false;
      var thisQuery = '';

      pendingChanges.map(changes => {
        changes.split(',').forEach(function (change) {
          if (ranOnce) {
            thisQuery = thisQuery + ' ';
          }
          thisQuery = alterTable ? thisQuery + 'ALTER TABLE ' + self.tableEscaped(model) : '';
          thisQuery = thisQuery + ' ' + change;
          ranOnce = true;
        });
      });


      self.execute(thisQuery, cb);
    }
  }


  MsSQL.prototype.dropForeignKeys = function (model, actualFks) {
    var self = this;
    var m = this.getModelDefinition(model);

    var fks = actualFks;
    var sql = [];
    var correctFks = m.settings.foreignKeys || {};

    // drop foreign keys for removed fields
    if (fks && fks.length) {
      var removedFks = [];
      fks.forEach(function (fk) {
        var needsToDrop = false;
        var newFk = correctFks[fk.fkName];
        if (newFk) {
          var fkCol = expectedColNameForModel(newFk.foreignKey, m);
          var fkEntity = self.getModelDefinition(newFk.entity);
          var fkRefKey = expectedColNameForModel(newFk.entityKey, fkEntity);
          var fkEntityName = (typeof newFk.entity === 'object') ? newFk.entity.name : newFk.entity;
          var fkRefTable = self.table(fkEntityName);

          needsToDrop = fkCol != fk.fkColumnName ||
            fkRefKey != fk.pkColumnName ||
            fkRefTable != fk.pkTableName;
        } else {
          needsToDrop = true;
        }

        if (needsToDrop) {

          sql.push('DROP CONSTRAINT ' + fk.fkName);
          removedFks.push(fk); // keep track that we removed these
        }
      });

      // update out list of existing keys by removing dropped keys
      removedFks.forEach(function (k) {
        var index = actualFks.indexOf(k);
        if (index !== -1) actualFks.splice(index, 1);
      });
    }
    return sql;
  };

  function expectedColNameForModel(propName, modelToCheck) {

    var mssql = modelToCheck.properties[propName].mssql;
    if (typeof mssql === 'undefined') {
      return propName;
    }
    var colName = mssql.columnName;

    if (typeof colName === 'undefined') {
      return propName;
    }
    return colName;
  }
}
