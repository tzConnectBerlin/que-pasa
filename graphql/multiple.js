/**
 * Sourced from: https://github.com/graphile/postgraphile/issues/191#issuecomment-495055728
 */

const { makeAddInflectorsPlugin } = require("graphile-utils");
const lodash = require("lodash");

const MakeSchemaPrefixPlugin = makeAddInflectorsPlugin(
  {
    _functionName(proc) {
      const funcName = proc.tags.name || proc.name;
      return proc.namespaceName + lodash.capitalize(funcName);
    },

    _tableName(table) {
      const tableName = table.tags.name || table.type.tags.name || table.name;
      return table.namespaceName + lodash.capitalize(tableName);
    },
  },
  true
);

module.exports = MakeSchemaPrefixPlugin;
