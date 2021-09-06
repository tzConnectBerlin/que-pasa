use anyhow::Result;
use std::vec::Vec;

use crate::config::ContractID;
use crate::itertools::Itertools;
use crate::sql::table::{Column, Table};
use crate::storage_structure::typing::SimpleExprTy;

#[derive(Clone, Debug)]
pub struct PostgresqlGenerator {
    contract_id: ContractID,
}

impl PostgresqlGenerator {
    pub(crate) fn new(contract_id: &ContractID) -> Self {
        Self {
            contract_id: contract_id.clone(),
        }
    }

    pub(crate) fn create_sql(&self, column: Column) -> Option<String> {
        let name = Self::quote_id(&column.name);
        match column.column_type {
            SimpleExprTy::Address => Some(self.address(&name)),
            SimpleExprTy::Bool => Some(self.bool(&name)),
            SimpleExprTy::Bytes => Some(self.bytes(&name)),
            SimpleExprTy::Int => Some(self.int(&name)),
            SimpleExprTy::KeyHash => Some(self.string(&name)),
            SimpleExprTy::Mutez => Some(self.numeric(&name)),
            SimpleExprTy::Nat => Some(self.nat(&name)),
            SimpleExprTy::Stop => None,
            SimpleExprTy::String => Some(self.string(&name)),
            SimpleExprTy::Timestamp => Some(self.timestamp(&name)),
            SimpleExprTy::Unit => Some(self.unit(&name)),
        }
    }

    pub(crate) fn quote_id(s: &str) -> String {
        format!("\"{}\"", s)
    }

    pub(crate) fn address(&self, name: &str) -> String {
        format!("{} VARCHAR(127) NULL", name)
    }

    pub(crate) fn bool(&self, name: &str) -> String {
        format!("{} BOOLEAN NULL", name)
    }

    pub(crate) fn bytes(&self, name: &str) -> String {
        format!("{} TEXT NULL", name)
    }

    pub(crate) fn int(&self, name: &str) -> String {
        format!("{} NUMERIC(64) NULL", name)
    }

    pub(crate) fn nat(&self, name: &str) -> String {
        format!("{} NUMERIC(64) NULL", name)
    }

    pub(crate) fn numeric(&self, name: &str) -> String {
        format!("{} NUMERIC(64) NULL", name)
    }

    pub(crate) fn string(&self, name: &str) -> String {
        format!("{} TEXT NULL", name)
    }

    pub(crate) fn timestamp(&self, name: &str) -> String {
        format!("{} TIMESTAMP WITH TIME ZONE NULL", name)
    }

    pub(crate) fn unit(&self, name: &str) -> String {
        format!("{} VARCHAR(128) NULL", name)
    }

    pub(crate) fn start_table(&self, name: &str) -> String {
        format!(
            include_str!("../../sql/postgresql-table-header.sql"),
            contract_schema = self.contract_id.name,
            table = name
        )
    }

    pub(crate) fn end_table(&self) -> String {
        include_str!("../../sql/postgresql-table-footer.sql").to_string()
    }

    pub(crate) fn create_columns(&self, table: &Table) -> Result<Vec<String>> {
        let mut cols: Vec<String> = match Self::parent_name(&table.name) {
            Some(t) => vec![format!(r#""{table}_id" INTEGER"#, table = t)],
            None => vec![],
        };
        for column in &table.columns {
            if let Some(val) = self.create_sql(column.clone()) {
                cols.push(val);
            }
        }
        Ok(cols)
    }

    fn table_sql_columns(&self, table: &Table) -> Vec<String> {
        let mut cols: Vec<String> = table
            .columns
            .iter()
            .filter(|x| self.create_sql((*x).clone()).is_some())
            .map(|x| x.name.clone())
            .collect();

        if let Some(x) = Self::parent_name(&table.name) {
            cols.push(format!("{}_id", x))
        };
        cols.iter()
            .map(|c| Self::quote_id(c))
            .collect()
    }

    fn indices(&self, table: &Table) -> Vec<String> {
        let mut indices = table.indices.clone();
        if let Some(parent_key) = self.parent_key(table) {
            indices.push(parent_key);
        }
        indices
            .iter()
            .map(|idx| Self::quote_id(idx))
            .collect()
    }

    pub(crate) fn create_index(&self, table: &Table) -> String {
        let uniqueness_constraint = match table.has_uniqueness() {
            true => "UNIQUE",
            false => "",
        };
        format!(
            "CREATE {unique} INDEX ON \"{contract_schema}\".\"{table}\"({columns});\n",
            unique = uniqueness_constraint,
            contract_schema = self.contract_id.name,
            table = table.name,
            columns = self.indices(table).join(", ")
        )
    }

    pub(crate) fn parent_name(name: &str) -> Option<String> {
        name.rfind('.')
            .map(|pos| name[0..pos].to_string())
    }

    fn parent_key(&self, table: &Table) -> Option<String> {
        Self::parent_name(&table.name).map(|parent| format!("{}_id", parent))
    }

    fn create_foreign_key_constraint(&self, table: &Table) -> Option<String> {
        Self::parent_name(&table.name).map(|parent| {
            format!(
                r#"FOREIGN KEY ("{table}_id") REFERENCES "{contract_schema}"."{table}"(id)"#,
                contract_schema = self.contract_id.name,
                table = parent,
            )
        })
    }

    pub(crate) fn create_common_tables() -> String {
        include_str!("../../sql/postgresql-common-tables.sql").to_string()
    }

    pub(crate) fn create_table_definition(
        &self,
        table: &Table,
    ) -> Result<String> {
        let mut v: Vec<String> = vec![self.start_table(&table.name)];
        let mut columns: Vec<String> = self.create_columns(table)?;
        columns[0] = format!("\t{}", columns[0]);
        if let Some(fk) = self.create_foreign_key_constraint(table) {
            columns.push(fk);
        }
        let mut s = columns.join(",\n\t");
        s.push_str(",\n\t");
        v.push(s);
        v.push(self.end_table());
        v.push(self.create_index(table));
        Ok(v.join("\n"))
    }

    pub(crate) fn create_view_definition(
        &self,
        table: &Table,
    ) -> Result<String> {
        if table.name == "storage" {
            return Ok("".to_string());
        }
        if table.contains_snapshots() {
            self.create_views_for_snapshot_table(table)
        } else {
            self.create_views_for_changes_table(table)
        }
    }

    fn create_views_for_snapshot_table(&self, table: &Table) -> Result<String> {
        let columns: Vec<String> = self.table_sql_columns(table);
        Ok(format!(
            r#"
CREATE VIEW "{contract_schema}"."{table}_live" AS (
    SELECT
        {columns}
    FROM "{contract_schema}"."{table}" t
    JOIN tx_contexts ctx
      ON  ctx.id = t.tx_context_id
      AND ctx.level = (
            SELECT
                MAX(ctx.level) AS _level
            FROM "{contract_schema}"."{table}" t_
            JOIN tx_contexts ctx ON t_.tx_context_id = ctx.id
      )
);

CREATE VIEW "{contract_schema}"."{table}_ordered" AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY
                ctx.level,
                ctx.operation_group_number,
                ctx.operation_number,
                ctx.content_number,
                COALESCE(ctx.internal_number, -1)
        ) AS ordering,
        {columns}
    FROM "{contract_schema}"."{table}" t
    JOIN tx_contexts ctx
      ON ctx.id = t.tx_context_id
);
"#,
            contract_schema = self.contract_id.name,
            table = table.name,
            columns = columns
                .iter()
                .map(|c| format!("t.{}", c))
                .join(", "),
        ))
    }

    fn create_views_for_changes_table(&self, table: &Table) -> Result<String> {
        let columns: Vec<String> = self.table_sql_columns(table);
        let indices: Vec<String> = table
            .indices
            .iter()
            .cloned()
            .filter(|index| index != &"tx_context_id".to_string())
            .collect();

        Ok(format!(
            r#"
CREATE VIEW "{contract_schema}"."{table}_live" AS (
    SELECT
        {columns}
    FROM (
        SELECT DISTINCT ON({indices})
            t.*
        FROM "{contract_schema}"."{table}" t
        JOIN tx_contexts ctx
          ON ctx.id = t.tx_context_id
        ORDER BY
            {indices},
            ctx.level DESC,
            ctx.operation_group_number DESC,
            ctx.operation_number DESC,
            ctx.content_number DESC,
            COALESCE(ctx.internal_number, -1) DESC
    ) t
    where not t.deleted
);

CREATE VIEW "{contract_schema}"."{table}_ordered" AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY
                ctx.level,
                ctx.operation_group_number,
                ctx.operation_number,
                ctx.content_number,
                COALESCE(ctx.internal_number, -1)
        ) AS ordering,
        {columns}
    FROM "{contract_schema}"."{table}" t
    JOIN tx_contexts ctx
      ON ctx.id = t.tx_context_id
);
"#,
            contract_schema = self.contract_id.name,
            table = table.name,
            columns = columns
                .iter()
                .map(|c| format!("t.{}", c))
                .join(", "),
            indices = indices
                .iter()
                .map(|c| format!("t.{}", c))
                .join(", "),
        ))
    }

    /*
    fn escape(s: &str) -> String {
        s.to_string()
            .replace("'", "''")
            .replace("\\", "\\\\")
    }
    */
}
