use anyhow::Result;
use std::vec::Vec;

use crate::config::{ContractID, QUEPASA_VERSION};
use crate::sql::table::{Column, Table};
use crate::storage_structure::typing::{ExprTy, SimpleExprTy};

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

    pub(crate) fn create_sql(column: &Column) -> Option<String> {
        match column.name.as_str() {
            "id" => return Some("id BIGSERIAL PRIMARY KEY".to_string()),
            "tx_context_id" => {
                return Some("tx_context_id BIGINT NOT NULL".to_string())
            }
            "deleted" => {
                return Some(
                    "deleted BOOLEAN NOT NULL DEFAULT 'false'".to_string(),
                )
            }
            "bigmap_id" => {
                return Some("bigmap_id INTEGER NOT NULL".to_string())
            }
            _ => {}
        }

        let name = Self::quote_id(&column.name);
        match column.column_type {
            SimpleExprTy::Address => Some(Self::address(&name)),
            SimpleExprTy::Bool => Some(Self::bool(&name)),
            SimpleExprTy::Bytes => Some(Self::bytes(&name)),
            SimpleExprTy::Int | SimpleExprTy::Nat | SimpleExprTy::Mutez => {
                Some(Self::numeric(&name))
            }
            SimpleExprTy::KeyHash
            | SimpleExprTy::Signature
            | SimpleExprTy::Contract => Some(Self::string(&name)),
            SimpleExprTy::Stop => None,
            SimpleExprTy::String => Some(Self::string(&name)),
            SimpleExprTy::Timestamp => Some(Self::timestamp(&name)),
            SimpleExprTy::Unit => Some(Self::unit(&name)),
        }
    }

    pub(crate) fn quote_id(s: &str) -> String {
        format!("\"{}\"", s)
    }

    pub(crate) fn address(name: &str) -> String {
        format!("{} VARCHAR(127) NULL", name)
    }

    pub(crate) fn bool(name: &str) -> String {
        format!("{} BOOLEAN NULL", name)
    }

    pub(crate) fn bytes(name: &str) -> String {
        format!("{} TEXT NULL", name)
    }

    pub(crate) fn numeric(name: &str) -> String {
        format!("{} NUMERIC NULL", name)
    }

    pub(crate) fn string(name: &str) -> String {
        format!("{} TEXT NULL", name)
    }

    pub(crate) fn timestamp(name: &str) -> String {
        format!("{} TIMESTAMP WITH TIME ZONE NULL", name)
    }

    pub(crate) fn unit(name: &str) -> String {
        format!("{} VARCHAR(128) NULL", name)
    }

    pub(crate) fn start_table(&self, name: &str) -> String {
        format!(
            include_str!("../../sql/table-header.sql"),
            contract_schema = self.contract_id.name,
            table = name
        )
    }

    pub(crate) fn end_table(&self) -> String {
        include_str!("../../sql/table-footer.sql").to_string()
    }

    pub(crate) fn create_columns(&self, table: &Table) -> Result<Vec<String>> {
        let mut cols: Vec<String> = match Self::table_parent_name(table) {
            Some(t) => vec![format!(
                r#""{parent_ref}" BIGINT"#,
                parent_ref = Self::parent_ref(&t)
            )],
            None => vec![],
        };
        for column in table.get_columns() {
            if !table.id_unique && column.name == *"id" {
                cols.push("id BIGINT NOT NULL".to_string());
                continue;
            }
            if let Some(val) = Self::create_sql(column) {
                cols.push(val);
            }
        }
        Ok(cols)
    }

    pub(crate) fn table_sql_columns(
        table: &Table,
        with_keywords: bool,
    ) -> Vec<String> {
        let mut cols: Vec<String> = table
            .get_columns()
            .iter()
            .filter(|x| {
                with_keywords
                    || !table
                        .keywords()
                        .iter()
                        .any(|keyword| keyword == &x.name)
            })
            .filter(|x| Self::create_sql(x).is_some())
            .map(|x| x.name.clone())
            .collect();

        if let Some(parent) = Self::table_parent_name(table) {
            cols.push(Self::parent_ref(&parent))
        };
        cols.iter()
            .map(|c| Self::quote_id(c))
            .collect()
    }

    pub(crate) fn table_sql_indices(
        table: &Table,
        with_keywords: bool,
    ) -> Vec<String> {
        let mut indices = table.indices.clone();
        if let Some(parent_key) = Self::parent_key(table) {
            indices.push(parent_key);
        }
        indices
            .iter()
            .filter(|idx| {
                with_keywords
                    || !table
                        .keywords()
                        .iter()
                        .any(|keyword| &keyword == idx)
            })
            .map(|idx| Self::quote_id(idx))
            .collect()
    }

    pub(crate) fn create_index(&self, table: &Table) -> Vec<String> {
        if table.indices.is_empty() {
            return vec![];
        }
        let uniqueness_constraint = match table.has_uniqueness() {
            true => "UNIQUE",
            false => "",
        };
        let mut res: Vec<String> = vec![format!(
            r#"CREATE {unique} INDEX ON "{contract_schema}"."{table}"({columns});"#,
            unique = uniqueness_constraint,
            contract_schema = self.contract_id.name,
            table = table.name,
            columns = Self::table_sql_indices(table, true).join(", ")
        )];
        if let Some(parent) = Self::table_parent_name(table) {
            res.push(format!(
                r#"CREATE INDEX ON "{contract_schema}"."{table}"("{parent_ref}");"#,
                contract_schema = self.contract_id.name,
                table = table.name,
                parent_ref = Self::parent_ref(&parent),
            ));
        };
        if !table.id_unique {
            res.push(format!(
                r#"CREATE INDEX ON "{contract_schema}"."{table}"(id);"#,
                contract_schema = self.contract_id.name,
                table = table.name,
            ));
        }
        res
    }

    fn table_parent_name(table: &Table) -> Option<String> {
        if !table.contains_snapshots() {
            // bigmap table rows dont have a direct relation with the parent
            // element in the storage type, as they can survive parent row
            // changes at later levels
            return None;
        }
        Self::parent_name(&table.name)
    }

    pub(crate) fn parent_name(name: &str) -> Option<String> {
        name.rfind('.')
            .map(|pos| name[0..pos].to_string())
    }

    pub(crate) fn parent_ref(parent_table: &str) -> String {
        let parent_leafname = match parent_table.rfind('.') {
            None => parent_table.to_string(),
            Some(pos) => parent_table[pos + 1..].to_string(),
        };
        format!("{}_id", parent_leafname)
    }

    fn parent_key(table: &Table) -> Option<String> {
        Self::table_parent_name(table).map(|parent| Self::parent_ref(&parent))
    }

    fn create_foreign_key_constraint(&self, table: &Table) -> Vec<String> {
        let mut fks: Vec<(String, String, String)> =
            table.fk.keys().cloned().collect();

        if let Some(parent) = Self::table_parent_name(table) {
            fks.push((Self::parent_ref(&parent), parent, "id".to_string()));
        };

        fks.into_iter().map(|(col, ref_table, ref_col)| {
            format!(
                r#"FOREIGN KEY ("{col}") REFERENCES "{contract_schema}"."{ref_table}"({ref_col})"#,
                contract_schema = self.contract_id.name,
                col = col,
                ref_table = ref_table,
                ref_col = ref_col,
            )
        }).collect::<Vec<String>>()
    }

    pub(crate) fn create_common_tables() -> String {
        format!(
            include_str!("../../sql/common-tables.sql"),
            quepasa_version = QUEPASA_VERSION,
        )
    }

    pub(crate) fn create_table_definition(
        &self,
        table: &Table,
    ) -> Result<String> {
        let mut v: Vec<String> = vec![self.start_table(&table.name)];
        let mut columns: Vec<String> = self.create_columns(table)?;
        columns[0] = format!("\t{}", columns[0]);
        columns.extend(self.create_foreign_key_constraint(table));
        let mut s = columns.join(",\n\t");
        s.push_str(",\n\t");
        v.push(s);
        v.push(self.end_table());
        v.extend(self.create_index(table));
        Ok(v.join("\n"))
    }

    pub(crate) fn create_derived_table_definitions(
        &self,
        table: &Table,
    ) -> Result<Vec<String>> {
        let mut live = table.clone();
        live.name = format!("{}_live", live.name);
        live.add_column("level", &ExprTy::SimpleExprTy(SimpleExprTy::Int));
        live.add_column(
            "level_timestamp",
            &ExprTy::SimpleExprTy(SimpleExprTy::Timestamp),
        );
        if !table.contains_snapshots() {
            live.drop_column("deleted");
        }
        // TODO: should remove this from the uniqueness constraint, as it's
        // more correct. however, currently we rely on uniq constraints to start
        // with tx_context_id, it's used as an index to speed up delete cascading
        //live.drop_index("tx_context_id");
        live.add_fk("id".to_string(), table.name.clone(), "id".to_string());

        let mut ordered = table.clone();
        ordered.name = format!("{}_ordered", ordered.name);
        ordered.add_column("level", &ExprTy::SimpleExprTy(SimpleExprTy::Int));
        ordered.add_column(
            "level_timestamp",
            &ExprTy::SimpleExprTy(SimpleExprTy::Timestamp),
        );
        ordered
            .add_column("ordering", &ExprTy::SimpleExprTy(SimpleExprTy::Int));
        if !table.contains_snapshots() {
            ordered.drop_column("bigmap_id");
        }
        ordered.add_fk("id".to_string(), table.name.clone(), "id".to_string());
        ordered.id_unique = false;

        Ok(vec![
            self.create_table_definition(&live)?,
            self.create_table_definition(&ordered)?,
        ])
    }

    /*
    fn escape(s: &str) -> String {
        s.to_string()
            .replace("'", "''")
            .replace("\\", "\\\\")
    }
    */
}
