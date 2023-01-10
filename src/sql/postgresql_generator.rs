use anyhow::Result;
use askama::Template;
use std::vec::Vec;

use crate::config::{ContractID, QUEPASA_VERSION};
use crate::sql::table::{Column, Table};
use crate::storage_structure::typing::ExprTy;

#[derive(Template)]
#[template(path = "create-changes-functions.sql", escape = "none")]
struct CreateChangesFunctionsTmpl<'a> {
    main_schema: &'a str,
    contract_schema: &'a str,
    table: &'a str,
    columns: &'a [String],
    indices: &'a [String],
    typed_columns: &'a [String],
}

#[derive(Template)]
#[template(path = "create-snapshot-functions.sql", escape = "none")]
struct CreateSnapshotFunctionsTmpl<'a> {
    main_schema: &'a str,
    contract_schema: &'a str,
    table: &'a str,
    columns: &'a [String],
    typed_columns: &'a [String],
}

#[derive(Template)]
#[template(path = "create-entrypoint-changes-functions.sql", escape = "none")]
struct CreateEntrypointChangesFunctionsTmpl<'a> {
    main_schema: &'a str,
    contract_schema: &'a str,
    table: &'a str,
    columns: &'a [String],
    typed_columns: &'a [String],
}

#[derive(Template)]
#[template(path = "create-function-shortcuts.sql", escape = "none")]
struct CreateFunctionShortcutsTmpl<'a> {
    main_schema: &'a str,
    contract_schema: &'a str,
    table: &'a str,
    function_postfix: &'a str,
    typed_columns: &'a [String],
}

#[derive(Clone, Debug)]
pub struct PostgresqlGenerator {
    main_schema: String,
    contract_id: ContractID,
}

impl PostgresqlGenerator {
    pub(crate) fn new(main_schema: String, contract_id: &ContractID) -> Self {
        Self {
            main_schema,
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
            "bigmap_id" => return Some("bigmap_id INTEGER".to_string()),
            _ => {}
        }

        let name = Self::quote_id(&column.name);
        match column.column_type {
            ExprTy::Address => Some(Self::address(&name)),
            ExprTy::Bool => Some(Self::bool(&name)),
            ExprTy::Bytes => Some(Self::bytes(&name)),
            ExprTy::Int | ExprTy::Nat | ExprTy::Mutez => {
                Some(Self::numeric(&name))
            }
            ExprTy::KeyHash | ExprTy::Signature | ExprTy::Contract => {
                Some(Self::string(&name))
            }
            ExprTy::Stop => None,
            ExprTy::String => Some(Self::string(&name)),
            ExprTy::Timestamp => Some(Self::timestamp(&name)),
            ExprTy::Unit => Some(Self::unit(&name)),
            _ => panic!(
                "unrecoverable err, cannot make sql column for type {:#?}",
                column.column_type
            ),
        }
    }

    pub(crate) fn quote_id(s: &str) -> String {
        format!("\"{}\"", s)
    }

    pub(crate) fn address(name: &str) -> String {
        format!("{} VARCHAR(127)", name)
    }

    pub(crate) fn bool(name: &str) -> String {
        format!("{} BOOLEAN", name)
    }

    pub(crate) fn bytes(name: &str) -> String {
        format!("{} TEXT", name)
    }

    pub(crate) fn numeric(name: &str) -> String {
        format!("{} NUMERIC", name)
    }

    pub(crate) fn string(name: &str) -> String {
        format!("{} TEXT", name)
    }

    pub(crate) fn timestamp(name: &str) -> String {
        format!("{} TIMESTAMP WITH TIME ZONE", name)
    }

    pub(crate) fn unit(name: &str) -> String {
        format!("{} VARCHAR(128)", name)
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

    pub(crate) fn create_table_functions(
        &self,
        contract_schema: &str,
        table: &Table,
    ) -> Result<Vec<String>> {
        let mut columns: Vec<String> =
            Self::table_sql_columns(table, false).to_vec();

        if columns.is_empty() {
            return Ok(vec![]);
        }
        columns.push("id".to_string());

        let mut typed_columns: Vec<String> = table
            .get_columns()
            .iter()
            .filter(|x| {
                !table
                    .keywords()
                    .iter()
                    .any(|keyword| keyword == &x.name)
                    && Self::create_sql(x).is_some()
            })
            .map(|x| Self::create_sql(x).unwrap())
            .collect();
        if let Some(parent) = Self::table_parent_name(table) {
            typed_columns.push(format!(
                r#""{parent_ref}" BIGINT"#,
                parent_ref = Self::parent_ref(&parent)
            ));
        };
        typed_columns.push("id BIGINT".to_string());

        if table.contains_pointers() {
            let shallow_tmpl = CreateSnapshotFunctionsTmpl {
                main_schema: &self.main_schema,
                contract_schema,
                table: &table.name,
                columns: &columns,
                typed_columns: &typed_columns,
            };
            let shallow_shortcuts = CreateFunctionShortcutsTmpl {
                main_schema: &self.main_schema,
                contract_schema,
                table: &table.name,
                function_postfix: "at",
                typed_columns: &typed_columns,
            };

            let mut deep_typed_columns: Vec<String> = typed_columns
                .iter()
                .filter(|c| !c.starts_with("bigmap_id "))
                .cloned()
                .collect();
            deep_typed_columns.insert(0, "in_table TEXT".to_string());
            deep_typed_columns.insert(0, "in_schema TEXT".to_string());
            let deep_tmpl = CreateEntrypointChangesFunctionsTmpl {
                main_schema: &self.main_schema,
                contract_schema,
                table: &table.name,
                columns: &columns,
                typed_columns: &deep_typed_columns,
            };
            let deep_shortcuts = CreateFunctionShortcutsTmpl {
                main_schema: &self.main_schema,
                contract_schema,
                table: &table.name,
                function_postfix: "at_deref",
                typed_columns: &deep_typed_columns,
            };

            return Ok(vec![
                shallow_tmpl.render()?,
                deep_tmpl.render()?,
                shallow_shortcuts.render()?,
                deep_shortcuts.render()?,
            ]);
        }

        let shortcuts = CreateFunctionShortcutsTmpl {
            main_schema: &self.main_schema,
            contract_schema,
            table: &table.name,
            function_postfix: "at",
            typed_columns: &typed_columns,
        };

        if table.contains_snapshots() {
            let tmpl = CreateSnapshotFunctionsTmpl {
                main_schema: &self.main_schema,
                contract_schema,
                table: &table.name,
                columns: &columns,
                typed_columns: &typed_columns,
            };
            return Ok(vec![tmpl.render()?, shortcuts.render()?]);
        }

        let tmpl = CreateChangesFunctionsTmpl {
            main_schema: &self.main_schema,
            contract_schema,
            table: &table.name,
            columns: &columns,
            typed_columns: &typed_columns,
            indices: &Self::table_sql_indices(table, false),
        };
        Ok(vec![tmpl.render()?, shortcuts.render()?])
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

    pub(crate) fn table_parent_name(table: &Table) -> Option<String> {
        if !table.contains_snapshots() {
            // bigmap table rows dont have a direct relation with the parent
            // element in the storage type, as they can survive parent row
            // changes at later levels
            return None;
        }
        Self::parent_name(&table.name)
    }

    pub(crate) fn parent_name(name: &str) -> Option<String> {
        if name.starts_with("entry.") && name.matches('.').count() == 1 {
            return None;
        }
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

    pub(crate) fn create_common_tables(main_schema: &str) -> String {
        format!(
            include_str!("../../sql/common-tables.sql"),
            main_schema = main_schema,
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
        live.add_column("level", &ExprTy::Int);
        live.add_column("level_timestamp", &ExprTy::Timestamp);
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
        ordered.add_column("level", &ExprTy::Int);
        ordered.add_column("level_timestamp", &ExprTy::Timestamp);
        ordered.add_column("ordering", &ExprTy::Int);
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
