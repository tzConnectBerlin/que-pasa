use crate::storage::Expr;
use crate::table;

pub type TableVec = Vec<table::Table>;

pub struct TableBuilder {
    pub tables: TableVec,
}

impl TableBuilder {
    pub fn new() -> Self {
        Self { tables: vec![] }
    }

    fn flatten2(&mut self, current_table: &table::Table, expr: &Expr, vec: &mut Vec<Expr>) {
        match expr {
            Expr::Map(l, key, value) | Expr::BigMap(l, key, value) => self.start_table(
                Some(current_table.name.clone()),
                l.clone(),
                Some((**key).clone()),
                (**value).clone(),
            ),

            Expr::Pair(_l, left, right) => {
                self.flatten2(current_table, right, vec);
                self.flatten2(current_table, left, vec);
            }

            Expr::Option(_l, e) => match &**e {
                // flatten an optional struct into multiple optional fields
                Expr::Pair(_, left, right) => {
                    let mut v: Vec<Expr> = vec![];
                    self.flatten2(current_table, &*left, &mut v);
                    self.flatten2(current_table, &*right, &mut v);
                    let options: Vec<Expr> = v
                        .iter()
                        .map(|e| Expr::Option(None, Box::new(e.clone())))
                        .collect();
                    vec.extend(options);
                }
                _ => vec.push(expr.clone()),
            },

            Expr::Or(l, _left, _right) => vec.push(Expr::String(l.clone())),

            _ => vec.push(expr.clone()),
        }
    }

    fn flatten(&mut self, current_table: &table::Table, expr: &Expr) -> Vec<Expr> {
        let mut vec: Vec<Expr> = vec![];
        self.flatten2(current_table, expr, &mut vec);
        vec
    }

    pub fn start_table(
        &mut self,
        parent_name: Option<String>,
        name: Option<String>,
        indices: Option<Expr>,
        columns: Expr,
    ) {
        let name = match name {
            Some(x) => x,
            None => "storage".to_string(),
        };
        let table_name: String = match parent_name {
            Some(ref x) => format!("{}_{}", x, name),
            None => name,
        };
        let mut table: table::Table = table::Table::new(parent_name, table_name);
        match indices {
            Some(indices) => table.set_indices(self.flatten(&table, &indices)),
            None => (),
        }
        table.set_columns(self.flatten(&table, &columns));
        self.tables.push(table.clone());
    }

    pub fn build(&mut self, expr: Expr) {
        self.start_table(None, None, None, expr)
    }
}
