use storage::Expr;

pub struct postgresql_generator {

}

impl postgresql_generator {

    pub fn create_sql(expr: Expr) {
        match expr {
            Expr::Address() => create_address(),
            
        }
    }


    pub fn create_address(a: Expr::Address) -> string{
        // name sql_type null
        format!("{} VARCHAR(128) NULL,", a.name);
    }

    pub fn start_table(s: string) {
        // use address?
        format!("CREATE TABLE {} ({})", s.name, s.address);
    }

    pub fn create_indices(indices: string, columns: string) {
        format!("CREATE INDEX {} ON {} INCLUDE {}", index_name, table_name, column);
    }
}