use postgresql_generator::PostgresqlGenerator;

extern crate peg;

pub mod postgresql_generator;
pub mod storage;
pub mod table;
pub mod table_builder;

fn main() {
    let s = include_str!("../test/storage1.tz");
    let mut builder = table_builder::TableBuilder::new();
    let _ast = match storage::storage::expr(s) {
        Ok(ast) => builder.start_table(None, Some("storage".to_string()), None, ast),
        Err(e) => println!("{:?}", e),
    };
    for table_name in builder.tables.keys() {
        let table = builder.tables.get(table_name).unwrap();
        //print!("{}: ", table_name);
        println!("{:?}", table);
        let mut generator = PostgresqlGenerator::new();
        println!(
            "{}",
            generator.create_table_definition(table, &builder.tables)
        );
    }
}
