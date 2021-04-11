extern crate peg;

#[derive(Clone, Debug)]
pub enum Expr {
    Address(Option<String>),
    BigMap(String, Box<Expr> ,Box<Expr>),
    Map(String, Box<Expr>, Box<Expr>),
    Int(String),
    Nat(Option<String>),
    Pair(Option<String>, Box<Expr>,Box<Expr>),
    String(Option<String>),
    Timestamp(Option<String>),
    Unit(Option<String>),
    Option(String, Box<Expr>),
    Or(Option<String>, Box<Expr>, Box<Expr>),
}

peg::parser! {
    grammar storage() for str {

        rule _() = [' ' | '\n']*

        pub rule address() -> Expr =
            _ "(address " _ l:label() _ ")" { Expr::Address(Some(l.to_owned())) } /
            _ "address" _ { Expr::Address(None) }

        pub rule big_map() -> Expr =
            _ "(big_map " _ label:label() _ left:expr() _ right:expr() _ ")" {
                Expr::BigMap(label.to_owned(), Box::new(left), Box::new(right))
            }

        pub rule expr() -> Expr =
        x:address() { x }
        / x:big_map() { x }
        / x:int() { x }
        / x:map() { x }
        / x:nat() { x }
        / x:option() { x }
        / x:or() { x }
        / x:pair() { x }
        / x:string() { x }
        / x:timestamp() { x}
        / x:unit() { x }

        pub rule int() -> Expr = _ "(int" _ l:label() _ ")" { Expr::Int(l.to_owned()) }

        pub rule label() -> std::string::String = "%" s:$(['a'..='z' | 'A'..='Z' | '_']+) {
            println!("label: {}", s);
            s.to_owned() }

        pub rule map() -> Expr =
            _ "(map " _ label:label() _ left:expr() _ right:expr() _ ")" {
                Expr::Map(label.to_owned(), Box::new(left), Box::new(right))
            }

        pub rule nat() -> Expr = _ "(nat" _ l:label() _ ")" { Expr::Nat(Some(l)) } /
            _ "nat" _ { Expr::Nat(None) }

        pub rule option() -> Expr = _ "(option" _ l:label() _ e:expr() _ ")" {
            Expr::Option(l.to_owned(), Box::new(e)) }

        pub rule or() -> Expr = "(or" _ l:label()? _ left:expr() _ right:expr() ")"
            { Expr::Or(l, Box::new(left), Box::new(right)) }

        pub rule pair() -> Expr =
            _"(pair" _ l:label()? _ left:expr() _ right:expr() _ ")" _ {
                Expr::Pair(l, Box::new(left), Box::new(right))
            }

        pub rule string() -> Expr =
            _ "(string" _ l:label()  _ ")" { Expr::String(Some(l.to_owned())) } /
            _ "string" _ { Expr::String(None) }


        pub rule timestamp() -> Expr =
            _ "(timestamp" _ l:label() _ ")" { Expr::Timestamp(Some(l.to_owned())) } /
            _ "timestamp" _ { Expr::Timestamp(None) }

        pub rule unit() -> Expr = _ "(unit" _ l:label() _ ")" { Expr::Unit(Some(l.to_owned())) } /
            _ "unit" _ { Expr::Unit(None) }



    }

}


fn main() {
    let s = include_str!("../test/storage1.tz");
    let ast = storage::expr(s);
    println!("{:?}", ast);
}
