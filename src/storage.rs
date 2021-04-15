extern crate peg;

#[derive(Clone, Debug)]
pub enum Expr {
    Address,
    BigMap(Box<Ele>, Box<Ele>),
    Map(Box<Ele>, Box<Ele>),
    Int,
    Nat,
    Pair(Box<Ele>, Box<Ele>),
    String,
    Timestamp,
    Unit,
    Option(Box<Ele>),
    Or(Box<Ele>, Box<Ele>),
}

#[derive(Clone, Debug)]
pub struct Ele {
    pub expr: Expr,
    pub name: Option<String>,
}

peg::parser! {
    pub grammar storage() for str {

        rule _() = [' ' | '\n']*

        pub rule address() -> Ele =
            _ "(address " _ l:label() _ ")" _ { Ele { name : Some(l), expr : Expr::Address, } } /
            _ "address" _ { Ele { name : None, expr : Expr::Address, } }

        pub rule big_map() -> Ele =
            _ "(big_map " _ label:label()? _ left:expr() _ right:expr() _ ")" _ {
                Ele { name : label, expr : Expr::BigMap(Box::new(left), Box::new(right)), }
            }

        pub rule expr() -> Ele =
        x:address() { x }
        / x:big_map() { x }
        / x:int() { x }
        / x:map() { x }
        / x:mutez() { x }
        / x:nat() { x }
        / x:option() { x }
        / x:or() { x }
        / x:pair() { x }
        / x:string() { x }
        / x:timestamp() { x}
        / x:unit() { x }

        pub rule int() -> Ele = _ "(int" _ l:label() _ ")" { Ele { name : Some(l), expr : Expr::Int } }

        pub rule label() -> std::string::String = "%" s:$(['a'..='z' | 'A'..='Z' | '0'..='9' | '_']+) {
            s.to_owned()
        }

        pub rule map() -> Ele =
            _ "(map " _ label:label()? _ left:expr() _ right:expr() _ ")" _ {
                Ele { name : label, expr : Expr::Map(Box::new(left), Box::new(right)) }
            }

        pub rule mutez() -> Ele = _ "(mutez" _ l:label() _ ")" {
            Ele { name : Some(l), expr : Expr::Nat, } } /
            _ "mutez" _ { Ele { name : None, expr : Expr::Nat } }

        pub rule nat() -> Ele = _ "(nat" _ l:label() _ ")" {
           Ele { name : Some(l), expr : Expr::Nat } } /
            _ "nat" _ { Ele { name : None, expr : Expr::Nat, } }

        pub rule option() -> Ele = _ "(option" _ l:label() _ e:expr() _ ")" _ {
            Ele { name : Some(l), expr : Expr::Option(Box::new(e)) } }

        pub rule or() -> Ele = "(or" _ l:label()? _ left:expr() _ right:expr() ")" _
            { Ele { name : l, expr : Expr::Or(Box::new(left), Box::new(right)), } }


        pub rule pair() -> Ele =
            _"(pair" _ l:label()? _ left:expr() _ right:expr() _ ")" _ {
            Ele { name : l, expr : Expr::Pair(Box::new(left), Box::new(right)) }
            }

        pub rule string() -> Ele =
            _ "(string" _ l:label()  _ ")" _ { Ele { name : Some(l), expr : Expr::String } } /
            _ "string" _ { Ele { name : None, expr : Expr::String } }


        pub rule timestamp() -> Ele =
            _ "(timestamp" _ l:label() _ ")" _ { Ele { name : Some(l), expr : Expr::Timestamp, } } /
            _ "timestamp" _ { Ele { name : None, expr : Expr::Timestamp, } }

        pub rule unit() -> Ele = _ "(unit" _ l:label() _ ")" _
            { Ele { name : Some(l), expr : Expr::Unit, } } /
            _ "unit" _ { Ele { name : None, expr : Expr::Unit } }

    }
}