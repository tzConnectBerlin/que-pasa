extern crate peg;

#[derive(Clone, Copy, Debug)]
pub enum SimpleExpr {
    Address,
    Int,
    Nat,
    String,
    Timestamp,
    Unit,
}

#[derive(Clone, Debug)]
pub enum ComplexExpr {
    BigMap(Box<Ele>, Box<Ele>),
    Map(Box<Ele>, Box<Ele>),
    Pair(Box<Ele>, Box<Ele>),
    Or(Box<Ele>, Box<Ele>),
    Option(Box<Ele>), // TODO: move this out into SimpleExpr??
}

#[derive(Clone, Debug)]
pub enum Expr {
    SimpleExpr(SimpleExpr),
    ComplexExpr(ComplexExpr),
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
            _ "(address " _ l:label() _ ")" _ { Ele { name : Some(l), expr : (Expr::SimpleExpr(SimpleExpr::Address)), } } /
            _ "address" _ { Ele { name : None, expr : Expr::SimpleExpr(SimpleExpr::Address), } }

        pub rule big_map() -> Ele =
            _ "(big_map " _ label:label()? _ left:expr() _ right:expr() _ ")" _ {
                Ele { name : label, expr : Expr::ComplexExpr(ComplexExpr::BigMap(Box::new(left), Box::new(right))), }
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

        pub rule int() -> Ele = _ "(int" _ l:label() _ ")" { Ele { name : Some(l), expr : Expr::SimpleExpr(SimpleExpr::Int) } }

        pub rule label() -> std::string::String = "%" s:$(['a'..='z' | 'A'..='Z' | '0'..='9' | '_']+) {
            s.to_owned()
        }

        pub rule map() -> Ele =
            _ "(map " _ label:label()? _ left:expr() _ right:expr() _ ")" _ {
                Ele { name : label, expr : Expr::ComplexExpr(ComplexExpr::Map(Box::new(left), Box::new(right))) }
            }

        pub rule mutez() -> Ele = _ "(mutez" _ l:label() _ ")" {
            Ele { name : Some(l), expr : Expr::SimpleExpr(SimpleExpr::Nat), } } /
            _ "mutez" _ { Ele { name : None, expr : Expr::SimpleExpr(SimpleExpr::Nat) } }

        pub rule nat() -> Ele = _ "(nat" _ l:label() _ ")" {
           Ele { name : Some(l), expr : Expr::SimpleExpr(SimpleExpr::Nat) } } /
            _ "nat" _ { Ele { name : None, expr : Expr::SimpleExpr(SimpleExpr::Nat), } }

        pub rule option() -> Ele = _ "(option" _ l:label() _ e:expr() _ ")" _ {
            Ele { name : Some(l), expr : Expr::ComplexExpr(ComplexExpr::Option(Box::new(e))) } }

        pub rule or() -> Ele = "(or" _ l:label()? _ left:expr() _ right:expr() ")" _
            { Ele { name : l, expr : Expr::ComplexExpr(ComplexExpr::Or(Box::new(left), Box::new(right))), } }


        pub rule pair() -> Ele =
            _"(pair" _ l:label()? _ left:expr() _ right:expr() _ ")" _ {
            Ele { name : l, expr : Expr::ComplexExpr(ComplexExpr::Pair(Box::new(left), Box::new(right))) }
            }

        pub rule string() -> Ele =
            _ "(string" _ l:label()  _ ")" _ { Ele { name : Some(l), expr : Expr::SimpleExpr(SimpleExpr::String) } } /
            _ "string" _ { Ele { name : None, expr : Expr::SimpleExpr(SimpleExpr::String) } }


        pub rule timestamp() -> Ele =
            _ "(timestamp" _ l:label() _ ")" _ { Ele { name : Some(l), expr : Expr::SimpleExpr(SimpleExpr::Timestamp), } } /
            _ "timestamp" _ { Ele { name : None, expr : Expr::SimpleExpr(SimpleExpr::Timestamp), } }

        pub rule unit() -> Ele = _ "(unit" _ l:label() _ ")" _
            { Ele { name : Some(l), expr : Expr::SimpleExpr(SimpleExpr::Unit), } } /
            _ "unit" _ { Ele { name : None, expr : Expr::SimpleExpr(SimpleExpr::Unit) } }

    }
}
