let unwrap (type a) (x_opt: a option): a =
    match x_opt with
      Some x -> x
    | None   -> failwith "expected Some"


type storage = {
    ledger : (nat, string) big_map;
}

type param =
      Copy of address
    | Nop of unit
let main (p, strg: param * storage): operation list * storage =
    match p with
      Copy target_contract ->
        let c: (nat, string) big_map contract =
            unwrap ((Tezos.get_entrypoint_opt "%overwrite" target_contract) : ((nat, string) big_map) contract option)
        in ([Tezos.transaction strg.ledger 0mutez c]), strg
    | Nop ()               ->
        ([] : operation list), strg
