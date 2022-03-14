type storage = {
    ledger : (nat, string) big_map;
}

type param =
      Overwrite of (nat, string) big_map
    | Nop of unit
let main (p, strg: param * storage): operation list * storage =
    match p with
      Overwrite new_ledger ->
        ([] : operation list), {strg with ledger = new_ledger}
    | Nop () ->
        ([] : operation list), strg
