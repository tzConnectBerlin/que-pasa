type storage = {
    ledger : (nat, address) big_map;
}

type param = Overwrite of (nat, address) big_map
let main (p, strg: param * storage): operation list * storage =
    match p with
      Overwrite new_ledger ->
        ([] : operation list), {strg with ledger = new_ledger}
