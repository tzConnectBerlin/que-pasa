type storage = {
    a_nat_list : nat list
}

type param =
      Append of nat
    | Overwrite of nat list
    | MaybeOverwrite of (nat list) option  // the type doesn't make much sense, but still need to make sure que pasa deals with this
let rec main (p, strg: param * storage): operation list * storage =
    match p with
      Append n ->
        ([] : operation list), {strg with a_nat_list = n :: strg.a_nat_list}
    | Overwrite new_list ->
        ([] : operation list), {strg with a_nat_list = new_list}
    | MaybeOverwrite maybe_new_list ->
        let res = match maybe_new_list with
                    Some new_list -> ([] : operation list), {strg with a_nat_list = new_list}
                  | None -> ([] : operation list), strg
        in res
