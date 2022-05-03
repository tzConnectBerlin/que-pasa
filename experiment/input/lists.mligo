type storage = {
    a_nat_list : nat list
}

type param =
      Append of nat
    | Overwrite of nat list
let main (p, strg: param * storage): operation list * storage =
    match p with
      Append n ->
        ([] : operation list), {strg with a_nat_list = n :: strg.a_nat_list}
    | Overwrite new_list ->
        ([] : operation list), {strg with a_nat_list = new_list}
