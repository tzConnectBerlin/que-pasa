type storage = {
    a_map : (string, nat) map
}

type param =
      Set of string * nat option
    | Overwrite of (string, nat) map
    | MaybeOverwrite of ((string, nat) map) option  // the type doesn't make much sense, but still need to make sure que pasa deals with this
let rec main (p, strg: param * storage): operation list * storage =
    match p with
      Set (k,v) ->
        ([] : operation list), {strg with a_map = Map.update k v strg.a_map}
    | Overwrite new_map ->
        ([] : operation list), {strg with a_map = new_map}
    | MaybeOverwrite maybe_new_map ->
        let res = match maybe_new_map with
                    Some new_map -> ([] : operation list), {strg with a_map = new_map}
                  | None -> ([] : operation list), strg
        in res
