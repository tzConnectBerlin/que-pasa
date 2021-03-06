use std::collections::HashMap;

pub(crate) fn is_contract_denylisted(address: &str) -> bool {
    let denylisted = DENYLIST.get(address).is_some();
    if denylisted {
        warn!("ignoring denylisted contract {}", address);
    }
    denylisted
}

lazy_static! {
    static ref DENYLIST: HashMap<String, ()> = init_denylist();
}

fn init_denylist() -> HashMap<String, ()> {
    let mut m = HashMap::new();
    // Following contract is denylisted because:
    //  type is: Pair (KeyHash, Map (String, Timestamp))
    //  but values are of shape: [Elt]
    // see eg:
    //  https://better-call.dev/mainnet/opg/opNPz4UwVgKvFkUeLDczz7yZhPYyj5VBnptqgQgfPj6Ux6yUzHa/contents
    m.insert("KT1FHAtLjG6S6tfjmrDeEySVLeP8a16T4Ngr".to_string(), ());
    m
}
