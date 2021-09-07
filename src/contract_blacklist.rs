use std::collections::HashMap;

pub(crate) fn is_contract_blacklisted(address: &str) -> bool {
    let blacklisted = BLACKLIST.get(address).is_some();
    if blacklisted {
        warn!("ignoring blacklisted contract {}", address);
    }
    blacklisted
}

lazy_static! {
    static ref BLACKLIST: HashMap<String, ()> = init_blacklist();
}

fn init_blacklist() -> HashMap<String, ()> {
    let mut m = HashMap::new();
    // Following contract is blacklisted because:
    //  type is: Pair (KeyHash, Map (String, Timestamp))
    //  but values are of shape: [Elt]
    // see eg:
    //  https://better-call.dev/mainnet/opg/opNPz4UwVgKvFkUeLDczz7yZhPYyj5VBnptqgQgfPj6Ux6yUzHa/contents
    m.insert("KT1FHAtLjG6S6tfjmrDeEySVLeP8a16T4Ngr".to_string(), ());
    m
}
