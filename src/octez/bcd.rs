// bcd => better-call.dev


pub struct BCD {
    api_url: String,
}

impl BCD {
    pub fn new(url: String) Self {
        Self {
            api_url: url,
        }
    }

    pub fn get_levels_with_contract(&self, contract: String, last_id: String) -> (Vec<u32>, String) {

    }
}
