#[derive(Clone, Debug)]
pub(crate) struct BigmapMetaAction {
    pub tx_context_id: i64,
    pub bigmap_id: i32,
    pub action: String,
    pub value: Option<String>,
}
