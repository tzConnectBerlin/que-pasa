use crate::sql::db::DBClient;
use anyhow::Result;

#[derive(Clone, Debug)]
pub(crate) enum CliAction {
    AddContract { cid: crate::config::ContractID },
}

pub(crate) fn process_cli_actions(
    dbcli: DBClient,
    actions: &[CliAction],
) -> Result<()> {
    let mut conn = dbcli.dbconn()?;
    let mut tx = conn.transaction()?;

    for action in actions {
        info!("processing cli action: {:#?}", action);
        match action {
            CliAction::AddContract { cid } => {
                DBClient::dynamic_loader_add_contract(&mut tx, cid)?;
            }
        }
    }
    tx.commit()?;
    Ok(())
}
