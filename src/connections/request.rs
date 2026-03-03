#[derive(bincode::Decode, bincode::Encode)]
pub enum ConnectionRequests {
    Discovery,
    Connection(ConnectionRequest),
    Query(QueryCommand)
}

#[derive(bincode::Decode, bincode::Encode)]
pub struct ConnectionRequest {}

#[derive(bincode::Decode, bincode::Encode)]
pub struct SessionRequest {}

#[derive(bincode::Decode, bincode::Encode)]
pub enum QueryCommand {
    GetMembers
}