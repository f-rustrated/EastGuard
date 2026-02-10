#[derive(bincode::Decode, bincode::Encode)]
pub enum ConnectionRequests {
    Discovery,
    Connection(ConnectionRequest),
}

#[derive(bincode::Decode, bincode::Encode)]
pub struct ConnectionRequest {}

#[derive(bincode::Decode, bincode::Encode)]
pub struct SessionRequest {}
