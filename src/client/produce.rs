//! Thin raw produce — routes to the range's write leader, follows data-plane
//! redirects. `data` is shipped verbatim; encoding (codec tag + records, see `d1`/`c2`)
//! and batching/compression/idempotency are C2's job and wrap this.

use crate::client::Client;
use crate::client::error::ClientError;
use crate::connections::protocol::{
    ClientDataPlaneRequest, ClientRequest, ClientResponse, DataPlaneResponse, ProduceRequest,
};
use crate::control_plane::metadata::{EntryId, RangeId};

impl Client {
    pub(crate) async fn produce(
        &self,
        topic: &str,
        routing_key: &[u8],
        data: Vec<u8>,
        record_count: u32,
    ) -> Result<EntryId, ClientError> {
        let routing = self.resolve_topic_if_missing(topic).await?;
        let range_id = routing
            .range_id(routing_key)
            .ok_or(ClientError::TopicNotFound)?;

        self.produce_to_range(topic, range_id, routing_key, data, record_count)
            .await
    }

    /// Produce one entry under `routing_key`, returning the committed `entry_id`.
    /// Routes to the cached write leader; a redirect self-corrects and drops the
    /// stale entry so the next call re-resolves.
    pub(crate) async fn produce_to_range(
        &self,
        topic: &str,
        range_id: RangeId,
        routing_key: &[u8],
        data: Vec<u8>,
        record_count: u32,
    ) -> Result<EntryId, ClientError> {
        // Describe once to seed the cache (gives the first hop).
        let routing = self.resolve_topic_if_missing(topic).await?;

        // Start at the cached leader; fall back to a seed if the key has no cached
        // active range (the server will redirect).
        let start = routing
            .write_leader(routing_key)
            .unwrap_or(self.next_known_node());

        let request = ProduceRequest {
            topic_name: topic.to_string(),
            range_id,
            routing_key: routing_key.to_vec(),
            data,
            record_count,
        };

        let served = self.call(start, request).await?;
        // A redirect -> the cached leader was stale; drop it so the next produce re-describes.
        if served.redirected {
            self.cache.invalidate(topic);
        }
        match served.response {
            ClientResponse::DataPlane(DataPlaneResponse::Produced { entry_id }) => Ok(entry_id),
            ClientResponse::DataPlane(DataPlaneResponse::StaleRange) => {
                self.cache.invalidate(topic);
                Err(ClientError::StaleRange)
            }
            _ => Err(ClientError::UnexpectedResponse),
        }
    }
}
