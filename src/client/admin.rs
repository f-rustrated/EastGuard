//! Admin / control-plane calls — create, delete, describe, list. Each is the same
//! redirect-follow loop pointed at a control-plane request, started from a seed.
//! `describe` additionally seeds the routing cache so subsequent produces route
//! directly.

use crate::client::Client;
use crate::client::error::ClientError;
use crate::connections::protocol::{
    ClientRequest, ClientResponse, ControlPlaneRequest, ControlPlaneResponse, TopicDetail,
    TopicSummary,
};
use crate::control_plane::metadata::strategy::StoragePolicy;

impl Client {
    /// Create a topic. `Ok(true)` if newly created, `Ok(false)` if it already existed.
    pub async fn create_topic(
        &self,
        name: &str,
        storage_policy: StoragePolicy,
    ) -> Result<bool, ClientError> {
        let request = ClientRequest::ControlPlane(ControlPlaneRequest::CreateTopic {
            name: name.to_string(),
            storage_policy,
        });
        let served = self.call(self.next_known_node(), request).await?;
        match served.response {
            ClientResponse::ControlPlane(ControlPlaneResponse::TopicCreated) => Ok(true),
            ClientResponse::ControlPlane(ControlPlaneResponse::AlreadyExists) => Ok(false),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Delete a topic. A missing topic surfaces as `ClientError::TopicNotFound`.
    pub async fn delete_topic(&self, name: &str) -> Result<(), ClientError> {
        let request = ClientRequest::ControlPlane(ControlPlaneRequest::DeleteTopic {
            name: name.to_string(),
        });
        let served = self.call(self.next_known_node(), request).await?;
        // The redirect loop already turned a `TopicNotFound` response into an error.
        self.cache.invalidate(name);
        match served.response {
            ClientResponse::ControlPlane(ControlPlaneResponse::TopicDeleted) => Ok(()),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    pub async fn resolve_topic(&self, name: &str) -> Result<TopicDetail, ClientError> {
        let request = ClientRequest::ControlPlane(ControlPlaneRequest::DescribeTopic {
            name: name.to_string(),
        });
        let served = self.call(self.next_known_node(), request).await?;
        match served.response {
            ClientResponse::ControlPlane(ControlPlaneResponse::TopicDetail(detail)) => {
                self.cache.insert(&detail);
                Ok(detail)
            }
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Topics whose metadata is hosted by the node that answers (per-node, not a
    /// global listing — global list is a future scatter-gather).
    pub async fn list_hosted_topics(&self) -> Result<Box<[TopicSummary]>, ClientError> {
        let request = ClientRequest::ControlPlane(ControlPlaneRequest::ListHostedTopics);
        let served = self.call(self.next_known_node(), request).await?;
        match served.response {
            ClientResponse::ControlPlane(ControlPlaneResponse::TopicList { topics }) => Ok(topics),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }
}
