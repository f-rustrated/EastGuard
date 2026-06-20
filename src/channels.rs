use tokio::sync::mpsc;

#[derive(Clone)]
pub(crate) struct BatchSender<T>(mpsc::Sender<Box<[T]>>);

impl<T> BatchSender<T> {
    pub(crate) async fn send_batch(&self, cmds: Box<[T]>) {
        if !cmds.is_empty() {
            let _ = self.0.send(cmds).await;
        }
    }
}

impl<T> From<mpsc::Sender<Box<[T]>>> for BatchSender<T> {
    fn from(tx: mpsc::Sender<Box<[T]>>) -> Self {
        Self(tx)
    }
}
