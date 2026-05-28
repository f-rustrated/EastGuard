use tokio::sync::mpsc;

pub(crate) struct BatchSender<T>(mpsc::Sender<Box<[T]>>);

impl<T> BatchSender<T> {
    pub(crate) async fn send_batch(&self, cmds: Vec<T>) {
        if !cmds.is_empty() {
            let _ = self.0.send(cmds.into_boxed_slice()).await;
        }
    }

    #[allow(dead_code)]
    pub(crate) fn blocking_send(&self, batch: Box<[T]>) {
        let _ = self.0.blocking_send(batch);
    }

    pub(crate) fn blocking_send_batch(&self, cmds: Vec<T>) {
        if !cmds.is_empty() {
            let _ = self.0.blocking_send(cmds.into_boxed_slice());
        }
    }
}

impl<T> From<mpsc::Sender<Box<[T]>>> for BatchSender<T> {
    fn from(tx: mpsc::Sender<Box<[T]>>) -> Self {
        Self(tx)
    }
}
