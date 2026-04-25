use crate::log::Event;
use async_stream::stream;
use futures::Stream;

trait Executor {
    // Executes an event, producing a stream of Chunks
    fn execute(event: Event) -> impl Stream<Item = u64>;
}
