use fe2o3_amqp::types::messaging::{Message, MessageAnnotations};
use fe2o3_amqp::{Connection, Receiver, Sender, Session};
use time::format_description::well_known::Iso8601;
use time::OffsetDateTime;

#[tokio::main]
async fn main() {
    let mut connection = Connection::open("connection-1", "amqp://bipede:bipede@localhost:5672")
        .await
        .unwrap();

    let mut session = Session::begin(&mut connection).await.unwrap();

    let mut sender = Sender::attach(
        &mut session,
        "rust-sender-link-1",
        //"q2?last-value=true", // should create as last value
        "q2",
    )
    .await
    .unwrap();

    let receiver = Receiver::attach(&mut session, "rust-receiver-link-1", "q2")
        .await
        .unwrap();

    // First trigger
    schedule_next(&mut sender).await;
    receiver_main(receiver, sender).await;
}

async fn schedule_next(sender: &mut Sender) {
    let message = Message::builder()
        .message_annotations(
            MessageAnnotations::builder().insert("x-opt-delivery-delay", 15 * 1000),
        )
        .value("batata")
        .build();
    sender.send(message).await.unwrap();
}

async fn receiver_main(mut receiver: Receiver, mut sender: Sender) {
    while let Ok(delivery) = receiver.recv::<String>().await {
        receiver.accept(&delivery).await.unwrap();
        let now = OffsetDateTime::now_utc();
        schedule_next(&mut sender).await;
        println!(
            "received {:?} at {}",
            delivery.body(),
            now.format(&Iso8601::DEFAULT).unwrap()
        )
    }
    receiver.close().await.unwrap();
}
