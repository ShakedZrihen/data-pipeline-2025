from utils import receive_messages_from_sqs

def enricher():
    messages = receive_messages_from_sqs()  # returns a list now

    if not messages:
        print("📭 No messages available in queue.")
        return

    print(f"📥 Read {len(messages)} message(s):")
    for i, msg in enumerate(messages, start=1):
        print(f"\n— Message {i} —")
        print(f"ID:   {msg.get('MessageId')}")
        print(f"Body: {msg.get('Body')}")

if __name__ == "__main__":
    enricher()
