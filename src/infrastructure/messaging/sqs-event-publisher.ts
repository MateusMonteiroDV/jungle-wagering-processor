import { SendMessageCommand } from "@aws-sdk/client-sqs";
import type { EventPublisherPort } from "../../application/ports";
import { OutboxMessage } from "../../domain/messaging/outbox-message";
import { SqsQueueManager } from "./sqs-client";

export class SqsEventPublisher implements EventPublisherPort {
  constructor(private readonly sqs: SqsQueueManager) {}

  async publish(message: OutboxMessage): Promise<void> {
    const queueUrl = await this.sqs.getEventsQueueUrl();
    await this.sqs.client.send(
      new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: JSON.stringify(message.payload),
        MessageGroupId: message.aggregateId,
        MessageDeduplicationId: message.id,
        MessageAttributes: {
          eventType: {
            DataType: "String",
            StringValue: message.eventType,
          },
          eventId: {
            DataType: "String",
            StringValue: message.id,
          },
        },
      }),
    );
  }
}
