import {
  ChangeMessageVisibilityCommand,
  CreateQueueCommand,
  DeleteMessageCommand,
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  Message,
  ReceiveMessageCommand,
  SQSClient,
  SendMessageCommand,
} from "@aws-sdk/client-sqs";
import { AppEnv } from "../config/env";

export class SqsQueueManager {
  public readonly client: SQSClient;
  private wagerQueueUrl?: string;
  private dlqUrl?: string;
  private eventsQueueUrl?: string;

  constructor(private readonly env: AppEnv) {
    this.client = new SQSClient({
      region: env.AWS_REGION,
      endpoint: env.SQS_ENDPOINT,
      credentials: {
        accessKeyId: env.AWS_ACCESS_KEY_ID,
        secretAccessKey: env.AWS_SECRET_ACCESS_KEY,
      },
    });
  }

  async ensureQueues(): Promise<{
    wagerQueueUrl: string;
    dlqUrl: string;
    eventsQueueUrl: string;
  }> {
    const dlqRes = await this.client.send(
      new CreateQueueCommand({
        QueueName: this.env.SQS_WAGER_DLQ_NAME,
        Attributes: {
          FifoQueue: "true",
          ContentBasedDeduplication: "false",
        },
      }),
    );
    this.dlqUrl = dlqRes.QueueUrl!;

    const dlqAttr = await this.client.send(
      new GetQueueAttributesCommand({
        QueueUrl: this.dlqUrl,
        AttributeNames: ["QueueArn"],
      }),
    );
    const dlqArn = dlqAttr.Attributes?.QueueArn!;

    const wagerRes = await this.client.send(
      new CreateQueueCommand({
        QueueName: this.env.SQS_WAGER_QUEUE_NAME,
        Attributes: {
          FifoQueue: "true",
          ContentBasedDeduplication: "false",
          VisibilityTimeout: String(this.env.SQS_VISIBILITY_TIMEOUT_SEC),
          RedrivePolicy: JSON.stringify({
            deadLetterTargetArn: dlqArn,
            maxReceiveCount: String(this.env.SQS_MAX_RECEIVE_COUNT),
          }),
        },
      }),
    );
    this.wagerQueueUrl = wagerRes.QueueUrl!;

    const eventsRes = await this.client.send(
      new CreateQueueCommand({
        QueueName: this.env.SQS_EVENTS_QUEUE_NAME,
        Attributes: {
          FifoQueue: "true",
          ContentBasedDeduplication: "false",
        },
      }),
    );
    this.eventsQueueUrl = eventsRes.QueueUrl!;

    return {
      wagerQueueUrl: this.wagerQueueUrl,
      dlqUrl: this.dlqUrl,
      eventsQueueUrl: this.eventsQueueUrl,
    };
  }

  async getWagerQueueUrl(): Promise<string> {
    if (this.wagerQueueUrl) return this.wagerQueueUrl;
    const res = await this.client.send(
      new GetQueueUrlCommand({ QueueName: this.env.SQS_WAGER_QUEUE_NAME }),
    );
    this.wagerQueueUrl = res.QueueUrl!;
    return this.wagerQueueUrl;
  }

  async getDlqUrl(): Promise<string> {
    if (this.dlqUrl) return this.dlqUrl;
    const res = await this.client.send(
      new GetQueueUrlCommand({ QueueName: this.env.SQS_WAGER_DLQ_NAME }),
    );
    this.dlqUrl = res.QueueUrl!;
    return this.dlqUrl;
  }

  async getEventsQueueUrl(): Promise<string> {
    if (this.eventsQueueUrl) return this.eventsQueueUrl;
    const res = await this.client.send(
      new GetQueueUrlCommand({ QueueName: this.env.SQS_EVENTS_QUEUE_NAME }),
    );
    this.eventsQueueUrl = res.QueueUrl!;
    return this.eventsQueueUrl;
  }

  async checkReadiness(): Promise<boolean> {
    await this.getWagerQueueUrl();
    return true;
  }

  async getDlqApproximateDepth(): Promise<number> {
    const dlqUrl = await this.getDlqUrl();
    const res = await this.client.send(
      new GetQueueAttributesCommand({
        QueueUrl: dlqUrl,
        AttributeNames: [
          "ApproximateNumberOfMessages",
          "ApproximateNumberOfMessagesNotVisible",
        ],
      }),
    );
    const visible = parseInt(
      res.Attributes?.ApproximateNumberOfMessages ?? "0",
      10,
    );
    const notVisible = parseInt(
      res.Attributes?.ApproximateNumberOfMessagesNotVisible ?? "0",
      10,
    );
    return visible + notVisible;
  }

  async receiveWagerMessages(
    maxMessages = 10,
    waitTimeSeconds = this.env.SQS_WAIT_TIME_SEC,
  ): Promise<Message[]> {
    const queueUrl = await this.getWagerQueueUrl();
    const res = await this.client.send(
      new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: maxMessages,
        WaitTimeSeconds: waitTimeSeconds,
        AttributeNames: ["All"],
        MessageAttributeNames: ["All"],
      }),
    );
    return res.Messages ?? [];
  }

  async deleteWagerMessage(receiptHandle: string): Promise<void> {
    const queueUrl = await this.getWagerQueueUrl();
    await this.client.send(
      new DeleteMessageCommand({
        QueueUrl: queueUrl,
        ReceiptHandle: receiptHandle,
      }),
    );
  }

  async changeWagerMessageVisibility(
    receiptHandle: string,
    visibilityTimeoutSec: number,
  ): Promise<void> {
    const queueUrl = await this.getWagerQueueUrl();
    await this.client.send(
      new ChangeMessageVisibilityCommand({
        QueueUrl: queueUrl,
        ReceiptHandle: receiptHandle,
        VisibilityTimeout: visibilityTimeoutSec,
      }),
    );
  }

  async sendToDlq(
    rawBody: string,
    messageGroupId: string,
    deduplicationId: string,
    failureReason: string,
  ): Promise<void> {
    const dlqUrl = await this.getDlqUrl();
    await this.client.send(
      new SendMessageCommand({
        QueueUrl: dlqUrl,
        MessageBody: rawBody,
        MessageGroupId: messageGroupId,
        MessageDeduplicationId: deduplicationId,
        MessageAttributes: {
          failureReason: {
            DataType: "String",
            StringValue: failureReason,
          },
        },
      }),
    );
  }
}
