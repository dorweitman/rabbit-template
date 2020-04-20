import * as amqp from 'amqplib';

export interface RabbitMQOptions {
    queue: amqp.Options.AssertQueue;
    channel: {
        prefetch?: number;
    };
    message: amqp.Options.Publish
    exchange: amqp.Options.AssertExchange;
    consumer: amqp.Options.Consume;
}

export const defaultOptions = {
    queue: {
        durable: true,
    },
    channel: {
        prefetch: 1,
    },
    message: {
        persistent: true,
        contentType: 'application/json',
    },
    exchange: {
        durable: true,
    },
    consumer: {
        noAck: false,
    },
};

export class RabbitMQ {
    private connectionUri: string;
    protected connection: amqp.Connection | undefined;

    constructor(connectionUri: string = 'amqp://localhost') {
        this.connectionUri = connectionUri;
    }

    async initialize() {
        if (!this.connection) {
            this.connection = await amqp.connect(this.connectionUri);
        }

        this.connection.on('error', connectionErrorHandler);
    }

    async closeConnection() {
        if (this.connection) {
            await this.connection.close();
        }
    }
}

export function proccessMessage(
    channel: amqp.ConfirmChannel,
    messageHandler: (
        message: Object,
        fields?: amqp.MessageFields,
        properties?: amqp.MessageProperties
    ) => Promise<void> | void
) {
    return async (message: amqp.Message | null) => {
        if (!message) {
            return;
        }

        const { content, fields, properties } = message;
        const messageString = content.toString();

        try {
            await messageHandler(JSON.parse(messageString), fields, properties);

            channel.ack(message);
        } catch (err) {
            channel.nack(message, false, false);
        }
    };
};

export function sendToQueue(
    channel: amqp.ConfirmChannel,
    queue: string,
    message: Buffer,
    options: amqp.Options.Publish
): Promise<any> {
    return new Promise((resolve, reject) => {
        channel.sendToQueue(queue, message, options, (err, ok) => {
            if (err) {
                reject(err);
            } else {
                resolve(ok);
            }
        });
    });
};

export function connectionErrorHandler(error: Error) {
    console.error('[RabbitMQ] connection error:', error);
};

export function channelErrorHandler(error: Error) {
    console.error('[RabbitMQ] channel error:', error);
};

export function objectToBuffer(obj: Object) {
    return Buffer.from(JSON.stringify(obj));
};