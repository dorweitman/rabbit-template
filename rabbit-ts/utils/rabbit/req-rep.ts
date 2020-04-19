import * as amqp from 'amqplib';
import { v4 as uuid } from 'uuid';

import {
    RabbitMQ,
    RabbitMQOptions,
    defaultOptions,
    sendToQueue,
    channelErrorHandler,
    objectToBuffer,
} from './index';

export default class RabbitMQReqRep extends RabbitMQ {
    private channel!: amqp.ConfirmChannel;

    constructor(connectionUri: string) {
        super(connectionUri);
    }

    async client(
        queue: string,
        messageHandler: (
            message: Object,
            fields?: amqp.MessageFields,
            properties?: amqp.MessageProperties
        ) => Promise<void> | void,
        options: RabbitMQOptions = defaultOptions
    ) {
        if (!this.connection) {
            throw new Error('No connection available');
        }

        if (!this.channel) {
            this.channel = await this.connection.createConfirmChannel();
        }

        this.channel.on('error', channelErrorHandler);

        const assertedQueue = await this.channel.assertQueue('', options.queue);

        const correlationId = uuid();

        this.channel.consume(
            assertedQueue.queue,
            proccessReturnedMessage(correlationId, messageHandler),
            options.consumer,
        );

        return (message: Object) => sendToQueue(this.channel, queue, objectToBuffer(message), {
            correlationId,
            replyTo: assertedQueue.queue,
            ...options.message,
        });
    }

    async server(
        queue: string,
        messageHandler: (
            message: Object,
            fields?: amqp.MessageFields,
            properties?: amqp.MessageProperties
        ) => Promise<Object> | Object,
        options: RabbitMQOptions = defaultOptions
    ) {
        if (!this.connection) {
            throw new Error('No connection available');
        }

        if (!this.channel) {
            this.channel = await this.connection.createConfirmChannel();
        }

        this.channel.assertQueue(queue, options.queue);

        if (options.channel.prefetch) {
            this.channel.prefetch(options.channel.prefetch);
        }

        this.channel.consume(
            queue,
            replyProcessedMessage(this.channel, messageHandler, options.message),
            options.consumer,
        );
    }
}

function proccessReturnedMessage(
    correlationId: string,
    messageHandler: (
        message: Object,
        fields?: amqp.MessageFields,
        properties?: amqp.MessageProperties
    ) => Promise<void> | void,
) {
    return async (message: amqp.ConsumeMessage | null) => {
        if (!message) {
            return;
        }
        const { content, fields, properties } = message;
        const messageString = content.toString();

        if (properties.correlationId === correlationId) {
            await messageHandler(JSON.parse(messageString), fields, properties);
        }
    }
};

function replyProcessedMessage(
    channel: amqp.ConfirmChannel,
    messageHandler: (
        message: Object,
        fields?: amqp.MessageFields,
        properties?: amqp.MessageProperties
    ) => Promise<Object> | Object,
    options?: amqp.Options.Publish
) {
    return async (message: amqp.ConsumeMessage | null) => {
        if (!message) {
            return;
        }

        const { content, fields, properties } = message;
        const { replyTo, correlationId } = properties;
        const messageString = content.toString();

        try {
            const newMessage = await messageHandler(JSON.parse(messageString), fields, properties);

            await sendToQueue(channel, replyTo, objectToBuffer(newMessage), {
                correlationId,
                ...options,
            });

            channel.ack(message);
        } catch (err) {
            channel.nack(message, false, false);
        }
    };
};