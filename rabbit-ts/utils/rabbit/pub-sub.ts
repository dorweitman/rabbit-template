import * as amqp from 'amqplib';

import {
    RabbitMQ,
    RabbitMQOptions,
    defaultOptions,
    proccessMessage,
    channelErrorHandler,
    objectToBuffer,
} from './index';

export default class RabbitMQPubSub extends RabbitMQ {
    private channel!: amqp.ConfirmChannel;

    async publisher(
        exchange: string,
        exchangeType: string,
        routingKey: string = '',
        options: RabbitMQOptions = defaultOptions
    ) {
        if (!this.connection) {
            throw new Error('No connection available');
        }

        if (!this.channel) {
            this.channel = await this.connection.createConfirmChannel();

            this.channel.on('error', channelErrorHandler);

            await this.channel.assertExchange(exchange, exchangeType, options.exchange);
        }

        return (message: Object) => publishToExchange(this.channel, exchange, routingKey, objectToBuffer(message), options.message);
    }

    async subscriber(
        exchange: string,
        exchangeType: string,
        messageHandler: (
            message: Object,
            fields?: amqp.MessageFields,
            properties?: amqp.MessageProperties
        ) => Promise<void> | void,
        queue: string = '',
        pattern: string = '',
        options: RabbitMQOptions = defaultOptions
    ) {
        if (!this.connection) {
            throw new Error('No connection available');
        }

        if (!this.channel) {
            this.channel = await this.connection.createConfirmChannel();
        }

        await this.channel.assertExchange(exchange, exchangeType, options.exchange);

        if (options.channel.prefetch) {
            this.channel.prefetch(options.channel.prefetch);
        }

        const assertedQueue = await this.channel.assertQueue(queue, options.queue);

        this.channel.on('error', channelErrorHandler);

        await this.channel.bindQueue(assertedQueue.queue, exchange, pattern);

        this.channel.consume(
            assertedQueue.queue,
            proccessMessage(this.channel, messageHandler),
            options.consumer,
        );
    }
}

function publishToExchange(
    channel: amqp.ConfirmChannel,
    exchange: string,
    routingKey: string,
    message: Buffer,
    options: amqp.Options.Publish
): Promise<any> {
    return new Promise((resolve, reject) => {
        channel.publish(exchange, routingKey, message, options, (err, ok) => {
            if (err) {
                reject(err);
            } else {
                resolve(ok);
            }
        });
    });
};