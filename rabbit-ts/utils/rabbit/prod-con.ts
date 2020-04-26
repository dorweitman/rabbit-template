import * as amqp from 'amqplib';

import {
    RabbitMQ,
    RabbitMQOptions,
    defaultOptions,
    proccessMessage,
    channelErrorHandler,
    objectToBuffer,
} from './index';

/**
 * Class implementing rabbitmq producer/consumer model
 * @extends RabbitMQ
 */
export default class RabbitMQProdCon extends RabbitMQ {
    private channel!: amqp.ConfirmChannel;

    async producer(
        queue: string,
        options: RabbitMQOptions = defaultOptions
    ) {
        if (!this.connection) {
            throw new Error('No connection available');
        }

        if (!this.channel) {
            this.channel = await this.connection.createConfirmChannel();
        }

        this.channel.on('error', channelErrorHandler);

        this.channel.assertQueue(queue, options.queue);

        return (message: Object) => sendToQueue(this.channel, queue, objectToBuffer(message), options.message);
    }

    async consumer(
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

        this.channel.assertQueue(queue, options.queue);

        if (options.channel.prefetch) {
            this.channel.prefetch(options.channel.prefetch);
        }

        this.channel.consume(
            queue,
            proccessMessage(this.channel, messageHandler),
            options.consumer,
        );
    }
}

function sendToQueue(
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