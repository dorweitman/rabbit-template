const uuid = require('uuid');

const {
    RabbitMQ,
    defaultOptions,
    sendToQueue,
    channelErrorHandler,
    objectToBuffer,
} = require('./index');

/**
 * Class implementing rabbitmq request/reply (rpc) model
 * @extends RabbitMQ
 */
class RabbitMQReqRep extends RabbitMQ {
    constructor(connectionUri) {
        super(connectionUri);
    }

    /**
     * Creates an RPC Client 
     * @param {string} queue - Name of queue to send messages 
     * @param {Function} messageHandler - Function to handle recieved messages 
     * @param {object} [options] - RabbitMQ configuration 
     * @return {Function} Asynchronous function to send messages 
     */
    async client(queue, messageHandler, options = defaultOptions) {
        if (!this.channel) {
            this.channel = await this.connection.createConfirmChannel();
        }

        this.channel.on('error', channelErrorHandler);

        const assertedQueue = await this.channel.assertQueue('', options.queue);

        const correlationId = uuid.v4();

        this.channel.consume(
            assertedQueue.queue,
            proccessReturnedMessage(correlationId, messageHandler),
            options.consumer,
        );

        return (message) => sendToQueue(this.channel, queue, objectToBuffer(message), {
            correlationId,
            replyTo: assertedQueue.queue,
            ...options.message,
        });
    }

    /**
     * Creates an RPC Server
     * @param {String} queue - Name of queue to consume messages
     * @param {Function} messageHandler - Function to handle incoming messages
     * @param {object} [options] - RabbitMQ configuration 
     */
    async server(queue, messageHandler, options = defaultOptions) {
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

const proccessReturnedMessage = (correlationId, messageHandler) => {
    return async (message) => {
        const { content, fields, properties } = message;
        const messageString = content.toString();

        if (properties.correlationId === correlationId) {
            await messageHandler(JSON.parse(messageString), fields, properties);
        }
    }
};

const replyProcessedMessage = (channel, messageHandler, options) => {
    return async (message) => {
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

module.exports = RabbitMQReqRep; 