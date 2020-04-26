const {
    RabbitMQ,
    defaultOptions,
    proccessMessage,
    channelErrorHandler,
    objectToBuffer,
    sendToQueue,
} = require('./index');

/**
 * Class implementing rabbitmq producer/consumer model
 * @extends RabbitMQ
 */
class RabbitMQProdCon extends RabbitMQ {
    /**
     * Creates a RabbitMQ Producer
     * @param {string} queue - Name of queue to send messages 
     * @param {object} [options] - RabbitMQ configuration 
     * @return {Promise} Asynchronous function to send messages 
     */
    async producer(queue, options = defaultOptions) {
        if (!this.connection) {
            throw new Error('No connection available');
        }

        if (!this.channel) {
            this.channel = await this.connection.createConfirmChannel();
        }

        this.channel.on('error', channelErrorHandler);

        this.channel.assertQueue(queue, options.queue);

        return (message) => sendToQueue(this.channel, queue, objectToBuffer(message), options.message);
    }

    /**
     * Creates a RabbitMQ Consumer
     * @param {string} queue - Name of queue to consume messages 
     * @param {Function} messageHandler - Function to handle recieved messages 
     * @param {object} [options] - RabbitMQ configuration 
     */
    async consumer(queue, messageHandler, options = defaultOptions) {
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

module.exports = RabbitMQProdCon;