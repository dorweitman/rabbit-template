const {
    RabbitMQ,
    defaultOptions,
    proccessMessage,
    channelErrorHandler,
    objectToBuffer
} = require('./index');

/**
 * Class implementing rabbitmq publish/subscribe model
 * @extends RabbitMQ
 */
class RabbitMQPubSub extends RabbitMQ {
    /**
     * Creates a Rabbit Publisher 
     * @param {string} exchange - Exchage name 
     * @param {string} exchangeType - Exchange type
     * @param {string} [routingKey] - Routing messages key 
     * @param {object} [options] - RabbitMQ configuration 
     * @return {Function} Asynchronous function to send messages     
    **/
    async publisher(exchange, exchangeType, routingKey = '', options = defaultOptions) {
        if (!this.connection) {
            throw new Error('No connection available');
        }
        
        if (!this.channel) {
            this.channel = await this.connection.createConfirmChannel();

            this.channel.on('error', channelErrorHandler);

            await this.channel.assertExchange(exchange, exchangeType, options.exchange);
        }

        return (message) => publishToExchange(this.channel, exchange, routingKey, objectToBuffer(message), options.message);
    }

    /**
     * Creates a RabbitMQ Subscriber 
     * @param {string} exchange - Exchage name 
     * @param {string} exchangeType - Exchange type
     * @param {Function} messageHandler - Function to handle recieved messages 
     * @param {string} [queue] - A specific queue to recieve messages from 
     * @param {string} [pattern] - A pattern for recieving messages 
     * @param {object} [options] - RabbitMQ configuration 
     */
    async subscriber(exchange, exchangeType, messageHandler, queue = '', pattern = '', options = defaultOptions) {
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

const publishToExchange = (channel, exchange, routingKey, message, options) => {
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

module.exports = RabbitMQPubSub;