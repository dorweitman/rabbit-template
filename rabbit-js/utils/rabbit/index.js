const amqp = require('amqplib');

const defaultOptions = {
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

/** Class wrapping basic RabbitMQ functionality. */
class RabbitMQ {
    /**
     * Create a rabbit connection.
     * @param {string} connectionUri - The connection uri.
     */
    constructor(connectionUri = 'amqp://localhost') {
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

const proccessMessage = (channel, messageHandler) => async (message) => {
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

const sendToQueue = (channel, queue, message, options) => {
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

const connectionErrorHandler = (error) => {
    console.error('[RabbitMQ] connection error:', error);
};

const channelErrorHandler = (error) => {
    console.error('[RabbitMQ] channel error:', error);
};

const objectToBuffer = obj => Buffer.from(JSON.stringify(obj));

module.exports = {
    RabbitMQ,
    defaultOptions,
    proccessMessage,
    channelErrorHandler,
    objectToBuffer,
    sendToQueue,
};