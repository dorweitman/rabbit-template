const RabbitMQ  = require('./utils/rabbit/req-rep');

const main = async () => {
    const rabbit = new RabbitMQ('amqp://localhost');
    
    await rabbit.initialize();

    const queueName = '12341234567';
    const exchangeName = 'log1145'; 
    const exchangeType = 'fanout'; 


    const testFunction = (message) => {
        console.log(message)
        return message;
    }; 

    // await rabbit.consumer(queueName, testFunction);
    // await rabbit.subscriber(exchangeName, exchangeType, testFunction);
    await rabbit.server('rpc_queue1', testFunction); 
};

main().catch(err => {
    console.error(`Failed with error: `, err);
});