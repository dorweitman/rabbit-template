import RabbitMQ from '../utils/rabbit/req-rep';

const main = async () => {
    const rabbit = new RabbitMQ('amqp://localhost');
    
    await rabbit.initialize();

    const queueName = '085505440';
    const exchangeName = 'log11456'; 
    const exchangeType = 'fanout'; 


    const testFunction = (message: Object) => {
        console.log(message); 
        return message;
    }; 

  
    // await rabbit.consumer(queueName, testFunction);
    // await rabbit.subscriber(exchangeName, exchangeType, testFunction);
    await rabbit.server(queueName, testFunction); 
};

main().catch(err => {
    console.error(`Failed with error: `, err);
});