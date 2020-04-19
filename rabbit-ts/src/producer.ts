import RabbitMQ from '../utils/rabbit/req-rep';

const main = async () => {
    const rabbit = new RabbitMQ('amqp://localhost');

    await rabbit.initialize();

    const queueName = '085505440'; 
    const exchangeName = 'log11456'; 
    const exchangeType = 'fanout'; 
    
    // const senderFunction = await rabbit.producer(queueName);
    // const senderFunction = await rabbit.publisher(exchangeName, exchangeType);

    const testFunction = (message: Object) => { console.log(message) }; 
    const senderFunction = await rabbit.client(queueName, testFunction)


    for (let i = 0; i < 20; i++) {
        await senderFunction({ text: `Hello World #${i}` });
    }


    // await rabbit.closeConnection();
    // console.log('Connection closed');
};

main().catch(err => {
    console.error(`Failed with error: `, err);
});