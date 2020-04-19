import RabbitMQ from '../utils/rabbit/prod-con';

const main = async () => {
    const rabbit = new RabbitMQ('amqp://localhost');

    await rabbit.initialize();

    const queueName = '12341234567'; 
    const exchangeName = 'log1145'; 
    const exchangeType = 'fanout'; 
    
    // const senderFunction = await rabbit.producer(queueName);
    // const senderFunction = await rabbit.publisher(exchangeName, exchangeType);

    // const testFunction = (message: Object) => console.log(message); 
    const senderFunction = await rabbit.producer('rpc_queue1'); 

    for (let i = 0; i < 20; i++) {
        await senderFunction({ text: `Hello World #${i}` });
    }


    // await rabbit.closeConnection();
    // console.log('Connection closed');
};

main().catch(err => {
    console.error(`Failed with error: `, err);
});