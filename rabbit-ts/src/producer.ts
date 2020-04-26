import RabbitMQ from '../utils/rabbit/prod-con';

const main = async () => {
    const rabbit = new RabbitMQ();

    await rabbit.initialize();

    const queueName = 'queue-name'; 
    
    const senderFunction = await rabbit.producer(queueName);

    for (let i = 0; i < 20; i++) {
        await senderFunction({ text: `Hello World #${i}` });
    }

    await rabbit.closeConnection();

    console.log('Connection closed');
};

main().catch(err => {
    console.error(`Failed with error: `, err);
});