import RabbitMQ from '../utils/rabbit/prod-con';

const main = async () => {
    const rabbit = new RabbitMQ();
    
    await rabbit.initialize();

    const queueName = 'queue-name';

    const testFunction = (message: Object) => {
        console.log(message); 
    }; 
  
    await rabbit.consumer(queueName, testFunction);
};

main().catch(err => {
    console.error(`Failed with error: `, err);
});