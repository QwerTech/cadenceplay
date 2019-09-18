docker-compose up

#register domain
docker run --network=host --rm ubercadence/cli:master --do test-domain domain register -rd 1

docker run --network=host --rm ubercadence/cli:master --do test-domain domain describe

#start WF
docker run --network=host --rm ubercadence/cli:master --do test-domain workflow start --tasklist HelloWorldTaskList --workflow_type HelloWorld::sayHello --execution_timeout 3600 --input \"World123\"

#history
docker run --network=host --rm ubercadence/cli:master --do test-domain workflow list

#WF execution details
docker run --network=host --rm ubercadence/cli:master --do test-domain workflow showid 1965109f-607f-4b14-a5f2-24399a7b8fa7

#help
docker run --network=host --rm ubercadence/cli:master workflow help start

#send signal
docker run --network=host --rm ubercadence/cli:master --do test-domain workflow signal --workflow_id "bed741cc-2d82-4afd-9845-46a741bb033e" --name "HelloWorld::updateGreeting" --input \"Hi\"

#workflow describe
docker run --network=host --rm ubercadence/cli:master --do test-domain workflow describe  --workflow_id "HelloActivityWorker"