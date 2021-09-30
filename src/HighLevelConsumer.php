<?php

namespace Greensight\LaravelPhpRdKafkaConsumer;

use Greensight\LaravelPhpRdKafka\KafkaManager;
use Greensight\LaravelPhpRdKafkaConsumer\Exceptions\KafkaConsumerException;
use Greensight\LaravelPhpRdKafkaConsumer\Exceptions\KafkaConsumerTimedOutException;
use RdKafka\Exception as RdKafkaException;
use RdKafka\KafkaConsumer;
use RdKafka\Message;
use Throwable;

class HighLevelConsumer
{
    protected KafkaConsumer $consumer;

    public function __construct(
        protected string $topicName, 
        ?string $consumerName, 
        protected int $consumeTimeout = 5000,
    )
    {
        $manager = resolve(KafkaManager::class);
        $this->producer =  is_null($consumerName) ? $manager->consumer() : $manager->consumer($consumerName);
    }

    /**
     * @throws KafkaException
     * @throws RdKafkaException
     * @throws Throwable
     */
    public function listen(string $processorClassName, string $processorType, string|bool $processorQueue): void
    {
        $this->consumer->subscribe([ $this->topicName ]);

        while (true) {
            $message = $this->consumer->consume($this->consumeTimeout);

            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $this->executeProcessor($processorClassName, $processorType, $processorQueue, $message);
                    $this->consumer->commitAsync($message);
                    break;

                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    throw new KafkaConsumerTimedOutException('Kafka error: ' . $message->errstr());
                    break;

                default:
                    throw new KafkaConsumerException('Kafka error: ' . $message->errstr());
            }
        }
    }

    protected function executeProcessor(string $className, string $type, string|bool $queue, Message $message): void
    {
        $queue 
            ? $this->executeQueueableProcessor($className, $type, $queue, $message) 
            : $this->executeSyncProcessor($className, $type, $message);
    }

    protected function executeSyncProcessor(string $className, string $type, Message $message): void
    {
        if ($type === 'job') {
            $className::dispatchSync($message);
        } elseif ($type === 'action') {
            resolve($className)->execute($message);
        }
    }

    protected function executeQueueableProcessor(string $className, string $type, string|bool $queue, Message $message): void
    {
        if ($type === 'job') {
            is_string($queue) ? $className::dispatch($message)->onQueue($queue) : $className::dispatch($message);
        } elseif ($type === 'action') {
            $processor = resolve($className);
            is_string($queue) ? $processor->onQueue($queue)->execute($message) : $processor->execute($message);
        }
    }
}