<?php

namespace Greensight\LaravelPhpRdKafkaConsumer;

use Greensight\LaravelPhpRdKafka\KafkaManager;
use Greensight\LaravelPhpRdKafkaConsumer\Exceptions\KafkaConsumerException;
use RdKafka\Exception as RdKafkaException;
use RdKafka\KafkaConsumer;
use RdKafka\Message;
use Throwable;

class HighLevelConsumer
{
    protected KafkaConsumer $consumer;

    public function __construct(
        protected string $topicName, 
        ?string $consumerName = null, 
        protected int $consumeTimeout = 20000,
    )
    {
        $manager = resolve(KafkaManager::class);
        $this->consumer =  is_null($consumerName) ? $manager->consumer() : $manager->consumer($consumerName);
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
                    // This also happens when there is no new messages in the topic after the specified timeout: https://github.com/arnaud-lb/php-rdkafka/issues/343
                    // We cannot differentiate broker timeout, poll timeout and eof timeout and are forced to keep on polling as a result.
                    // When kafka broker goes back online the connection will mostly likely be reestablished.
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