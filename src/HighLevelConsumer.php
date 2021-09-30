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
        string $consumerName, 
        protected int $consumeTimeout,
        protected bool $exitByTimeout, 
    )
    {
        $this->consumer = resolve(KafkaManager::class)->consumer($consumerName);
    }

    /**
     * @throws KafkaException
     * @throws RdKafkaException
     * @throws Throwable
     */
    public function listen(string $processorClass, string $type): void
    {
        $this->consumer->subscribe([ $this->topicName ]);

        while (true) {
            $message = $this->consumer->consume($this->consumeTimeout);

            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $this->executeProcessor($processorClass, $type, $message);
                    $this->consumer->commitAsync($message);
                    break;

                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    if ($this->exitByTimeout) {
                        throw new KafkaConsumerTimedOutException('Kafka error: ' . $message->errstr());
                    }
                    break;

                default:
                    throw new KafkaConsumerException('Kafka error: ' . $message->errstr());
            }
        }
    }

    protected function executeProcessor(string $processorClass, string $type, Message $message): void
    {
        if ($type === 'job') {
            dispatch(new $processorClass($message));
        } elseif ($type === 'action') {
            resolve($processorClass)->execute($message);
        }
    }
}