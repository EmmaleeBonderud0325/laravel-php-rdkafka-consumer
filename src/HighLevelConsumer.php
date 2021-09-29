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
    public function listen(string $handlerClass, string $type): void
    {
        $this->consumer->subscribe([ $this->topicName ]);

        while (true) {
            $message = $this->consumer->consume($this->consumeTimeout);

            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    // send new handler instance to queue
                    $this->executeHandler($handlerClass, $type, $message);
                    $this->consumer->commitAsync($message);
                    break;

                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    if ($this->exitByTimeout) {
                        return;
                    }
                    break;

                default:
                    throw new KafkaConsumerException('Kafka error: ' . $message->errstr());
            }
        }
    }

    protected function executeHandler(string $handlerClass, string $type, Message $message): void
    {
        if ($type === 'job') {
            dispatch(new $handlerClass($message));
        } elseif ($type === 'action') {
            resolve($handlerClass)->execute($message);
        }
    }
}