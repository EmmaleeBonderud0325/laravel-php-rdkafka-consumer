<?php

namespace Ensi\LaravelPhpRdKafkaConsumer\Tests;

use Ensi\LaravelPhpRdKafka\KafkaFacade;
use Ensi\LaravelPhpRdKafka\KafkaManager as BaseKafkaManager;
use Ensi\LaravelPhpRdKafkaConsumer\Tests\Consumer\KafkaConsumer;
use RdKafka\Message;

class KafkaManagerFaker
{
    protected array $messages = [];

    public function __construct(
        protected string $topicName
    ) {
    }

    public function addMessage(Message $message): self
    {
        $message->err = RD_KAFKA_RESP_ERR_NO_ERROR;

        return $this->addMessageRaw($message);
    }

    public function addMessageRaw(Message $message): self
    {
        $this->messages[] = $message;

        return $this;
    }

    public function bind(): void
    {
        app()->scoped(
            BaseKafkaManager::class,
            fn () => new KafkaManager($this->makeKafkaConsumer())
        );
    }

    private function makeKafkaConsumer(): KafkaConsumer
    {
        return new KafkaConsumer($this->topicName, $this->messages);
    }

    public static function new(string $topicKey, string $consumer = 'default'): self
    {
        return new self(KafkaFacade::topicNameByClient('consumer', $consumer, $topicKey));
    }
}
