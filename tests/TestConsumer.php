<?php

namespace Ensi\LaravelPhpRdKafkaConsumer\Tests;

use function PHPUnit\Framework\assertContains;

use RdKafka\Message;

class TestConsumer
{
    public array $messages = [];

    public static function fake(string $topicKey): void
    {
        setConsumerTopicConfig($topicKey, static::class);

        app()->scoped(TestConsumer::class, static::class);
    }

    public function execute(Message $message): void
    {
        $this->messages[] = $message;
    }

    public static function assertMessageConsumed(Message $message): void
    {
        assertContains($message, resolve(static::class)->messages);
    }
}
