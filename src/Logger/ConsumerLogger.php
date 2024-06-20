<?php

namespace Ensi\LaravelPhpRdKafkaConsumer\Logger;

use Psr\Log\AbstractLogger;
use Psr\Log\LoggerInterface;
use Stringable;

final class ConsumerLogger extends AbstractLogger implements ConsumerLoggerInterface
{
    public function __construct(
        protected LoggerInterface $logger,
        protected string $topicKey,
        protected string $consumer,
    ) {
    }

    public function getTopicKey(): string
    {
        return $this->topicKey;
    }

    public function getConsumer(): string
    {
        return $this->consumer;
    }

    public function getLogger(): LoggerInterface
    {
        return $this->logger;
    }

    public function log($level, Stringable|string $message, array $context = []): void
    {
        $context = array_merge($context, [
            'topic_key' => $this->topicKey,
            'consumer' => $this->consumer,
        ]);

        $this->logger->log($level, $message, $context);
    }
}
