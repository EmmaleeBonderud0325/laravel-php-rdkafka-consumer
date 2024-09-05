<?php

namespace Ensi\LaravelPhpRdKafkaConsumer\Loggers;

use Psr\Log\AbstractLogger;
use Psr\Log\LoggerInterface;
use Stringable;

final class ConsumerLogger extends AbstractLogger implements ConsumerLoggerInterface
{
    public function __construct(
        protected LoggerInterface $logger,
        protected string          $topicKey,
        protected string          $consumerName,
    ) {
    }

    public function getTopicKey(): string
    {
        return $this->topicKey;
    }

    public function getConsumerName(): string
    {
        return $this->consumerName;
    }

    public function getLogger(): LoggerInterface
    {
        return $this->logger;
    }

    public function log($level, Stringable|string $message, array $context = []): void
    {
        $context = array_merge($context, [
            'topic_key' => $this->topicKey,
            'consumer_name' => $this->consumerName,
        ]);

        $this->logger->log($level, $message, $context);
    }
}
