<?php

namespace Ensi\LaravelPhpRdKafkaConsumer\Loggers;

use Psr\Log\LoggerInterface;

interface ConsumerLoggerInterface extends LoggerInterface
{
    public function getLogger(): LoggerInterface;

    public function getTopicKey(): string;

    public function getConsumerName(): string;
}
