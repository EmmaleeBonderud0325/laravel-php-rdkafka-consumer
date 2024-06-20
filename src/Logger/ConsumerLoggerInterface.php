<?php

namespace Ensi\LaravelPhpRdKafkaConsumer\Logger;

use Psr\Log\LoggerInterface;

interface ConsumerLoggerInterface extends LoggerInterface
{
    public function getLogger(): LoggerInterface;

    public function getTopicKey(): string;

    public function getConsumer(): string;
}
